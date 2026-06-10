# frozen_string_literal: true

module DispatchPolicy
  # Hooks into ActiveJob::Base. Adds:
  #   - the `dispatch_policy :name do … end` class macro
  #   - an `around_enqueue` callback that stages jobs declaring a policy
  #   - a `perform_all_later` patch that handles bulk enqueue
  module JobExtension
    extend ActiveSupport::Concern

    included do
      class_attribute :dispatch_policy_name, instance_writer: false
    end

    class_methods do
      def dispatch_policy(name, &block)
        policy = PolicyDSL.build(name.to_s, &block)
        DispatchPolicy.registry.register(policy, owner: self.name)
        self.dispatch_policy_name = policy.name

        around_enqueue do |job, block|
          DispatchPolicy::JobExtension.around_enqueue_for(job, block)
        end
      end
    end

    # Called by the around_enqueue lambda. Public so it can be tested directly.
    def self.around_enqueue_for(job, block)
      return block.call if Bypass.active?
      return block.call unless DispatchPolicy.config.enabled

      policy = DispatchPolicy.registry.fetch(job.class.dispatch_policy_name)
      return block.call unless policy

      if retry_attempt?(job) && policy.bypass_retries?
        return block.call
      end

      # `klass.deserialize(payload)` (used elsewhere — see Forwarder, retries)
      # only sets @serialized_arguments. ActiveJob defers the actual
      # arguments deserialization to perform_now via the private
      # deserialize_arguments_if_needed. If something deserializes a job
      # and re-enqueues it without going through perform_now (e.g. a
      # custom retry path), `job.arguments` would be []. Guard against
      # that here so the context proc always sees the real args.
      ensure_arguments_materialized!(job)

      queue_name    = job.queue_name&.to_s || policy.queue_name
      ctx           = policy.build_context(job.arguments, queue_name: queue_name)
      partition_key = policy.partition_key_for(ctx)
      shard         = policy.shard_for(ctx)
      payload       = Serializer.serialize(job)

      Repository.stage!(
        policy_name:   policy.name,
        partition_key: partition_key,
        queue_name:    queue_name,
        shard:         shard,
        job_class:     job.class.name,
        job_data:      payload,
        context:       ctx.to_jsonb,
        scheduled_at:  scheduled_time(job),
        priority:      job.priority || 0
      )

      job.successfully_enqueued = true
      false # halts the around_enqueue chain so the real adapter never sees the job
    end

    def self.retry_attempt?(job)
      (job.respond_to?(:executions) ? job.executions.to_i : 0).positive?
    end

    # Whether a job should be staged through admission control rather than
    # handed straight to the adapter. Mirrors the single-enqueue decision in
    # around_enqueue_for: it needs a registered policy and must not be a
    # retry on a bypass_retries policy. Used by the bulk path to split jobs
    # so the ones we don't own fall through to the adapter instead of being
    # silently dropped.
    def self.stageable?(job)
      return false unless job.class.respond_to?(:dispatch_policy_name)

      name = job.class.dispatch_policy_name
      return false unless name

      policy = DispatchPolicy.registry.fetch(name)
      return false unless policy
      return false if retry_attempt?(job) && policy.bypass_retries?

      true
    end

    def self.scheduled_time(job)
      ts = job.scheduled_at
      return nil if ts.nil?
      return ts   if ts.is_a?(Time)

      Time.at(Float(ts))
    rescue ArgumentError, TypeError
      nil
    end

    # ActiveJob's `arguments` getter is a plain attr_accessor that returns
    # the in-memory @arguments. After `klass.deserialize(payload)`, that
    # array is empty until perform_now triggers
    # `deserialize_arguments_if_needed` (a private method). Anywhere we
    # read `job.arguments` outside of perform we must materialize first,
    # or the context proc receives [] and falls back to its defaults.
    def self.ensure_arguments_materialized!(job)
      return unless job.respond_to?(:deserialize_arguments_if_needed, true)
      job.send(:deserialize_arguments_if_needed)
    end

    # ---- perform_all_later support -------------------------------------------

    # Rails 7.1+ exposes ActiveJob.perform_all_later. We override it to route
    # jobs declaring a dispatch_policy through a single bulk INSERT, while
    # delegating jobs without a policy to the original enqueue_all path.
    module BulkEnqueue
      def perform_all_later(*jobs)
        flat = jobs.flatten
        return super if flat.empty?
        # Critical: respect Bypass exactly like the per-job around_enqueue
        # does. Forwarder.dispatch deserializes admitted jobs and calls
        # ActiveJob.perform_all_later under Bypass.with — without this
        # check, BulkEnqueue would re-stage them, creating an infinite
        # admission loop with the wrong context (job.arguments is still []
        # at that point because ActiveJob defers deserialization).
        return super if DispatchPolicy::Bypass.active?
        return super unless DispatchPolicy.config.enabled
        return super unless DispatchPolicy.registry.size.positive?

        # Split exactly like the single path decides: jobs we own get staged,
        # everything else (no policy, unregistered policy name, or a retry on
        # a bypass_retries policy) goes straight to the adapter. Dropping them
        # — as a `next unless policy` inside the row builder would — silently
        # loses jobs the caller expected to be enqueued.
        to_stage, to_adapter = flat.partition { |job| JobExtension.stageable?(job) }

        super(to_adapter) if to_adapter.any?

        return nil if to_stage.empty?

        rows = to_stage.map do |job|
          policy = DispatchPolicy.registry.fetch(job.class.dispatch_policy_name)

          # See JobExtension.ensure_arguments_materialized! — we need this
          # for the same reason as the single-enqueue path.
          JobExtension.ensure_arguments_materialized!(job)

          queue_name    = job.queue_name&.to_s || policy.queue_name
          ctx           = policy.build_context(job.arguments, queue_name: queue_name)
          partition_key = policy.partition_key_for(ctx)
          shard         = policy.shard_for(ctx)
          payload       = Serializer.serialize(job)

          {
            policy_name:   policy.name,
            partition_key: partition_key,
            queue_name:    queue_name,
            shard:         shard,
            job_class:     job.class.name,
            job_data:      payload,
            context:       ctx.to_jsonb,
            scheduled_at:  JobExtension.scheduled_time(job),
            priority:      job.priority || 0
          }
        end

        # Only mark enqueued AFTER the INSERT commits. If stage_many! raises,
        # a caller that rescues and inspects successfully_enqueued? must not
        # be told the jobs were enqueued when they weren't.
        Repository.stage_many!(rows)
        to_stage.each { |job| job.successfully_enqueued = true }
        nil # ActiveJob.perform_all_later contract returns nil
      end
    end
  end
end
