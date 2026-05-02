# frozen_string_literal: true

require_relative "../test_helper"

class JobExtensionTest < Minitest::Test
  class Recorder
    @rows = []
    class << self; attr_accessor :rows; end
  end

  def setup
    super
    Recorder.rows = []

    # Stub Repository.stage! so this test doesn't need a database.
    DispatchPolicy::Repository.singleton_class.send(:alias_method, :__orig_stage, :stage!) unless DispatchPolicy::Repository.singleton_class.method_defined?(:__orig_stage)
    DispatchPolicy::Repository.singleton_class.define_method(:stage!) { |**kw| Recorder.rows << kw; true }

    # Build a job class that uses the macro
    klass = Class.new(ActiveJob::Base) do
      include DispatchPolicy::JobExtension

      dispatch_policy :testpolicy do
        context ->(args) { { key: args.first } }
        partition_by ->(c) { c[:key] }
        gate :throttle, rate: 5, per: 60
      end

      def perform(_arg); end
    end
    Object.const_set(:JobExtensionTestJob, klass) unless Object.const_defined?(:JobExtensionTestJob)
  end

  def teardown
    if DispatchPolicy::Repository.singleton_class.method_defined?(:__orig_stage)
      DispatchPolicy::Repository.singleton_class.send(:alias_method, :stage!, :__orig_stage)
    end
    Object.send(:remove_const, :JobExtensionTestJob) if Object.const_defined?(:JobExtensionTestJob)
  end

  def test_perform_later_routes_through_stage
    JobExtensionTestJob.perform_later("hello")

    assert_equal 1, Recorder.rows.size
    row = Recorder.rows.first
    assert_equal "testpolicy", row[:policy_name]
    assert_equal "hello", row[:partition_key]
    assert_equal "JobExtensionTestJob", row[:job_class]
    # Context is enriched with the job's queue_name so shard_by/gates can use it.
    assert_equal "hello",   row[:context]["key"]
    assert_equal "default", row[:context]["queue_name"]
    assert_equal "default", row[:shard]
  end

  def test_bypass_block_does_not_stage
    DispatchPolicy::Bypass.with do
      JobExtensionTestJob.perform_later("inside-bypass")
    end
    assert_empty Recorder.rows
  end

  def test_perform_all_later_routes_jobs_with_policy_through_bulk_stage
    # Stub stage_many! to capture the bulk-insert call.
    repo = DispatchPolicy::Repository.singleton_class
    repo.send(:alias_method, :__orig_stage_many, :stage_many!) unless repo.method_defined?(:__orig_stage_many)
    captured = []
    repo.define_method(:stage_many!) { |rows| captured.replace(rows); rows.size }

    # Install BulkEnqueue patch (railtie installs in real apps; in tests we
    # do it manually to avoid relying on Rails::Railtie boot order).
    unless ActiveJob.singleton_class.include?(DispatchPolicy::JobExtension::BulkEnqueue)
      ActiveJob.singleton_class.prepend(DispatchPolicy::JobExtension::BulkEnqueue)
    end

    jobs = [JobExtensionTestJob.new("a"), JobExtensionTestJob.new("b"), JobExtensionTestJob.new("c")]
    ActiveJob.perform_all_later(jobs)

    assert_equal 3, captured.size
    keys = captured.map { |r| r[:partition_key] }
    assert_equal %w[a b c], keys
    assert captured.all? { |r| r[:policy_name] == "testpolicy" }
  ensure
    repo.send(:alias_method, :stage_many!, :__orig_stage_many) if repo.method_defined?(:__orig_stage_many)
  end

  # Regression: BulkEnqueue.perform_all_later under Bypass.with must NOT
  # re-stage. Forwarder.dispatch deserializes admitted jobs and calls
  # ActiveJob.perform_all_later under Bypass to hand them off to the real
  # adapter. If BulkEnqueue ignores Bypass it re-stages — and because
  # `klass.deserialize` leaves @arguments = [], the context proc falls back
  # to its defaults, sending all jobs to a single garbage partition. We
  # observed this in the dummy app: the same job_ids re-staged forever
  # under partition_key "throttle=ep:default" while the real args said
  # endpoint:0.
  def test_perform_all_later_under_bypass_does_not_stage
    repo = DispatchPolicy::Repository.singleton_class
    repo.send(:alias_method, :__orig_stage_many, :stage_many!) unless repo.method_defined?(:__orig_stage_many)
    captured_stage = []
    repo.define_method(:stage_many!) { |rows| captured_stage.replace(rows); rows.size }

    unless ActiveJob.singleton_class.include?(DispatchPolicy::JobExtension::BulkEnqueue)
      ActiveJob.singleton_class.prepend(DispatchPolicy::JobExtension::BulkEnqueue)
    end

    # Capture what reaches the adapter via the original perform_all_later
    # fallback (per-job enqueue with TestAdapter).
    received_at_adapter = []
    JobExtensionTestJob.queue_adapter.singleton_class.alias_method(:__orig_enqueue, :enqueue)
    JobExtensionTestJob.queue_adapter.singleton_class.define_method(:enqueue) do |job|
      received_at_adapter << job
      __orig_enqueue(job)
    end

    jobs = [JobExtensionTestJob.new("a"), JobExtensionTestJob.new("b")]
    DispatchPolicy::Bypass.with { ActiveJob.perform_all_later(jobs) }

    assert_empty captured_stage,
                 "perform_all_later under Bypass must not re-stage; that path caused an infinite admission loop in the dummy app"
    assert_equal 2, received_at_adapter.size,
                 "perform_all_later under Bypass must reach the real adapter via super"
  ensure
    repo.send(:alias_method, :stage_many!, :__orig_stage_many) if repo.method_defined?(:__orig_stage_many)
    JobExtensionTestJob.queue_adapter.singleton_class.alias_method(:enqueue, :__orig_enqueue) if JobExtensionTestJob.queue_adapter.singleton_class.method_defined?(:__orig_enqueue)
  end

  # Regression: ActiveJob's `arguments` is a plain attr_accessor; it does
  # NOT trigger lazy deserialization. After `klass.deserialize(payload)`
  # the in-memory @arguments stays []; only @serialized_arguments has the
  # data. Reading job.arguments at this point gives [] and the context
  # proc falls back to defaults. We must call deserialize_arguments_if_needed
  # before reading arguments anywhere outside perform_now.
  def test_around_enqueue_materialises_arguments_after_deserialize
    # Build a job, serialize it, then construct a fresh instance via
    # deserialize — the same shape Forwarder produces.
    fresh = JobExtensionTestJob.new("alpha")
    payload = fresh.serialize

    rebuilt = JobExtensionTestJob.new
    rebuilt.deserialize(payload)
    # Pre-condition: ActiveJob defers materialization, so direct reads return [].
    assert_equal [], rebuilt.instance_variable_get(:@arguments),
                 "ActiveJob holds raw deserialized form in @serialized_arguments until perform_now"

    rebuilt.enqueue # triggers our around_enqueue → ensure_arguments_materialized!

    assert_equal 1, Recorder.rows.size, "the deserialized job must stage once"
    row = Recorder.rows.first
    assert_equal "alpha", row[:partition_key],
                 "context proc must see the materialized 'alpha', not [] (which would yield throttle=)"
  end

  def test_bulk_enqueue_materialises_arguments_after_deserialize
    repo = DispatchPolicy::Repository.singleton_class
    repo.send(:alias_method, :__orig_stage_many, :stage_many!) unless repo.method_defined?(:__orig_stage_many)
    captured = []
    repo.define_method(:stage_many!) { |rows| captured.replace(rows); rows.size }

    unless ActiveJob.singleton_class.include?(DispatchPolicy::JobExtension::BulkEnqueue)
      ActiveJob.singleton_class.prepend(DispatchPolicy::JobExtension::BulkEnqueue)
    end

    rebuilt = [JobExtensionTestJob.new("alpha"), JobExtensionTestJob.new("beta")].map do |fresh|
      payload = fresh.serialize
      r = JobExtensionTestJob.new
      r.deserialize(payload)
      r
    end

    ActiveJob.perform_all_later(rebuilt)

    assert_equal 2, captured.size
    assert_equal %w[alpha beta], captured.map { |r| r[:partition_key] },
                 "BulkEnqueue must materialize arguments before reading them, otherwise context proc sees []"
  ensure
    repo.send(:alias_method, :stage_many!, :__orig_stage_many) if repo.method_defined?(:__orig_stage_many)
  end
end
