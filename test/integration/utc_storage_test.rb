# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../../app/models/dispatch_policy/application_record"
require_relative "../../app/models/dispatch_policy/partition"
require_relative "../../app/models/dispatch_policy/staged_job"
require_relative "../../app/models/dispatch_policy/inflight_job"
require_relative "../../app/models/dispatch_policy/tick_sample"

# A13: every datetime the gem writes is UTC, whatever the session says.
#
# These columns are `timestamp WITHOUT time zone` — a wall clock and no
# zone — and ActiveRecord reads them back as UTC because
# `default_timezone` is `:utc`. A bare `now()` is a timestamptz, so writing
# it stored the SESSION's wall clock instead, and a host that sets
# `variables: { timezone: … }` in database.yml got every one of them
# shifted by its offset: comparisons that decided wrongly (A10/A11) and
# values displayed wrongly (A13).
#
# `Repository::UTC_NOW` removes the category rather than compensating for
# it. This file is the end-to-end check: drive the gem through a skewed
# session and read the rows back through ActiveRecord, which is what every
# view does.
class UtcStorageTest < DispatchPolicy::IntegrationTest
  POLICY = "utc_storage"
  KEY    = "acct:1"
  # UTC+10 and UTC-10 (the Etc/GMT sign is inverted). No DST, so the offset
  # is the same whenever this runs.
  ZONES  = { "east" => "Etc/GMT-10", "west" => "Etc/GMT+10" }.freeze

  def teardown
    session_timezone("UTC")
    super
  end

  def session_timezone(zone)
    DispatchPolicy::Repository.connection.execute("SET TIME ZONE '#{zone}'")
  end

  # Every timestamp the gem wrote must be within a minute of true UTC when
  # read back the way a view reads it. Under the old scheme the skewed ones
  # were exactly ten hours out.
  ZONES.each do |direction, zone|
    define_method("test_every_written_timestamp_is_utc_#{direction}") do
      session_timezone(zone)
      truth = Time.now.utc

      DispatchPolicy::Repository.stage!(
        policy_name: POLICY, partition_key: KEY, queue_name: nil,
        job_class: "X", job_data: {}, context: {}
      )
      # Through the BULK path too, because that is the only one that lets
      # the column DEFAULT write `enqueued_at` — `stage!` supplies it
      # explicitly, so a test that only calls `stage!` leaves the four
      # column defaults completely unexercised. Reverting one of them was
      # SURVIVED until this line existed.
      DispatchPolicy::Repository.stage_many!([{
        policy_name: POLICY, partition_key: KEY, queue_name: nil,
        job_class: "X", job_data: {}, context: {}, scheduled_at: nil
      }])
      DispatchPolicy::Repository.claim_partitions(policy_name: POLICY, limit: 5)
      DispatchPolicy::Repository.insert_inflight!([{
        policy_name: POLICY, partition_key: KEY, active_job_id: "aj-#{direction}"
      }])
      DispatchPolicy::Repository.record_tick_sample!(
        policy_name: POLICY, duration_ms: 1, partitions_seen: 1, partitions_admitted: 0,
        partitions_denied: 0, jobs_admitted: 0, forward_failures: 0,
        pending_total: 1, inflight_total: 1, denied_reasons: {}
      )

      partition = DispatchPolicy::Partition.find_by(policy_name: POLICY, partition_key: KEY)
      written = {
        # written by a column DEFAULT
        "staged_jobs.enqueued_at (explicit)" => DispatchPolicy::StagedJob.order(:id).first.enqueued_at,
        "staged_jobs.enqueued_at (default)"  => DispatchPolicy::StagedJob.order(:id).last.enqueued_at,
        "inflight.admitted_at"      => DispatchPolicy::InflightJob.first.admitted_at,
        "inflight.heartbeat_at"     => DispatchPolicy::InflightJob.first.heartbeat_at,
        "tick_samples.sampled_at"   => DispatchPolicy::TickSample.first.sampled_at,
        # written by an UPDATE or an INSERT expression
        "partitions.created_at"     => partition.created_at,
        "partitions.updated_at"     => partition.updated_at,
        "partitions.last_enqueued_at"   => partition.last_enqueued_at,
        "partitions.context_updated_at" => partition.context_updated_at,
        "partitions.last_checked_at"    => partition.last_checked_at
      }

      written.each do |name, value|
        refute_nil value, "#{name} was not written"
        assert_in_delta truth.to_f, value.to_f, 60,
                        "#{direction}: #{name} is #{(value.to_f - truth.to_f).round} seconds " \
                        "from true UTC — the session's offset, stored"
      end
    end
  end

  # The lint. Every one of the 40-odd SQL sites has to use the constant;
  # a single bare `now()` reintroduces the whole class for whichever column
  # it touches, and no behavioural test can cover a site that does not
  # exist yet. This is a source assertion on purpose and it is the only
  # kind that fits: it is guarding an ABSENCE across a whole file.
  def test_no_sql_in_the_repository_uses_a_bare_now
    offenders = File.readlines("lib/dispatch_policy/repository.rb").each_with_index.filter_map do |line, i|
      next if line.lstrip.start_with?("#")        # prose may say now()
      next unless line.include?("now()")
      next if line.include?("AT TIME ZONE")       # the constant's own definition

      "#{i + 1}: #{line.strip}"
    end

    assert_empty offenders,
                 "SQL must use Repository::UTC_NOW, not a bare now(): a timestamptz written " \
                 "into a `timestamp WITHOUT time zone` column stores the SESSION's wall clock"
  end
end
