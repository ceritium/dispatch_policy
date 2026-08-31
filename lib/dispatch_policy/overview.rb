# frozen_string_literal: true

module DispatchPolicy
  # The dashboard's headline numbers, and the one config question the
  # operator hints need answered.
  #
  # These live here rather than inline in `DashboardController#index` so a
  # test can EXECUTE them. The test environment does not boot Rails, so
  # anything that only exists inside a controller action can be pinned
  # solely by reading the controller's source — and a source pin passes
  # for any change that leaves the asserted characters somewhere in the
  # file. Three mutations were "caught" that way: the tile was made to
  # count held rows and the hint to promise a retry that never comes,
  # with the old expressions preserved verbatim in a heredoc, and the
  # suite stayed green while the battery printed CAUGHT.
  module Overview
    module_function

    # `staged` is DELIVERABLE only. A held-back row is not backlog —
    # nothing is trying to admit it — so counting it there told the
    # operator work was moving when it was not, and fed a drain-time
    # estimate that could never come true. Held rows get their own tile.
    def totals
      {
        staged:        StagedJob.deliverable.count,
        quarantined:   StagedJob.quarantined.count,
        partitions:    Partition.count,
        active_parts:  Partition.active.count,
        paused_parts:  Partition.paused.count,
        in_flight:     InflightJob.count
      }
    end

    # Whether a held row will be released without anyone pressing a
    # button. `quarantine_retry_after = 0` ("hold forever") and
    # `sweep_every_ticks = 0` ("never sweep") are both documented values,
    # and EITHER one stops the release running — hence AND, not OR. The
    # hint promises an automatic retry only when this is true; promising
    # one that cannot arrive is the same defect class as a hint that
    # crashes the page, since it misleads exactly when held rows are the
    # problem.
    def quarantine_auto_release?(config = DispatchPolicy.config)
      config.quarantine_retry_after.to_i.positive? &&
        config.sweep_every_ticks.to_i.positive?
    end
  end
end
