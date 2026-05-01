# frozen_string_literal: true

DispatchPolicy.configure do |c|
  c.tick_max_duration         = 25
  c.partition_batch_size      = 50
  c.admission_batch_size      = 50
  c.idle_pause                = 0.4
  c.partition_inactive_after  = 24 * 60 * 60
  c.inflight_stale_after      = 5 * 60
  c.sweep_every_ticks         = 25
end
