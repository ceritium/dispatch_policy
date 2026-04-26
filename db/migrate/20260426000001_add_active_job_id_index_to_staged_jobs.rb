# frozen_string_literal: true

class AddActiveJobIdIndexToStagedJobs < ActiveRecord::Migration[7.1]
  disable_ddl_transaction!

  def change
    add_index :dispatch_policy_staged_jobs,
      %i[active_job_id],
      where: "completed_at IS NULL AND active_job_id IS NOT NULL",
      name: "idx_dp_staged_active_job_id_inflight",
      algorithm: :concurrently
  end
end
