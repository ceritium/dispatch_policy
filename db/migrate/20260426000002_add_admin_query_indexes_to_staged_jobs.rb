# frozen_string_literal: true

class AddAdminQueryIndexesToStagedJobs < ActiveRecord::Migration[7.1]
  disable_ddl_transaction!

  def change
    add_index :dispatch_policy_staged_jobs,
      %i[policy_name],
      where: "admitted_at IS NOT NULL AND completed_at IS NULL",
      name: "idx_dp_staged_inflight_by_policy",
      algorithm: :concurrently,
      if_not_exists: true

    add_index :dispatch_policy_staged_jobs,
      %i[policy_name completed_at],
      where: "completed_at IS NOT NULL",
      name: "idx_dp_staged_completed_by_policy",
      algorithm: :concurrently,
      if_not_exists: true
  end
end
