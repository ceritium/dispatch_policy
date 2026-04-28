# frozen_string_literal: true

module DispatchPolicy
  class ApplicationRecord < ActiveRecord::Base
    self.abstract_class = true

    # Multi-database support: when the host app stores jobs in a
    # separate database (e.g. Solid Queue's :queue role), point our
    # tables at the same connection so admission + adapter enqueue
    # share a transaction. Configured via DispatchPolicy.config
    # .database_role; nil leaves us on the primary.
    if defined?(DispatchPolicy) && (role = DispatchPolicy.config.database_role)
      connects_to database: { writing: role, reading: role }
    end
  end
end
