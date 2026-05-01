# frozen_string_literal: true

require "base64"
require "json"

module DispatchPolicy
  # Tiny keyset-pagination helper for the engine UI. Each sort mode declares
  # a single sortable column plus the row id as a deterministic tiebreaker
  # so two rows can never share the same cursor. NULLable columns are
  # coalesced to a sentinel ('1970-01-01' for timestamps) so the cursor
  # clause stays a simple tuple comparison.
  module CursorPagination
    SENTINEL_TS = "1970-01-01 00:00:00".freeze

    # name => { sql_order:, cursor_sql:, direction:, label: }
    # cursor_sql is the expression to extract the sort key for a row
    # (used both in ORDER BY and to build the cursor tuple).
    SORTS = {
      "pending" => {
        sql_order:  "pending_count DESC, id ASC",
        cursor_sql: "pending_count",
        direction:  :desc,
        label:      "pending desc"
      },
      "admitted" => {
        sql_order:  "total_admitted DESC, id ASC",
        cursor_sql: "total_admitted",
        direction:  :desc,
        label:      "lifetime admitted"
      },
      "stale" => {
        sql_order:  "COALESCE(last_checked_at, TIMESTAMP '#{SENTINEL_TS}') ASC, id ASC",
        cursor_sql: "COALESCE(last_checked_at, TIMESTAMP '#{SENTINEL_TS}')",
        direction:  :asc,
        label:      "stalest (round-trip)"
      },
      "recent" => {
        sql_order:  "COALESCE(last_admit_at, TIMESTAMP '#{SENTINEL_TS}') DESC, id ASC",
        cursor_sql: "COALESCE(last_admit_at, TIMESTAMP '#{SENTINEL_TS}')",
        direction:  :desc,
        label:      "recent admit"
      },
      "key" => {
        sql_order:  "partition_key ASC, id ASC",
        cursor_sql: "partition_key",
        direction:  :asc,
        label:      "partition key"
      }
    }.freeze

    DEFAULT_SORT = "pending"

    module_function

    def sort_for(name)
      SORTS[name] || SORTS.fetch(DEFAULT_SORT)
    end

    def encode(value, id)
      Base64.urlsafe_encode64(JSON.dump([value, id]), padding: false)
    end

    def decode(cursor)
      return nil if cursor.nil? || cursor.empty?

      decoded = JSON.parse(Base64.urlsafe_decode64(cursor))
      return nil unless decoded.is_a?(Array) && decoded.size == 2

      decoded
    rescue StandardError
      nil
    end

    # Apply a cursor tuple (value, id) to an AR scope under the given sort.
    # The tiebreaker on id is always ASC so id strictly advances forward.
    def apply(scope, sort_name, cursor)
      sort = sort_for(sort_name)
      return scope if cursor.nil?

      value, last_id = cursor
      case sort[:direction]
      when :desc
        scope.where(
          "(#{sort[:cursor_sql]} < ?) OR (#{sort[:cursor_sql]} = ? AND id > ?)",
          value, value, last_id
        )
      when :asc
        scope.where(
          "(#{sort[:cursor_sql]} > ?) OR (#{sort[:cursor_sql]} = ? AND id > ?)",
          value, value, last_id
        )
      end
    end

    # Read the cursor key from a row using the given sort. Returns the
    # raw value the cursor was built from (for emitting to the next link).
    def extract(row, sort_name)
      sort = sort_for(sort_name)
      column = sort[:cursor_sql]
      # cursor_sql may include a COALESCE(...). For row-side extraction we
      # mirror that with Ruby. The columns we coalesce are timestamps; we
      # use Time.at(0) as the equivalent sentinel.
      raw = case column
            when "pending_count", "total_admitted", "partition_key"
              row.send(column)
            when /COALESCE\(last_checked_at,/
              row.last_checked_at || Time.at(0)
            when /COALESCE\(last_admit_at,/
              row.last_admit_at   || Time.at(0)
            end
      [serialize_value(raw), row.id]
    end

    def serialize_value(v)
      case v
      when Time, ActiveSupport::TimeWithZone then v.utc.iso8601(6)
      else v
      end
    end
  end
end
