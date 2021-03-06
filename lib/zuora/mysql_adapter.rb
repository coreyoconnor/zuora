require 'sequel'

module Zuora
  # Not quite a connector in the same way the SoapConnector and SqliteConnector are. This is designed for
  # importing data.
  class MysqlAdapter

    attr_reader :db

    def initialize(db_info, output = STDOUT)
      @db = Sequel.mysql(db_info.merge("database" => "zuora"))
      @output = output
    end

    def disconnect
      @db.disconnect
    end

    def query(sql)
      result = db << sql
      hashed_result = result.map {|r| hash_result_row(r, result) }
      {
          :query_response => {
              :result => {
                  :success => true,
                  :size => result.count,
                  :records => hashed_result,
              }
          }
      }
    end

    def create(model)
      table = self.class.table_name(model.class)
      hash = model.to_hash
      hash.delete(:id)
      new_id = @db[table.to_sym].insert(hash)
      {
          :create_response => {
              :result => {
                  :success => true,
                  :id => new_id
              }
          }
      }
    end

    def multi_insert(records)
      tables = {}
      records.each do |record|
        table = table_name(record.class)
        tables[table] = [] unless tables.has_key?(table)
        tables[table] << record
      end

      new_ids = {}
      tables.each do |table, table_records|
        new_ids[table] = []
        hashes = camelize_hashes(table_records.map(&:to_hash))
        @output.puts "#{Time.now.to_s} Inserting records into #{table}..."
        # Make sure we don't try to insert too much data at once, or the query string might get too large
        hashes.each_slice(500) do |hashes_slice|
          new_ids[table] += @db[table.to_sym].on_duplicate_key_update.multi_insert(hashes_slice)
        end
      end
      {
          :create_response => {
              :result => {
                  :success => true,
                  :ids => new_ids
              }
          }
      }
    end

    def import_from(model, query_locator)
      @output.puts "Importing #{model.name.split('::').last} records with query locator #{query_locator}..."
      records, new_query_locator = model.get(query_locator)
      multi_insert(records)
      new_query_locator
    end

    def import(model, where = "")
      @output.puts "Importing #{model.name.split('::').last} records..."
      records, query_locator = model.where(where)
      multi_insert(records)
      while !query_locator.nil? and !query_locator.empty?
        query_locator = import_from(model, query_locator)
      end
    end

    def get_update_times(model)
      @db[:updates].first(:model => table_name(model))
    end

    def import_new(model)
      # NOTE(omar): These should work, but they produce a mysql error on my local db running mysql v5.5
      #max_updated_date = @db[table_name(model)].max(:UpdatedDate)
      #max_created_date = @db[table_name(model)].max(:CreatedDate)
      last_update_times = get_update_times(model)

      if last_update_times
        max_date_str = last_update_times[:update_start].strftime("%Y-%m-%dT%H:%M:%S%z")
        where = "CreatedDate >= #{max_date_str} OR UpdatedDate >= #{max_date_str}"
      else
        where = ""
      end

      update_start = Time.now
      import(model, where)
      update_end = Time.now
      set_update_times(model, update_start, update_end)
    end

    def camelize_hashes(hashes)
      camelized_hashes = []
      hashes.each do |hash|
        camelized_hash = {}
        hash.each { |key, value| camelized_hash[key.to_s.camelize] = value }
        camelized_hashes << camelized_hash
      end
      camelized_hashes
    end

    def update(model)
      table  = table_name(model.class)
      hash   = model.to_hash
      @db[table.to_sym][id].update(hash)
      {
          :update_response => {
              :result => {
                  :success => true,
                  :id => id
              }
          }
      }
    end

    def destroy(model)
      table = table_name(model.class)
      @db[table.to_sym][model.id].delete
      {
          :delete_response => {
              :result => {
                  :success => true,
                  :id => model.id
              }
          }
      }
    end

    def self.table_name(model)
      model.name.demodulize
    end

    def table_name(model)
      self.class.table_name(model)
    end

    protected

    def set_update_times(model, update_start, update_end)
      @db[:updates].on_duplicate_key_update <<
          { :model => table_name(model), :update_start => update_start, :update_end => update_end }
    end

    def hash_result_row(row, result)
      row = row.map {|r| r.nil? ? "" : r }
      Hash[result.columns.zip(row.to_a)]
    end
  end
end
