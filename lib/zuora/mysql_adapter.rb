require 'sequel'

module Zuora
  # Not quite a connector in the same way the SoapConnector and SqliteConnector are. This is designed for
  # importing data.
  class MysqlAdapter

    attr_reader :db

    def initialize(db_info)
      @db = Sequel.mysql(db_info.merge("database" => "zuora"))
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
        puts "#{Time.now.to_s} Inserting records into #{table}..."
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
      puts "Importing #{model.name.split('::').last} records with query locator #{query_locator}..."
      records, new_query_locator = model.get(query_locator)
      multi_insert(records)
      new_query_locator
    end

    def import(model, where = "")
      puts "Importing #{model.name.split('::').last} records..."
      records, query_locator = model.where(where)
      multi_insert(records)
      while !query_locator.nil? and !query_locator.empty?
        query_locator = import_from(model, query_locator)
      end
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

    def table_name(model)
      model.name.demodulize
    end

    protected

    def hash_result_row(row, result)
      row = row.map {|r| r.nil? ? "" : r }
      Hash[result.columns.zip(row.to_a)]
    end
=begin Wrong column types

    def parse_attributes(type, attrs = {})
      data = attrs.to_a.map do |a|
        key, value = a
        [key.underscore, value]
      end
      Hash[data]
    end

    def self.generate_tables
      Zuora::Objects::Base.subclasses.each do |model|
        create_table(model)
      end
    end

    def self.create_table_schema(model)
      table_name = self.table_name(model)
      attributes = model.attributes - [:id]
      attributes = attributes.map do |a|
        "'#{a.to_s.camelize}' text"
      end
      autoid = "'Id' integer PRIMARY KEY AUTOINCREMENT"
      attributes.unshift autoid
      attributes = attributes.join(", ")
      "CREATE TABLE 'main'.'#{table_name}' (#{attributes});"
    end

    def self.create_table(model)
      schema = self.create_table_schema(model)
      db.execute schema
    end

    def self.generate_schema
      schema = ""
      Zuora::Objects::Base.subclasses.each do |model|
        schema += create_table_schema(model) + "\n"
      end
      schema
    end
=end
  end
end
