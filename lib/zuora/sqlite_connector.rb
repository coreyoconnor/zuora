require 'sqlite3'

module Zuora
  #Sqlite3 in memoroy connector to simulate Zuora in test environments
  class SqliteConnector
    QUERY_LIMIT = 10
    QUERY_LOCATOR_DELIMITER = "|"

    cattr_accessor :db

    def initialize(model)
      @model = model
    end

    def query_with_offset(sql, offset = 0)
      result = db.query (sql + " LIMIT #{QUERY_LIMIT + 1} OFFSET #{offset}")
      if result.count > QUERY_LIMIT
        limited_result = result[0...QUERY_LIMIT]
        query_locator = [offset.to_i + QUERY_LIMIT, sql].join(QUERY_LOCATOR_DELIMITER)
      else
        limited_result = result
        query_locator = nil
      end
      hashed_result = limited_result.map {|r| hash_result_row(r, result) }
      {
          :query_response => {
              :result => {
                  :success => true,
                  :size => result.count,
                  :records => hashed_result,
                  :query_locator => query_locator
              }
          }
      }
    end

    def query(sql)
      query_with_ofset(sql, 0)
    end

    def query_more(query_locator)
      offset, sql = query_locator.split(QUERY_LOCATOR_DELIMITER)
      query_with_offset(sql, offset)
    end

    def create
      table = self.class.table_name(@model.class)
      hash = @model.to_hash
      hash.delete(:id)
      keys = []
      values = []
      hash.each do |key, value|
        keys << key.to_s.camelize
        values << value.to_s
      end
      place_holder = ['?'] * keys.length
      keys = keys.join(', ')
      place_holder = place_holder.join(', ')
      insert = "INSERT into '#{table}'(#{keys}) VALUES(#{place_holder})"
      db.execute insert, values
      new_id = db.last_insert_row_id
      {
        :create_response => {
          :result => {
            :success => true,
            :id => new_id
          }
        }
      }
    end

    def update
      table  = self.class.table_name(@model.class)
      hash   = @model.to_hash
      id     = hash.delete(:id)
      keys   = []
      values = []
      hash.each do |key, value|
        keys << "#{key.to_s.camelize}=?"
        values << value.to_s
      end
      keys   = keys.join(', ')
      update = "UPDATE '#{table}' SET #{keys} WHERE ID=#{id}"
      db.execute update, values
      {
        :update_response => {
          :result => {
            :success => true,
            :id => id
          }
        }
      }
    end

    def destroy
      table = self.class.table_name(@model.class)
      destroy = "DELETE FROM '#{table}' WHERE Id=?"
      db.execute destroy, @model.id
      {
        :delete_response => {
          :result => {
            :success => true,
            :id => @model.id
          }
        }
      }
    end

    def parse_attributes(type, attrs = {})
      data = attrs.to_a.map do |a|
        key, value = a
        [key.underscore, value]
      end
      Hash[data]
    end

    def self.build_schema
      self.db = SQLite3::Database.new ":memory:"
      self.generate_tables
    end

    def self.table_name(model)
      model.name.demodulize
    end

    protected

    def hash_result_row(row, result)
      row = row.map {|r| r.nil? ? "" : r }
      Hash[result.columns.zip(row.to_a)]
    end

    def self.generate_tables
      Zuora::Objects::Base.subclasses.each do |model|
        create_table(model)
      end
    end

    def self.create_table(model)
      table_name = self.table_name(model)
      attributes = model.attributes - [:id]
      attributes = attributes.map do |a|
        "'#{a.to_s.camelize}' text"
      end
      autoid = "'Id' integer PRIMARY KEY AUTOINCREMENT"
      attributes.unshift autoid
      attributes = attributes.join(", ")
      schema = "CREATE TABLE 'main'.'#{table_name}' (#{attributes});"
      db.execute schema
    end

  end
end
