module Zuora::Objects
  class ProductRatePlanChargeTier < Base
    belongs_to :product_rate_plan_charge

    # NOTE(omar): Missing discount_amount discount_percentage
    validates_presence_of :currency

    validates_inclusion_of :active, :in => [true, false]
    validates_inclusion_of :is_overage_price, :in => [true, false], :allow_nil => true
    validates_inclusion_of :price_format, :in => ['Flat Fee', 'Per Unit'], :allow_nil => true
    validates_numericality_of :price, :greater_than => 0

    define_attributes do
      read_only :created_date, :updated_date, :created_by_id, :updated_by_id, :tier
      restrain :starting_unit, :ending_unit, :is_overage_price, :price_format, :currency
      defaults :currency => 'USD'
    end

    @@special_attributes = [:price, :discount_amount, :discount_percentage]

    def self.get(query_locators = [])
      records = []
      new_query_locators = []
      found_locator = false
      query_locators.each do |query_locator|
        next if query_locator.nil? || query_locator.empty?
        found_locator = true
        result = self.connector.query_more(query_locator)
        result_hash = result.to_hash
        records += generate(result_hash, :query_more_response)
        new_query_locators << result_hash[:query_more_response][:result][:query_locator]
      end
      records, new_query_locators = where("", false) unless found_locator
      [records, new_query_locators]
    end

    # Zuora adds a limitation where we can only query for Price, DiscountAmount, or DiscoutnPercentage in a
    # query. Also, it looks like those queries only return records where the queried field is not NULL, and
    # only one of them can be not NULL. So, we need to create a query for each one and add up the results. I'm
    # hoping that exactly 1 is not NULL.
    def self.where(where, get_all = false)
      records = []
      query_locators = []
      shared_keys = (attributes - unselectable_attributes - @@special_attributes).map(&:to_s).map(&:camelcase)
      @@special_attributes.each do |special_attribute|
        keys = shared_keys + [special_attribute.to_s.camelcase]
        new_records, new_query_locator = where_keys(keys, where, get_all)
        records += new_records
        query_locators << new_query_locator unless new_query_locator.nil?
      end
      [records, query_locators]
    end
  end
end
