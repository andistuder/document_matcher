#!/usr/bin/env ruby
require 'csv'
require 'date'
require 'redis'
require 'oj'
require 'hiredis'

class DocumentMatcher
  attr_accessor :account_recoveries_file_name,
    :account_recoveries_file_headers,
    :transactions_file_name,
    :transactions_file_headers,
    :log_file_name,
    :output_file_name,
    :output_headers

  EPOCH = Date.new(1970,1,1).freeze

  def initialize(params_hash)
    params_hash.each do |k,v|
      self.send("#{k}=", v)
    end
    get_empty_db
    set_output_file_headers
  end

  def match_transactions_to_account_recoveries
    load_account_recoveries_from_csv

    i = 0
    CSV.foreach(transactions_file_name, :headers => false) do |row|
      i += 1

      transaction = process_bilcas_csv_row(row)
      matching_recoveries = find_account_recoveries(transaction[:created_at_day], transaction[:loan_part_id], transaction[:holder_reference])

      prcoess_matching_records(transaction, matching_recoveries)
    end

    truncate_db
    publish_log "Processed #{i} transactions"
  end

  private
  attr_reader :redis_client

  def load_account_recoveries_from_csv
    i = 0
    record = {}

    redis_client.pipelined do

      CSV.foreach(account_recoveries_file_name, :headers => false) do |row|
        i += 1

        record = process_csv_row(row, account_recoveries_file_headers)
        json_record = Oj.dump(record)

        redis_client.sadd "date:#{record[:created_at_day]}:account_recoveries", json_record
        redis_client.sadd "loan_part:#{record[:loan_part_id]}:account_recoveries", json_record
        redis_client.sadd "account:#{record[:cashfac_id]}:account_recoveries", json_record
      end
    end

    publish_log "FCA account recoveries count: #{i}, sample: \n"
    publish_log record.to_s
  end

  def process_bilcas_csv_row(row)
    process_csv_row(row, transactions_file_headers)
  end

  def process_csv_row(row, headers)
    record = Hash[headers.zip(row)]

    record.delete(:comment)
    record.delete(:account_id)
    record.delete(:recovery_id)

    headers.each do | header |
      if header.to_s.match('id$') && record[header].to_i > 0
        record[header] = record[header].to_i
      end
    end

    created_at = Date.parse(record.delete(:created_at))
    record[:created_at_day] = (created_at - EPOCH).to_i

    record[:amount_pennies] = record[:amount_pennies].to_i if record[:amount_pennies]
    record[:amount_pennies] = (record.delete(:amount).to_f * 100).round if record[:amount]
    record
  end

  def find_account_recoveries(created_at_day, loan_part_id, holder_reference)
    documents = redis_client.sinter "date:#{created_at_day}:account_recoveries",
      "loan_part:#{loan_part_id}:account_recoveries",
      "account:#{holder_reference}:account_recoveries"
    documents.map {|d| Oj.load(d)}
  end

  def prcoess_matching_records(transaction, matching_recoveries)

    if matching_recoveries.count == 1
      matching_recovery = matching_recoveries.first

      if matching_recovery[:amount_pennies] != transaction[:amount_pennies]
        publish_log "Found reconciliation issue transaction_id: #{transaction[:id]} FCA: #{matching_recovery[:amount_pennies]} Bilcas: #{transaction[:amount_pennies]}"
        export_recovery_transactions(transaction, matching_recovery[:amount_pennies])
      end

    elsif matching_recoveries.count > 1

      found_match = false
      matching_recoveries.each do | recovery |
        if recovery[:amount_pennies] == transaction[:amount_pennies]
          purge_account_recovery(recovery)
          found_match = true
          break
        end
      end
      publish_log "WARNING: more than one match but not one has a matching amount, transaction_id: #{transaction[:id]}" unless found_match

    else
      publish_log "WARNING: no matching recovery found for transaction_id: #{transaction[:id]}"
    end
  end

  def set_output_file_headers
    CSV.open(output_file_name, 'wb') { |csv| csv << output_headers }
  end

  def publish_log(log_entry)
    File.open(log_file_name, 'ab') {|log| log.puts log_entry }
    # puts log_entry
  end

  def purge_account_recovery(recovery)
    record = Oj.dump(recovery)
    redis_client.srem "date:#{recovery[:created_at_day]}:account_recoveries", record
    redis_client.srem "loan_part:#{recovery[:loan_part_id]}:account_recoveries", record
    redis_client.srem "account:#{recovery[:holder_reference]}:account_recoveries", record
  end

  def export_recovery_transactions(transaction, new_amount)
    transaction_entry = [transaction[:id], transaction[:holder_reference], transaction[:amount_pennies], new_amount]
    CSV.open(output_file_name, 'ab') {|csv| csv << transaction_entry}
  end

  def get_empty_db
    redis_db_index = 0
    loop do
      @redis_client = Redis.new(driver: :hiredis, db:redis_db_index)
      break if @redis_client.dbsize == 0
      redis_db_index += 1
    end
    publish_log "Connected to redis db #{redis_db_index}"
  end

  def truncate_db
    redis_client.flushdb
  end
end
