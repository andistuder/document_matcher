#!/usr/bin/env ruby
$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'document_matcher'

params = {}
params['fca_account_recoveries_file_name'] = '/tmp/account_recoveries.csv'
params['fca_account_recoveries_file_headers'] = [:id, :account_id, :cashfac_id, :loan_part_id, :amount, :recovery_id, :created_at]
params['bilcas_transactions_file_name'] = '/tmp/transactions.csv'
params['bilcas_transactions_file_headers'] = [:id, :comment, :holder_reference, :amount_pennies, :loan_part_id, :created_at]
params['log_file_name'] = '/tmp/document_matcher.log'
params['output_file_name'] = '/tmp/matched_transactions.csv'
params['output_headers'] = [:id, :holder_reference, :old_amount, :new_amount]

matcher = DocumentMatcher.new(params)
matcher.match_transactions_to_account_recoveries