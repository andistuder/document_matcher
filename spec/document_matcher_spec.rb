$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)
require 'document_matcher'

describe DocumentMatcher do
  subject { described_class.new(params) }

  let(:account_recoveries_file_headers) do
    [:id, :account_id, :cashfac_id, :loan_part_id, :amount, :recovery_id, :created_at]
  end

  let(:transactions_file_headers) do
    [:id, :comment, :holder_reference, :amount_pennies, :loan_part_id, :created_at]
  end

  let(:output_headers) { [:id, :holder_reference, :old_amount, :new_amount] }

  let(:log_file_name) { '/tmp/recovery_reconciler.log' }
  let(:output_file_name) { '/tmp/recovery_transactions_to_fix.csv' }
  let(:account_recoveries_file_name) { 'account_recoveries_sample_data.csv' }
  let(:transactions_file_name) { 'transactions_sample_data.csv' }

  describe '#match_transactions_to_account_recoveries' do
    let(:params) {
      {
        'account_recoveries_file_name' => account_recoveries_file_name,
        'account_recoveries_file_headers' => account_recoveries_file_headers,
        'transactions_file_name' => transactions_file_name,
        'transactions_file_headers' => transactions_file_headers,
        'log_file_name' => log_file_name,
        'output_file_name' => output_file_name,
        'output_headers' => output_headers
      }
    }

    before do
      allow(subject).to receive(:publish_log)
      allow(CSV).to receive(:foreach).with(transactions_file_name,headers:false).and_yield(transaction_row)
    end
    
    context 'when one transaction match' do

      let(:account_recovery_row) do
        [14,1994,"000000ASDFGHJK123680",34108,39.44,nil,"2013-09-20 11:16:16.893944"]
      end

      let(:transaction_row) do
        [77755888,"Recovery payment for item 1234567","000000ASDFGHJK123680",3598,34108,"2013-09-20 11:16:16.893944"]
      end

      let(:expected_transaction) do
        {:id=>77755888, :holder_reference=>"000000ASDFGHJK123680", :amount_pennies=>3598, :loan_part_id=>34108, :created_at_day=>15968}
      end

      before do
        allow(CSV).to receive(:foreach).with(account_recoveries_file_name,headers:false).and_yield(account_recovery_row)
      end

      it 'reports a reconciliation issue' do
        expect(subject).to receive(:export_recovery_transactions).with(expected_transaction, 3944)
        subject.match_transactions_to_account_recoveries
      end
    end

    context 'when more than one transaction match' do
      let(:account_recovery_row) do
        [14,1994,"000000ASDFGHJK123680",34108,39.44,nil,"2013-09-20 11:16:16.893944"]
      end

      let(:account_recovery_row2) do
        [18,1994,"000000ASDFGHJK123680",34108,19.57,nil,"2013-09-20 11:16:16.893944"]
      end

      let(:transaction_row) do
        [77755888,"Recovery payment for item 1234567","000000ASDFGHJK123680",3598,34108,"2013-09-20 11:16:16.893944"]
      end

      let(:expected_msg) { "WARNING: more than one match but not one has a matching amount, transaction_id: 77755888" }

      before do
        allow(CSV).to receive(:foreach).with(account_recoveries_file_name,headers:false).and_yield(account_recovery_row).and_yield(account_recovery_row2)
        allow(subject).to receive(:publish_log)
      end

      it 'reports a reconciliation issue' do
        subject.match_transactions_to_account_recoveries
        expect(subject).to have_received(:publish_log).with(expected_msg).once
      end
    end

    context 'when no transaction match' do
      let(:account_recovery_row) do
        [14,1994,"000000ASDFGHJK123680",34108,39.44,nil,"2013-09-20 11:16:16.893944"]
      end

      let(:transaction_row) do
        [77755888,"Recovery payment for item 1234567","000000ASDFGHJK123680",3598,34108,"2012-09-20 11:16:16.893944"]
      end

      let(:expected_msg) { "WARNING: no matching recovery found for transaction_id: 77755888" }

      before do
        allow(CSV).to receive(:foreach).with(account_recoveries_file_name,headers:false).and_yield(account_recovery_row)
      end

      it 'reports no matching recovery' do
        subject.match_transactions_to_account_recoveries
        expect(subject).to have_received(:publish_log).with(expected_msg).once
      end
    end
  end
end
