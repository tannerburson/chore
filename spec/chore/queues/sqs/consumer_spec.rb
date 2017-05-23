require 'spec_helper'

describe Chore::Queues::SQS::Consumer do
  let(:queue_name) { "test" }
  let(:queue_url) { "test_url" }
  let(:queue) { double("test_queue", :visibility_timeout=>10, :url=>"test_queue", :name=>"test_queue") }
  let(:options) { {} }
  let(:consumer) { Chore::Queues::SQS::Consumer.new(queue_name) }
  let(:message) { double("message list", :messages => [TestMessage.new("handle", "message body")]) }
  let(:message_data) {{:id=>message.messages[0].message_id, :queue=>queue_url, :visibility_timeout=>message.messages[0].attributes['VisibilityTimeout']}}
  let(:pool) { double("pool") }
  let(:sqs) { double('Aws::SQS::Client', :get_queue_url => double(:dumb, :queue_url => queue_url), :receive_message => message) }
  let(:backoff_func) { nil }

  before do
    allow(Aws::SQS::Client).to receive(:new).and_return(sqs)
    allow(pool).to receive(:empty!) { nil }
  end

  describe "consuming messages" do
    let!(:consumer_run_for_one_message) { allow(consumer).to receive(:running?).and_return(true, false) }
    let!(:messages_be_unique) { allow_any_instance_of(Chore::DuplicateDetector).to receive(:found_duplicate?).and_return(false) }
    let!(:queue_contain_messages) { allow(sqs).to receive(:receive_message).and_return(message) }

    # Should probably rely purely on external configuration
    xit 'should configure sqs' do
      allow(Chore.config).to receive(:aws_access_key).and_return('key')
      allow(Chore.config).to receive(:aws_secret_key).and_return('secret')

      expect(Aws::SQS::Client).to receive(:new).with(
        :access_key_id => 'key',
        :secret_access_key => 'secret'
      ).and_return(sqs)
      consumer.consume
    end

    it 'should not configure sqs multiple times' do
      allow(consumer).to receive(:running?).and_return(true, true, false)

      expect(Aws::SQS::Client).to receive(:new).once.and_return(sqs)
      consumer.consume
    end

    context "should receive a message from the queue" do

      it 'should use the default size of 10 when no queue_polling_size is specified' do
        expect(sqs).to receive(:receive_message).with(queue_url: queue_url, max_number_of_messages: 10, attribute_names: anything())
        consumer.consume
      end

      it 'should respect the queue_polling_size when specified' do
        allow(Chore.config).to receive(:queue_polling_size).and_return(5)
        expect(sqs).to receive(:receive_message).with(queue_url: queue_url, max_number_of_messages: 5, attribute_names: anything())
        consumer.consume
      end
    end

    it "should check the uniqueness of the message" do
      allow_any_instance_of(Chore::DuplicateDetector).to receive(:found_duplicate?).with(message_data).and_return(false)
      consumer.consume
    end

    it "should yield the message to the handler block" do
      allow_any_instance_of(Chore::DuplicateDetector).to receive(:found_duplicate?).and_return(false)
      expect { |b| consumer.consume(&b) }.to yield_with_args('handle', queue_name, 10, 'message body', 0)
    end

    it 'should not yield for a dupe message' do
      allow_any_instance_of(Chore::DuplicateDetector).to receive(:found_duplicate?).and_return(true)
      expect {|b| consumer.consume(&b) }.not_to yield_control
    end

    context 'with no messages' do
      let!(:consumer_run_for_one_message) { allow(consumer).to receive(:running?).and_return(true, true, false) }
      let!(:queue_contain_messages) { allow(sqs).to receive(:receive_message).and_return(message, double(:resp,messages:[])) }

      it 'should sleep' do
        expect(consumer).to receive(:sleep).with(1)
        consumer.consume
      end
    end

    context 'with messages' do
      let!(:consumer_run_for_one_message) { allow(consumer).to receive(:running?).and_return(true, true, false) }
      let!(:queue_contain_messages) { allow(sqs).to receive(:receive_message).and_return(message, message) }

      it 'should not sleep' do
        expect(consumer).to_not receive(:sleep)
        consumer.consume
      end
    end
  end

  describe '#delay' do
    let(:item) { Chore::UnitOfWork.new(message.messages[0].message_id, queue_url, 60, message.messages[0].body, 0, consumer) }
    let(:backoff_func) { lambda { |item| 2 } }

    it 'changes the visiblity of the message' do
      expect(sqs).to receive(:change_visibility).with(queue_url: queue_url, receipt_handle: item.id,visibility_timeout: 2)
      consumer.delay(item, backoff_func)
    end
  end

  describe '#reset_connection!' do
    it 'should reset the connection after a call to reset_connection!' do
      expect(Seahorse::Client::NetHttp::ConnectionPool).to receive(:pools).and_return([pool])
      expect(pool).to receive(:empty!)
      Chore::Queues::SQS::Consumer.reset_connection!
      consumer.send(:sqs)
    end

    it 'should not reset the connection between calls' do
      sqs = consumer.send(:sqs)
      expect(sqs).to be consumer.send(:sqs)
    end

    it 'should reconfigure sqs' do
      allow(consumer).to receive(:running?).and_return(true, false)
      allow_any_instance_of(Chore::DuplicateDetector).to receive(:found_duplicate?).and_return(false)

      allow(sqs).to receive(:receive_message).and_return(message)
      consumer.consume

      Chore::Queues::SQS::Consumer.reset_connection!

      expect(consumer).to receive(:running?).and_return(true, false)
      consumer.consume
    end
  end

  describe 'aws logging' do
    it 'should not set Aws logging if Chore log level is info' do
      allow(Chore.config).to receive(:log_level).and_return(Logger::INFO)

      allow(consumer).to receive(:running?).and_return(true, false)
      allow(sqs).to receive(:receive_message).and_return(message)

      expect(Aws::SQS::Client).to receive(:new).with(hash_not_including(:log_level => :info))
      consumer.consume
    end

    it 'should set the Aws logging if Chore log level is debug' do
      allow(Chore.config).to receive(:log_level).and_return(Logger::DEBUG)

      allow(consumer).to receive(:running?).and_return(true, false)
      allow(sqs).to receive(:receive_message).and_return(message)

      expect(Aws::SQS::Client).to receive(:new).with(hash_including(:logger => Chore.logger, :log_level => :debug))
      consumer.consume
    end
  end
end
