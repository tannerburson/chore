require 'spec_helper'

module Chore
  describe Queues::SQS::Publisher do
    let(:job) { {'class' => 'TestJob', 'args'=>[1,2,'3']}}
    let(:queue_name) { 'test_queue' }
    let(:queue_url) {"http://www.queue_url.com/test_queue"}
    let(:sqs) do
      double('sqs', :send_message => nil, :get_queue_url => double(:queue_resp,:queue_url => queue_url))
    end
    let(:publisher) { Queues::SQS::Publisher.new }
    let(:pool) { double("pool") }

    before(:each) do
      Aws::SQS::Client.stub(:new).and_return(sqs)
    end

    xit 'should configure sqs' do
      Chore.config.stub(:aws_access_key).and_return('key')
      Chore.config.stub(:aws_secret_key).and_return('secret')

      Aws::SQS.should_receive(:new).with(
        :access_key_id => 'key',
        :secret_access_key => 'secret'
      )
      publisher.publish(queue_name,job)
    end

    it 'should create send an encoded message to the specified queue' do
      sqs.should_receive(:send_message).with(hash_including(queue_url: queue_url, message_body: job.to_json))
      publisher.publish(queue_name,job)
    end

    it 'should lookup the queue when publishing' do
      sqs.should_receive(:get_queue_url).with(hash_including(queue_name: 'test_queue'))
      publisher.publish('test_queue', job)
    end

    it 'should lookup multiple queues if specified' do
      sqs.should_receive(:get_queue_url).with(hash_including(queue_name: 'test_queue'))
      sqs.should_receive(:get_queue_url).with(hash_including(queue_name: 'test_queue2'))
      publisher.publish('test_queue', job)
      publisher.publish('test_queue2', job)
    end

    it 'should only lookup a named queue once' do
      sqs.should_receive(:get_queue_url).with(hash_including(queue_name: 'test_queue')).once
      2.times { publisher.publish('test_queue', job) }
    end

    describe '#reset_connection!' do
      it 'should reset the connection after a call to reset_connection!' do
        Seahorse::Client::NetHttp::ConnectionPool.stub(:pools).and_return([pool])
        pool.should_receive(:empty!)
        Chore::Queues::SQS::Publisher.reset_connection!
        publisher.sqs
      end

      it 'should not reset the connection between calls' do
        sqs = publisher.sqs
        sqs.should be publisher.sqs
      end

      it 'should reconfigure sqs' do
        Chore::Queues::SQS::Publisher.reset_connection!
        Aws::SQS::Client.should_receive(:new)
        publisher.sqs
      end
    end
  end
end
