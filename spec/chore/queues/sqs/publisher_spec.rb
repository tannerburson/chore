require 'spec_helper'

module Chore
  describe Queues::SQS::Publisher do
    let(:job) { {'class' => 'TestJob', 'args'=>[1,2,'3']}}
    let(:queue_name) { 'test_queue' }
    let(:queue_url) {"http://www.queue_url.com/test_queue"}
    let(:queue) { double('queue', :send_message => nil) }
    let(:sqs) do
      double('sqs', :queues => double('queues', :named => queue, :url_for => queue_url, :[] => queue))
    end
    let(:publisher) { Queues::SQS::Publisher.new }
    let(:pool) { double("pool") }

    before(:each) do
      Aws::SQS.stub(:new).and_return(sqs)
    end

    it 'should configure sqs' do
      Chore.config.stub(:aws_access_key).and_return('key')
      Chore.config.stub(:aws_secret_key).and_return('secret')

      Aws::SQS.should_receive(:new).with(
        :access_key_id => 'key',
        :secret_access_key => 'secret'
      )
      publisher.publish(queue_name,job)
    end

    it 'should create send an encoded message to the specified queue' do
      queue.should_receive(:send_message).with(job.to_json)
      publisher.publish(queue_name,job)
    end

    it 'should lookup the queue when publishing' do
      sqs.queues.should_receive(:url_for).with('test_queue')
      publisher.publish('test_queue', job)
    end

    it 'should lookup multiple queues if specified' do
      sqs.queues.should_receive(:url_for).with('test_queue')
      sqs.queues.should_receive(:url_for).with('test_queue2')
      publisher.publish('test_queue', job)
      publisher.publish('test_queue2', job)
    end

    it 'should only lookup a named queue once' do
      sqs.queues.should_receive(:url_for).with('test_queue').once
      2.times { publisher.publish('test_queue', job) }
    end

    describe '#reset_connection!' do
      it 'should reset the connection after a call to reset_connection!' do
        Seahorse::Client::NetHttp::ConnectionPool.stub(:pools).and_return([pool])
        pool.should_receive(:empty!)
        Chore::Queues::SQS::Publisher.reset_connection!
        publisher.queue(queue_name)
      end

      it 'should not reset the connection between calls' do
        sqs = publisher.queue(queue_name)
        sqs.should be publisher.queue(queue_name)
      end

      it 'should reconfigure sqs' do
        Chore::Queues::SQS::Publisher.reset_connection!
        Aws::SQS.should_receive(:new)
        publisher.queue(queue_name)
      end
    end
  end
end
