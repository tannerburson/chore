require 'aws-sdk-core'
require 'chore/duplicate_detector'

Aws.eager_autoload!(services: %w(SQS))

module Chore
  module Queues
    module SQS
      # SQS Consumer for Chore. Requests messages from SQS and passes them to be worked on. Also controls
      # deleting completed messages within SQS.
      class Consumer < Chore::Consumer
        # Initialize the reset at on class load
        @@reset_at = Time.now

        Chore::CLI.register_option 'dedupe_servers', '--dedupe-servers SERVERS', 'List of mememcache compatible server(s) to use for storing SQS Message Dedupe cache'
        Chore::CLI.register_option 'queue_polling_size', '--queue_polling_size NUM', Integer, 'Amount of messages to grab on each request' do |arg|
          raise ArgumentError, "Cannot specify a queue polling size greater than 10" if arg > 10
        end

        def initialize(queue_name, opts={})
          super(queue_name, opts)
        end

        # Sets a flag that instructs the publisher to reset the connection the next time it's used
        def self.reset_connection!
          @@reset_at = Time.now
        end

        # Begins requesting messages from SQS, which will invoke the +&handler+ over each message
        def consume(&handler)
          while running?
            begin
              messages = handle_messages(&handler)
              sleep (Chore.config.consumer_sleep_interval || 1) if messages.empty?
            rescue Aws::SQS::Errors::NonExistentQueue => e
              Chore.logger.error "You specified a queue '#{queue_name}' that does not exist. You must create the queue before starting Chore. Shutting down..."
              raise Chore::TerribleMistake
            rescue => e
              Chore.logger.error { "SQSConsumer#Consume: #{e.inspect} #{e.backtrace * "\n"}" }
            end
          end
        end

        # Rejects the given message from SQS by +id+. Currently a noop
        def reject(id)

        end

        # Deletes the given message from SQS by +id+
        def complete(id)
          Chore.logger.debug "Completing (deleting): #{id}"
          sqs.delete_message(
            queue_url: queue_url,
            receipt_handle: id
          )
        end

        def delay(item, backoff_calc)
          delay = backoff_calc.call(item)
          Chore.logger.debug "Delaying #{item.id} by #{delay} seconds"
          sqs.change_visibility(
            queue_url: queue_url,
            receipt_handle: item.id,
            visibility_timeout: delay
          )

          return delay
        end

        private

        # Requests messages from SQS, and invokes the provided +&block+ over each one. Afterwards, the :on_fetch
        # hook will be invoked, per message
        def handle_messages(&block)
          resp = sqs.receive_message(
              queue_url: queue_url,
              max_number_of_messages: sqs_polling_amount,
              attribute_names: ['ApproximateReceiveCount', 'QueueArn', 'VisibilityTimeout'])
          messages = resp.messages
          messages.each do |message|
            unless duplicate_message?(message)
              block.call(message.receipt_handle, queue_name, message.attributes['VisibilityTimeout'], message.body, message.attributes['ApproximateReceiveCount'].to_i - 1)
            end
            Chore.run_hooks_for(:on_fetch, message.receipt_handle, message.body)
          end
          messages
        end

        # Checks if the given message has already been received within the timeout window for this queue
        def duplicate_message?(message)
          dupe_detector.found_duplicate?(id: message.message_id, queue: message.attributes['QueueArn'], visibility_timeout: message.attributes['VisibilityTimeout'])
        end

        # Returns the instance of the DuplicateDetector used to ensure unique messages.
        # Will create one if one doesn't already exist
        def dupe_detector
          @dupes ||= DuplicateDetector.new({:servers => Chore.config.dedupe_servers,
                                            :dupe_on_cache_failure => Chore.config.dupe_on_cache_failure})
        end

        def queue_url
          @queue_url ||= sqs.get_queue_url(queue_name: queue_name).queue_url
        end

        # Access to the configured SQS connection object
        def sqs
          if !@sqs_last_connected || (@@reset_at && @@reset_at >= @sqs_last_connected)
            Seahorse::Client::NetHttp::ConnectionPool.pools.each do |p|
              p.empty!
            end
            @sqs = nil
            @sqs_last_connected = Time.now
            @queue_url = nil
          end

          @sqs ||= Aws::SQS::Client.new(logger: Chore.logger, log_level: :debug)
        end

        def sqs_polling_amount
          Chore.config.queue_polling_size || 10
        end
      end
    end
  end
end
