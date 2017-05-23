require 'chore/publisher'

module Chore
  module Queues
    module SQS

      # SQS Publisher, for writing messages to SQS from Chore
      class Publisher < Chore::Publisher
        @@reset_next = true

        def initialize(opts={})
          super
          @sqs_queue_urls = {}
        end

        # Takes a given Chore::Job instance +job+, and publishes it by looking up the +queue_name+.
        def publish(queue_name,job)
          sqs.send_message(
            queue_url: url_for(queue_name),
            message_body: encode_job(job)
          )
        end

        # Sets a flag that instructs the publisher to reset the connection the next time it's used
        def self.reset_connection!
          @@reset_next = true
        end

        # Access to the configured SQS connection object
        def sqs
         if @@reset_next
            Seahorse::Client::NetHttp::ConnectionPool.pools.each do |p|
              p.empty!
            end
            @sqs = nil
            @@reset_next = false
            @sqs_queue_urls = {}
          end
          @sqs ||= Aws::SQS::Client.new
        end

        private
        def url_for(name)
          @sqs_queue_urls[name] ||= sqs.get_queue_url(queue_name: name).queue_url
        end
      end
    end
  end
end
