module Chore
  class Fetcher #:nodoc:
    attr_reader :manager, :consumers

    def initialize(manager)
      @stopping = false
      @manager = manager
      @strategy = Chore.config.consumer_strategy.new(self)
    end

    # Starts the fetcher with the configured Consumer Strategy. This will begin consuming messages from your queue
    def start
      Chore.logger.info "Fetcher starting up"

      # Clean up configured queues in case there are any resources left behind
      Chore.config.queues.each do |queue|
        Chore.config.consumer.cleanup(queue)
      end

      @strategy.fetch
    end

    # Stops the fetcher, preventing any further messages from being pulled from the queue
    def stop!
      unless @stopping
        Chore.logger.info "Fetcher shutting down"
        @stopping = true
        @strategy.stop!
      end
    end

    # Determines in the fetcher is in the process of stopping
    def stopping?
      @stopping
    end
  end
end
