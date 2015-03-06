require 'pp'
require 'singleton'
require 'optparse'
require 'chore'
require 'erb'
require 'set'

require 'chore/manager'

module Chore #:nodoc:

  # Class that handles the command line interactions in Chore.
  # It primarily is responsible for invoking the Chore process with the provided configuration
  # to begin processing jobs.
  class CLI
    include Singleton
    include Util

    attr_reader :options, :registered_opts

    def initialize
      @options = {}
      @registered_opts = {}
      @stopping = false
    end

    #
    # +register_option+ is a method for plugins or other components to register command-line config options.
    # * <tt>key</tt> is the name for this option that can be referenced from Chore.config.+key+
    # * <tt>*args</tt> is an <tt>OptionParser</tt> style list of options.
    # * <tt>&blk</tt> is an option block, passed to <tt>OptionParser</tt>
    #
    # === Examples
    #   Chore::CLI.register_option 'sample', '-s', '--sample-key SOME_VAL', 'A description of this value'
    #
    #   Chore::CLI.register_option 'something', '-g', '--something-complex VALUE', 'A description' do |arg|
    #     # make sure your key here matches the key you register
    #     options[:something] arg.split(',')
    #   end
    def self.register_option(key,*args,&blk)
      instance.register_option(key,*args,&blk)
    end

    def register_option(key,*args,&blk) #:nodoc:
      registered_opts[key] = {:args => args}
      registered_opts[key].merge!(:block => blk) if blk
    end

    # Start up the consuming side of the application. This calls Chore::Manager#start.
    def run!(args=ARGV)
      parse(args)
      @manager = Chore::Manager.new
      @manager.start
    end

    # Begins the Chore shutdown process. This will call Chore::Manager#shutdown if it is not already in the process of stopping
    # Exits with code 0
    def shutdown
      unless @stopping
        @stopping = true
        @manager.shutdown! if @manager
        exit(0)
      end
    end

    def parse_config_file(file) #:nodoc:
      data = File.read(file)
      data = ERB.new(data).result
      parse_opts(data.split(/\s/).map!(&:chomp).map!(&:strip))
    end

    def parse(args=ARGV) #:nodoc:
      Chore.configuring = true
      setup_options

      # parse once to load the config file & require options
      parse_opts(args)
      parse_config_file(@options[:config_file]) if @options[:config_file]

      validate!
      boot_system

      # parse again to pick up options required by loaded classes
      parse_opts(args)
      parse_config_file(@options[:config_file]) if @options[:config_file]
      detect_queues
      Chore.configure(options)
      Chore.configuring = false
    end


    private
    def setup_options #:nodoc:
      register_option "queues", "-q", "--queues QUEUE1,QUEUE2", "Names of queues to process (default: all known)" do |arg|
        options[:queues] = arg.split(",")
      end

      register_option "except_queues", "-x", "--except QUEUE1,QUEUE2", "Process all queues (cannot specify --queues), except for the ones listed here" do |arg|
        options[:except_queues] = arg.split(",")
      end

      register_option "verbose", "-v", "--verbose", "Print more verbose output. Use twice to increase." do
        options[:log_level] ||= Logger::WARN
        options[:log_level] = options[:log_level] - 1 if options[:log_level] > 0
      end

      register_option "environment", '-e', '--environment ENV', "Application environment"

      register_option "config_file", '-c', '--config-file FILE', "Location of a file specifying additional chore configuration"

      register_option 'require', '-r', '--require [PATH|DIR]', "Location of Rails application with workers or file to require"

      register_option 'num_workers', '--concurrency NUM', Integer, 'Number of workers to run concurrently'

      register_option 'queue_prefix', '--queue-prefix PREFIX', "Prefix to use on Queue names to prevent non-determinism in testing environments" do |arg|
        options[:queue_prefix] = arg.downcase << "_"
      end

      register_option 'max_attempts', '--max-attempts NUM', Integer, 'Number of times to attempt failed jobs'

      register_option 'worker_strategy', '--worker-strategy CLASS_NAME', 'Name of a class to use as the worker strategy (default: ForkingStrategy' do |arg|
        options[:worker_strategy] = constantize(arg)
      end

      register_option 'consumer', '--consumer CLASS_NAME', 'Name of a class to use as the queue consumer (default: SqsConsumer)' do |arg|
        options[:consumer] = constantize(arg)
      end

      register_option 'consumer_strategy', '--consumer-strategy CLASS_NAME', 'Name of a class to use as the consumer strategy (default: Chore::Strategy::Fetching::ThreadedConsumerStrategy' do |arg|
        options[:consumer_strategy] = constantize(arg)
      end

      register_option 'shutdown_timeout', '--shutdown-timeout SECONDS', Float, "Upon shutdown, the number of seconds to wait before force killing worker strategies (default: #{Chore::DEFAULT_OPTIONS[:shutdown_timeout]})"

      register_option 'dupe_on_cache_failure', '--dupe-on-cache-failure BOOLEAN', 'Determines the deduping behavior when a cache connection error occurs. When set to false, the message is assumed not to be a duplicate. (default: false)'

    end

    def parse_opts(argv) #:nodoc:
      @options ||= {}
      @parser = OptionParser.new do |o|
        registered_opts.each do |key,opt|
          if opt[:block]
            o.on(*opt[:args],&opt[:block])
          else
            o.on(*opt[:args]) do |arg|
              options[key.to_sym] = arg
            end
          end
        end
      end

      @parser.banner = "chore [options]"

      @parser.on_tail "-h", "--help", "Show help" do
        puts @parser
        exit 1
      end

      @parser.parse!(argv)

      @options
    end


    def detected_environment #:nodoc:
      options[:environment] ||= ENV['RAILS_ENV'] || ENV['RACK_ENV'] || 'development'
    end

    def boot_system #:nodoc:
      ENV['RACK_ENV'] = ENV['RAILS_ENV'] = detected_environment

      raise ArgumentError, "#{options[:require]} does not exist" unless File.exist?(options[:require])

      if File.directory?(options[:require])
        require 'rails'
        require 'chore/railtie'
        require File.expand_path("#{options[:require]}/config/environment.rb")
        ::Rails.application.eager_load!
      else
        require File.expand_path(options[:require])
      end
    end

    def detect_queues #:nodoc:
      if (options[:queues] && options[:except_queues])
        raise ArgumentError, "Cannot specify both --except and --queues"
      end

      if !options[:queues]
        options[:queues] = Set.new
        Chore::Job.job_classes.each do |j|
          klazz = constantize(j)
          options[:queues] << klazz.options[:name] if klazz.options[:name]
          options[:queues] -= (options[:except_queues] || [])
        end
      end

      original_queues = options[:queues].dup
      # Now apply the prefixing
      # Because the prefix could have been detected via the apps chore config file
      # Lets see if that is present before we check for a CLI passed prefix
      prefix = Chore.config.queue_prefix || options[:queue_prefix]
      options[:queues] = Set.new.tap do |queue_set|
        original_queues.each {|oq_name| queue_set << "#{prefix}#{oq_name}"}
      end
      raise ArgumentError, "No queues specified. Either include classes that include Chore::Job, or specify the --queues option" if options[:queues].empty?
    end

    def missing_option!(option) #:nodoc:
      puts "Missing argument: #{option}"
      exit(255)
    end

    def validate! #:nodoc:

      missing_option!("--require [PATH|DIR]") unless options[:require]

      if !File.exist?(options[:require]) ||
         (File.directory?(options[:require]) && !File.exist?("#{options[:require]}/config/application.rb"))
        puts "=================================================================="
        puts "  Please point chore to a Rails 3 application or a Ruby file    "
        puts "  to load your worker classes with -r [DIR|FILE]."
        puts "=================================================================="
        puts @parser
        exit(1)
      end

    end
  end
end

