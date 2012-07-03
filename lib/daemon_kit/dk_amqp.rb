require 'yaml'
require 'amqp'

module DaemonKit
  # Thin wrapper around the amqp gem, specifically designed to ease
  # configuration of a AMQP consumer daemon and provide some added
  # simplicity
  class AMQP

    @@instance = nil

    class << self

      def instance
        @instance ||= new
      end

      private :new

      def run(&block)
        instance.run(&block)
      end
    end

    def initialize( config = {} )
      @config = DaemonKit::Config.load('amqp').to_h( true )
    end

    def run(&block)
      # Ensure graceful shutdown of the connection to the broker
      DaemonKit.trap('INT') {
        DaemonKit.logger.warn "======================================================================================================================="
        DaemonKit.logger.warn "INTERRUPT SIGNAL RECEIVED!  Please be patient..   Server will gracefully shut down after in-progress work is completed!"
        DaemonKit.logger.warn "======================================================================================================================="
        ::AMQP.stop { ::EM.stop }
      }

      DaemonKit.trap('TERM') {
        DaemonKit.logger.warn "======================================================================================================================="
        DaemonKit.logger.warn "TERMINATE SIGNAL RECEIVED!  Please be patient..   Server will gracefully shut down after in-progress work is completed!"
        DaemonKit.logger.warn "======================================================================================================================="
        ::AMQP.stop { ::EM.stop }
      }

      # Start our event loop and AMQP client
      DaemonKit.logger.debug("AMQP.start(#{@config.inspect})")

      # Do we need to ssh tunnel to the AMQP server?
      if @config.has_key? :ssh_user
        require 'net/ssh/gateway'
        gateway = Net::SSH::Gateway.new(
          @config[:ssh_host], @config[:ssh_user]
        )
        gateway.open(@config[:host], @config[:port], @config[:port])
      end

      # Log AMPQ connection failures
      tcp_connection_failure_handler = Proc.new { |settings| 
        DaemonKit.logger.error "Failed to connect to #{settings[:host]}";
        ::EM.stop
      }
      @config[:on_tcp_connection_failure] = tcp_connection_failure_handler

      ::AMQP.start(@config, &block)
    end
  end
end
