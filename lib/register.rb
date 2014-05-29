# coding: UTF-8
require "nats/client"
require "bridge_client"
require "vcap/logging"
require "const"
require "config"

module Register
  class Broker
    attr_reader :config
    attr_reader :logger
    attr_reader :bridge_client

    def initialize(config)
      @config = Config.new(config)
      ['TERM', 'INT', 'QUIT'].each do |s| 
        trap(s) { shutdown() } 
      end
    end

    def shutdown
      logger.info("EXITTING broker...")
      exit!
    end

    def validate_config
      config.validate
    end

    def setup
      validate_config
      setup_logger
      setup_nats
      setup_bridge_client(@config, logger)
    end

    def setup_logger
      VCAP::Logging.setup_from_config(config['logging'])
      @logger = VCAP::Logging.logger('broker')
    end

    def setup_nats
      nats_uri = config['mbus']

      NATS.on_error do |e|
        logger.error("EXITING! NATS error: #{e}")
	    exit!
      end

      #EM.error_handler do |e|
	  #  logger.error "Eventmachine problem, #{e}"
      #end 

      NATS.start(:uri => nats_uri) do
	    NATS.subscribe('broker.register', :queue => :bk) { |msg| 
          instance = Yajl::Parser.parse(msg)
          logger.debug("received message #{instance}")
          bridge_client.request( instance, { :action => ACTION_REGISTER} )
        }

        NATS.subscribe('broker.unregister',:queue => :bk) { |msg| 
          instance = Yajl::Parser.parse(msg)
          logger.debug("received unregister message #{instance}")
          bridge_client.request( instance, { :action => ACTION_UNREGISTER} )
        }
      end
    end

    def setup_bridge_client(config, logger)
      @bridge_client = Register::BridgeClient.new(config, logger)
    end 

    def start
      logger.info("Register manager started.")
    end
  end
end

