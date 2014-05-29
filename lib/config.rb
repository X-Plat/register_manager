require "membrane"

module Register
  class Config
      EMPTY_CONFIG = {}

      def self.schema
          logging_schema = self.logging_schema
          rpc_schema = self.rpc_schema
          Membrane::SchemaParser.parse do
            {
              "mbus"            => String,
              "logging"         => logging_schema,
              "rpc"             => rpc_schema,
              "cluster"         => String,
            }
          end
      end

      def self.logging_schema
        Membrane::SchemaParser.parse do
          {
            "file"             => String,
            "level"            => enum("debug", "info", "warn"),
          }
        end
      end

      def self.rpc_schema
        Membrane::SchemaParser.parse do
          {
            "bridge"                              => String,
            optional("register_retry_delay")      => Fixnum,
            optional("register_conn_timeout")     => Fixnum,
            optional("register_inactive_timeout") => Fixnum,
            optional("queue_process_delay")       => Fixnum,
            optional("max_request_retry")         => Fixnum,
          }
        end
      end

      def initialize(config)
          @config = EMPTY_CONFIG.merge(config)
      end

      def validate
        self.class.schema.validate(@config)
      end
     
      def [](key)
        @config[key]
      end
  end
end
