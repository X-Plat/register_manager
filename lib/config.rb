require "membrane"

module Register
  class Config
      EMPTY_CONFIG = {}

      def self.schema
          Membrane::SchemaParser.parse do
            {
              "mbus"            => String,
              "logging"         => self.logging_schema,
              "rpc"             => self.rpc_schema,
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
        self.schema.validate(@config)
      end
  end
end
