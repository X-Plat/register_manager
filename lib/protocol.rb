require "membrane"

module Register
  class Protocol

    attr_accessor :instance

    def initialize(instance, opts={})
      @instance = instance.dup
      @instance.merge!(opts)
    end

    class << self

      def schema
        meta_schema = self.meta_schema  
        Membrane::SchemaParser.parse do
          {
            'app_uri'                 => enum([String], nil),        
            'app_id'                  => String,
            'app_name'                => String,
            'instance_ip'             => String,
            'instance_id'             => String,
            'instance_index'          => String,
            'instance_meta'           => meta_schema,
            optional('instance_user') => String,
            optional('instance_path') => String,
          }
        end
      end

      def meta_schema
        port_info_schema = self.port_info_schema  
        prod_ports_schema = self.prod_ports_schema
        Membrane::SchemaParser.parse do
          {
            'raw_ports'             => dict(String, port_info_schema),
            'prod_ports'            => dict(String, prod_ports_schema),
          }    
        end    
      end    

      def prod_ports_schema
        port_info_schema = self.port_info_schema  
        Membrane::SchemaParser.parse do
          {
            'host_port'              => Fixnum,
            'container_port'         => Fixnum,
            'port_info'              => port_info_schema,
          }    
        end    
      end

      def port_info_schema
        Membrane::SchemaParser.parse do
          {
            'port'                  => Fixnum,
            'bns'                   => bool,
            'http'                  => bool,
          }    
        end    
      end

      def validate(instance)
        self.schema.validate(instance)
      end

      def register_api
        "/addRMIports"
      end
      
      def register_method
        "post"
      end

      def unregister_method
        "delete"
      end

      def unregister_api
        "/delRMIports"
      end

      def create_api
        "/addServiceName"
      end
 
      def create_method
        "post"
      end
    end     

    def app_detail
      return {} unless instance && instance.class == Hash
      message = {}
      ['org_name', 'space_name'].each do |key|
          message[key] = instance['instance_tags'][key]
      end 

      ['app_name', 'cluster'].each do |key|
          message[key] = instance[key]
      end
      message
    end

    def instance_detail
      return {} unless instance && instance.class == Hash
      message = {}

      ['instance_id',
       'instance_ip'
      ].each { |key| message[key] = instance[key.to_s] }

      [ 'instance_index',
        'app_id' ].each { |key| message[key] = instance[key.to_s].to_i }

      message.merge(app_detail)
    end

    def register_protocol
      return {} unless instance && instance.class == Hash

      rmi_ports = parse_prod_ports('bns')
      return {} unless rmi_ports.size > 0 

      message = {}
      http_ports = parse_prod_ports('http')
      message['instance_http_port'] = http_ports.size > 0 ? 
          http_ports[0]['port']: rmi_ports[0]['port']
      message['instance_rmi_ports'] = rmi_ports
      message['instance_path'] = instance['instance_path'] || DEFAULT_APP_PATH
      message.merge(instance_detail)
    end

    #Parse the ports to be registered
    #@Param [Hash] ports: ports dispatched for the instance
    #@Return [Array] ports to register
    def parse_prod_ports(type)
      meta = instance.fetch('instance_meta', {})
      ports = meta.fetch('prod_ports', {}) if meta && meta.class == Hash
      return [] unless ports and ports.class == Hash

      prod_ports = []
      ports.each_pair do |name, desc| 
        next unless desc.class == Hash
        info = desc['port_info']
        prod_ports << {
            'name' => name, 
            'port' => desc['host_port']
        } if info && info[type]
      end
      prod_ports
    end

    #Unregister instance protocol
    def unregister_protocol
      instance_detail   
    end

    #Create bns protocal for instance
    def create_protocol
      app_detail
    end
  end
end
