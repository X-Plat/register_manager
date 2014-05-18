# coding: UTF-8
require 'const'
require 'protocol'
require 'em-http'
require 'err'

module Register
  class BridgeClient

    attr_reader :config
    attr_reader :logger
    attr_reader :waiting_bridge_queue

    def initialize(config, logger)
      @bridge = config['rpc']['bridge']
      @cluster = config['cluster']
      @logger = logger
      @waiting_bridge_queue = []
      @max_request_retry = config['rpc']['max_request_retry'] ||
          DEFAULT_MAX_REQUEST_RETRY
      @register_retry_delay = config['rpc']['register_retry_delay'] || 
          DEFAULT_BRIDGE_RETRY_DELAY
      @register_conn_timeout = config['rpc']['register_conn_timeout'] || 
          DEFAULT_BRIDGE_CONN_TIMEOUT
      @register_inactive_timeout = config['rpc']['register_inactive_timeout'] || 
          DEFAULT_BRIDGE_INACTIVE_TIMEOUT
      @queue_process_delay = config['rpc']['queue_process_delay'] ||
          DEFAULT_BRIDGE_PROCESS_DELAY
    end
    
    def is_in_waiting_queue?(ins_id)
       return false unless ins_id
       waiting_bridge_queue.include?(ins_id)
    end

    def bridge_instance_id(instance)
      "#{instance['app_id']}_#{instance['instance_index']}"
    end

    #Default request head
    def request_head
      { 
        'content-type' => 'application/json' 
      }
    end

    #Default request timeout parameters.
    def request_timeout
      {
        :connect_timeout => @register_conn_timeout,
        :inactivity_timeout => @register_inactive_timeout
      }
    end

   #Request payload, (un)register instance protocol is defined in dea,
   #+ However the service protocol is defined in bridgec.
   #+ @param [Hash] instance: instance to process.
   #+ @param [Hash] options: request options.
   #+ @return [Object] payload according to the bridge protocol.
   def request_payload(instance, options)
     Protocol.new(instance, 'cluster' => @cluster).send("#{options[:action]}_protocol")
   end

    #sending request to bridge.
    #+ @param [Hash] instance: instance to process.
    #+ @param [Hash] options: request options.
    #+ @param [Block] &callback: request callback function.    
    def request(instance, options, &callback)
      begin
        Protocol.validate(instance)
      rescue => e
        logger.error("Instance not matched with current protocol version, \
                     validate failed: #{e}")
        return
      end

      unless SUPPORTED_BRIDGE_ACTION.include?(options[:action]) 
        logger.warn("Not supported action, skip.")
        return
      end

      api = @bridge + '/' + Protocol.send("#{options[:action]}_api")
      method = Protocol.send("#{options[:action]}_method")
      payload = request_payload(instance, options)
      bid = bridge_instance_id(instance)

      if @waiting_bridge_queue.include?(bid)
         logger.debug("[RPC] instance #{bid} in queue, delay to process.")

         EM.add_timer(@queue_process_delay){
           request(instance, options, &callback)
         }
      else
        request_with_retry(instance, api, method, payload, options, &callback)
      end
    end

    #Sending request to bridge with retry.
    #+ @param [String] request api.
    #+ @param [Object] payload according to the bridge protocol.  
    #+ @param [Hash]  request options.
    #+ @param [Number] retries: retry times.
    #+ @param [Block] &callback: request callback function.
    def request_with_retry(instance, request_api, method, payload, options, 
                           retries = 0, &callback)

      bid = bridge_instance_id(payload)

      @logger.debug("[#{bid}] Starting #{retries} #{options[:action]} \
                   request #{payload}")

      @waiting_bridge_queue << bid unless is_in_waiting_queue?(bid)

      http = EventMachine::HttpRequest.new(request_api,
                                           request_timeout).send(method, {
                                           :head => request_head,
                                           :body => payload})

      http.callback {
        begin
          @logger.debug("[#{bid}] Received response #{http.response}")

          resp = Yajl::Parser.parse(http.response)

          if resp && resp["success"] == true
             @logger.info("[#{bid}] #{options[:action]} succeed,\
                           request payloda is #{payload}.")
             @waiting_bridge_queue.delete(bid)
             callback.call('succ') if callback
          elsif resp && resp["message"] && (resp["message"].include?(BNS_NOEXITS))
             @logger.warn("[#{bid}] No bns for #{payload[:app_name]}, Creating!")
             options_pre = options
             options[:action] = ACTION_CREATE
             request(instance, options, &callback)

             EM.add_timer(@queue_process_delay) do
               request(instance, options_pre, &callback)
             end
	      elsif retries < @max_request_retry
              EM.add_timer(@register_retry_delay) {
                request_with_retry(instance, request_api, method, payload, 
                                   options, retries += 1, &callback)
             }
          else
             @logger.warn("[#{bid}] Sending #{options[:action]} request\
                          #{payload} succ, while bridge return error #{resp}")
                          
             @waiting_bridge_queue.delete(bid)
             callback.call('failed') if callback
          end
        rescue => e
          @logger.warn("[#{bid}] #{options[:action]} request with \
                        exception #{e}, request data is #{payload}")
          callback.call('failed') if callback
        end
      }

      http.errback {
        if retries < @max_request_retry
           EM.add_timer(@register_retry_delay) {
             request_with_retry(instance, request_api, method, payload, 
                                options, retries += 1, &callback)
           }
        else
           @logger.warn("[#{bid}] Request bridge failed with error\
                         #{http.error}, request data is #{payload}")
           @waiting_bridge_queue.delete(bid)
           callback.call('failed') if callback
        end
      }
    end
  end
end
