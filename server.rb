#!/usr/bin/env ruby
require 'bunny'


class Server
    def initialize
        #connect to RabbitMQ server
        @connection = Bunny.new(host:"52.14.65.170",user: "admin", password: "admin")
        @connection.start
        @channel = @connection.create_channel
    end 

    #fuction to connect the consumer to the queue
    def start(queue_name,severities)
        #connect to queue_name
        @queue = @channel.queue(queue_name)
        #create exchange
        @exchange = @channel.direct("factorial_exchange")
        
        #call function to subcribe to queue
        subcribe_to_queue(severities)
    end 

    def stop
        channel.close
        connection.close
    end

    private 

    attr_reader :channel, :exchange, :queue, :connection

    def subcribe_to_queue(severities)
        #binding with the routing key
        severities.each do |severity|
            queue.bind(exchange, routing_key: severity)
        end

        queue.subscribe(block: true) do |_delivery_info, properties, payload|
            puts "Received #{payload.to_i}"
            @result = factorial(payload.to_i)

            
            #return the result to client 
            exchange.publish(@result.to_s, routing_key: properties.reply_to, correlation_id: properties.correlation_id)

            puts"Resent the result: #{payload} = #{@result} to #{properties.reply_to} with ID #{properties.correlation_id}"
        end 
        
    end 

    def factorial(number)
        @result = 1
        if number < 0 
            @result = "Sorry, factorial does not exist for negative numbers"
        elsif number == 0
            @result = 1 
        else 
            (1..number).each do |i|
                @result *= i 
            end 
        end
        return @result 
    end 
end 

begin
    server = Server.new
  
    puts ' [x] Awaiting RPC requests'
    server.start('odd_factorial',["odd"])
  rescue Interrupt => _
    server.stop
  end