#!/usr/bin/env ruby
require 'bunny'
require 'thread'

class Client 
    attr_accessor :call_id, :response, :lock, :condition, :connection,
                :channel, :server_queue_name, :reply_queue, :exchange , :room_condition , :room
    
    def initialize(server_queue_name)
        @connection = Bunny.new(host:"52.14.65.170",user: "admin", password: "admin")
        @connection.start
        @channel = @connection.create_channel
        @exchange = @channel.direct("factorial_exchange")
        @server_queue_name = server_queue_name

        setup_reply_queue
    end 

    def start(a,b)
        threads = []
        #create a threads
        a.times do |i|
            threads << Thread.new do
                #in each thread create b int number and send it to server      
                b.times do |j|
                    number = rand(1..10)
                    puts "Thread #{i+1} send number #{number}"
                    call_fatorial(number.to_s)
                end 
            end
            
        end
        threads.each{|t| t.join}
    end 

    def stop
        channel.close
        connection.close
    end

    private
    
    def call_fatorial(number)
        # wait for the signal to continue the execution
        lock.synchronize {
            #only one thread can wait for the response in room at a time 
            puts"Room #{@room}"
            @room -=1
            if (@room < 0)
                room_condition.wait(@lock)
                #puts"Room #{@room}"
            end
            @call_id = generate_uuid
            @exchange.publish(number.to_s,routing_key: @server_queue_name, correlation_id: @call_id, reply_to: @reply_queue.name)
            condition.wait(@lock) 
            @result = response

            puts "The factorial of #{number} is #{@result} The id is #{@call_id}"
            @room+=1
            room_condition.signal
        }
    end 

    #setting up the reply queue
    def setup_reply_queue
        @lock = Mutex.new
        @condition = ConditionVariable.new
        @room_condition = ConditionVariable.new
        @room = 1

        @reply_queue = channel.queue('', exclusive: true)
        
        @reply_queue.bind(exchange, routing_key: @reply_queue.name)
        reply_queue.subscribe do |_delivery_info, properties, payload|
            if properties[:correlation_id] == self.call_id
                self.response = payload.to_i
                # sends the signal to continue the execution of #call
                self.lock.synchronize { self.condition.signal }
            end 
        end 
    end

    #generate a unique random key
    def generate_uuid
        # very naive but good enough for code examples
        "#{rand}#{rand}#{rand}"
    end 
end 

client = Client.new("odd")
client.start(3,3)
client.stop