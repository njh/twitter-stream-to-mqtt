require 'rubygems'
require "bundler"

Bundler.require(:default)

# Make STDOUT unbuffered
STDOUT.sync = true

# Load the configuration file
CONFIG = YAML::load_file('config.yml')
SCREEN_NAMES = CONFIG['users'].map {|u| u.downcase}
HASHTAGS = CONFIG['hashtags'].map {|h| h.downcase}
PUBLISH_USER_KEYS = ['created_at', 'name', 'id', 'location', 'url', 'description']

def publish_status(mqtt, screen_name, data)
  return if data.nil?
  ['created_at', 'id', 'text'].each do |key|
    mqtt.publish("twitter/users/#{screen_name}/status/#{key}", data[key], retain=true)
  end
end


EventMachine::run do
  # Connect to the MQTT server
  puts "Connecting to #{CONFIG['mqtt']['host']}:#{CONFIG['mqtt']['port']}..."
  mqtt = EventMachine::MQTT::ClientConnection.connect(
    CONFIG['mqtt']['host'],
    CONFIG['mqtt']['port']
  )

  # Convert twitter screen names into user identifiers
  http = EventMachine::HttpRequest.new('https://api.twitter.com/1/users/lookup.json').get(
    :query => {'screen_name' => SCREEN_NAMES.join(',')}
  )
  
  http.errback { puts "Failed to lookup screen names: #{http.response}"; EM.stop }
  http.callback do
    users = JSON.parse(http.response)
    user_ids = []
    
    users.each do |user|
      screen_name = user['screen_name'].downcase
      user_ids << user['id']
      PUBLISH_USER_KEYS.each do |key|
        mqtt.publish("twitter/users/#{screen_name}/#{key}", user[key], retain=true)
      end
      publish_status(mqtt, screen_name, user['status'])
    end

    puts "Filtering by #{HASHTAGS.count} hashtags and following #{user_ids.count} users."
    stream = Twitter::JSONStream.connect(
      :path    => '/1/statuses/filter.json',
      :auth    => ENV['TWITTER_AUTH'],
      :ssl     => true,
      :params  => {
        'lang' => 'en',
        'follow' => user_ids,
        'track' => HASHTAGS.map {|h| "##{h}"}
      }
    )

    stream.each_item do |item|
      data = JSON.parse(item)
      screen_name = data['user']['screen_name'].downcase
  
      data['entities']['hashtags'].each do |hashtag|
        name = hashtag['text'].downcase
        if HASHTAGS.include?(name)
          mqtt.publish("twitter/hashtags/#{name}", "#{screen_name}: #{data['text']}")
        end
      end

      data['entities']['user_mentions'].each do |user|
        name = user['screen_name'].downcase
        if SCREEN_NAMES.include?(name)
          mqtt.publish("twitter/mentions/#{name}", "#{screen_name}: #{data['text']}")
        end
      end
  
      if SCREEN_NAMES.include?(screen_name)
        publish_status(mqtt, screen_name, data)
      end
    end
  
    stream.on_error do |message|
      puts "error: #{message}"
    end
    
    stream.on_reconnect do |timeout, retries|
      puts "reconnecting in: #{timeout} seconds"
    end
    
    stream.on_max_reconnects do |timeout, retries|
      puts "failed after #{retries} failed reconnects"
    end
  end
    
  trap('TERM') {  
    EventMachine.stop
  }
end
