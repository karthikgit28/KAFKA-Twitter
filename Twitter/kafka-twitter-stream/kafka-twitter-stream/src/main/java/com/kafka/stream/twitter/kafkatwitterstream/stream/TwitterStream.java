package com.kafka.stream.twitter.kafkatwitterstream.stream;

import java.io.StreamCorruptedException;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;

public class TwitterStream {
	
	public void createStream() {
		
		Properties property = new Properties();
		property.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		//Equal to group id
		property.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-twitter-streams-group");
		property.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.class.getName());
		property.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.class.getName());
		
		StreamsBuilder builder = new StreamsBuilder();
		
		KStream<String, String> inputTopic =  builder.stream("twitter_tweet");
		KStream<String, String> filteredStream = inputTopic.filter(
				(key,jsonTweet) -> extractId(jsonTweet) > 10000
				);
		filteredStream.to("important_tweet");
		
		KafkaStreams streams = new KafkaStreams(builder.build(), property);
		streams.start();
	}
	
	
	public Integer extractId(String jsonTweet) {
		JsonParser parser = new JsonParser();
		try {
		return parser.parse(jsonTweet).getAsJsonObject()
				.get("user")
				.getAsJsonObject()
				.getAsInt();
		}catch(Exception e) {
			return 0;
		}
	}

}
