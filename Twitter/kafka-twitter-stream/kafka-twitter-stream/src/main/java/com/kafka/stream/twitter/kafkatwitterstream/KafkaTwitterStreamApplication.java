package com.kafka.stream.twitter.kafkatwitterstream;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.kafka.stream.twitter.kafkatwitterstream.stream.TwitterStream;

@SpringBootApplication
public class KafkaTwitterStreamApplication implements CommandLineRunner{

	public static void main(String[] args) {
		SpringApplication.run(KafkaTwitterStreamApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		TwitterStream tweet = new TwitterStream();
		tweet.createStream();
	}

}
