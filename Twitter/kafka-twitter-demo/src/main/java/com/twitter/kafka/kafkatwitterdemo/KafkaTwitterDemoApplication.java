package com.twitter.kafka.kafkatwitterdemo;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.twitter.kafka.kafkatwitterdemo.producer.TwitterProducer;

@SpringBootApplication
public class KafkaTwitterDemoApplication implements CommandLineRunner{

	public static void main(String[] args) {
		SpringApplication.run(KafkaTwitterDemoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		TwitterProducer producer = new TwitterProducer();
		producer.run();
	}

}
