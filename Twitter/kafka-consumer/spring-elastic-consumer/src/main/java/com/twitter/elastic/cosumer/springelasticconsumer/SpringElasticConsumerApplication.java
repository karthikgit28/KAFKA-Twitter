package com.twitter.elastic.cosumer.springelasticconsumer;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.twitter.elastic.cosumer.springelasticconsumer.consumer.ElasticSearchConsumer;

@SpringBootApplication
public class SpringElasticConsumerApplication implements CommandLineRunner{

	public static void main(String[] args) {
		SpringApplication.run(SpringElasticConsumerApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		ElasticSearchConsumer search = new ElasticSearchConsumer();
		search.elasticConsumer();
	}

}
