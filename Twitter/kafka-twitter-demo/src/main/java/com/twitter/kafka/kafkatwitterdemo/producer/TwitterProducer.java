package com.twitter.kafka.kafkatwitterdemo.producer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

@Component
public class TwitterProducer {

	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

	BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

	private String consumerKey = "eBdg2XeZEJNnrdLR7xv70Fckf";
	private String consumerSecret = "OVEjDA9PbDYeYs8A7WWFTUDNH3hajA1iQUs2QZqitucOLh1oIC";
	private String token = "1131644221-j2mNcbbSQwtGlgAJXNR7qz9ZNstz3Q7mnMCp43l";
	private String secret = "1GFjQmZxZNqbkD8qw34nhurh99GcyJXr67nlVEV7762aM";


	public TwitterProducer() {

	}

	public void run() {

		Client client = createTwitterClient();
		client.connect();
		
		KafkaProducer<String, String> producer = createKakfaProducer();

		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5,TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				client.stop();
			}
			if(msg != null) {
				logger.info("Message Received***** " + msg);
				producer.send(new ProducerRecord<String, String>("twitter_tweet", null, msg), new Callback() {
					
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						logger.info("Message sent acknowledgemtn");
						logger.info("Partition: " + metadata.partition());
						logger.info("Offset: " + metadata.offset());
						logger.info("Topic: " + metadata.topic());
						logger.info("Timestamp: " + metadata.timestamp());
					}
				});
			}
		}
	}

	private Client createTwitterClient() {

		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		List<String> terms = Lists.newArrayList("bitcoin");
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder()
				.name("Hosebird-Client-01")                              // optional: mainly for the logs
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		return hosebirdClient;

	}
	
	private KafkaProducer<String, String> createKakfaProducer(){
		
		//Create Property
		Properties property = new Properties();
		property.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		property.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		property.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//Safe Producer
		property.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		property.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		property.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		
		//By default in kafka > 1.1
		property.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, Integer.toString(5));
		
		//High ThroughPut Producer
		property.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snippy");
		property.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		property.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); //32KB
		
		//Acknowledgement factor
		//Ack = 0, Producer doesnt wait for ackowledgment and data loss may occur
		//property.setProperty(ProducerConfig.ACKS_CONFIG, Integer.toString(0));
		
		//Ack=1, default, producer will wait till acknowledgemnt receives from Leader, data loss is reeduced
		//property.setProperty(ProducerConfig.ACKS_CONFIG, Integer.toString(1));
		
		//Ack=all, will wait till all replicas acknowledge, else exception is thrown, No data loss
		//property.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		
		//To control how many msg can be sent in parallel at a single point
		//max.in.flight.requests.per.connection = 5
		
		//To retry for particular count in case of failures
		//retries = 0
		
				
		//Create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(property);
		
		return producer;
	}

}
