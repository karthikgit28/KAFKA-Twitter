package com.twitter.elastic.cosumer.springelasticconsumer.consumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;


public class ElasticSearchConsumer {

	String hostName = "kakfaconsumer-427824206.ap-southeast-2.bonsaisearch.net";
	String userName = "bfjri5b3q1";
	String password = "ljrqpvstj8";
	String localHost = "localhost:9092";
	String topic= "twitter_tweet";

	Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getClass());

	public RestHighLevelClient createElasticClient() {

		final CredentialsProvider credentials = new BasicCredentialsProvider();
		credentials.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));

		RestClientBuilder restClientBuilder = RestClient.builder(
				new HttpHost(hostName, 443, "https")).setHttpClientConfigCallback(
						new RestClientBuilder.HttpClientConfigCallback() {
							@Override
							public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
								return httpClientBuilder.setDefaultCredentialsProvider(credentials);
							}
						});

		RestHighLevelClient elasticClient = new RestHighLevelClient(restClientBuilder);
		return elasticClient;
	}

	public KafkaConsumer<String,String> createKafkaConsumer(String topic) {

		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, localHost);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-twitter-group");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		//Controls how many records can be read at a time in poll
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

		//Disable auto commit since by default auto commit comits offset in 5sec hence if server is down befor reading 10 msgs next set will be read withut checking unread ones
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
	}

	public void elasticConsumer() {

		RestHighLevelClient client = createElasticClient();

		KafkaConsumer<String, String> consumer = createKafkaConsumer(topic);
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			//Synchronous process from here till poll set is completed- 10 in this example
			for(ConsumerRecord<String, String> record : records) {
				IndexRequest request = new IndexRequest("twitter", "tweets").source(record.value(), XContentType.JSON);
				try {
					IndexResponse response = client.index(request, RequestOptions.DEFAULT);
					String id = response.getId();
					logger.info("*******Id******" + id);
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		//client.close();
	}


	public void elasticConsumerBulkRequest() {

		RestHighLevelClient client = createElasticClient();

		KafkaConsumer<String, String> consumer = createKafkaConsumer(topic);
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			Integer count = records.count();
			//To Handle Bulk request
			BulkRequest bulkReq = new BulkRequest();

			//Synchronous process from here till poll set is completed- 10 in this example
			for(ConsumerRecord<String, String> record : records) {
				try {
					String id = extractId(record.value());
					IndexRequest request = new IndexRequest("twitter", "tweets",id).source(record.value(), XContentType.JSON);	
					bulkReq.add(request);
				}catch(Exception e) {
					logger.warn("Skipping bad data " + record.value());
				}
			}

			if(count > 0) {
				try {
					BulkResponse response = client.bulk(bulkReq, RequestOptions.DEFAULT);
					logger.info("Commiting offset");
					//Manual commit of offset when ENABLE_AUTO_COMMIT_CONFIG = false
					consumer.commitSync();
					logger.info("Offset commited");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		//client.close();
	}


	public String extractId(String tweet) {
		JsonParser parser = new JsonParser();
		return parser.parse(tweet).getAsJsonObject()
				.get("id_str")
				.getAsString();
	}

}
