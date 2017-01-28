package psu.gv.bigdata.twitter;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import psu.gv.bigdata.crud.topics;
import psu.gv.bigdata.text.SearchTweets;

public class TwitterProducer {
	
	private static final String topic = "twitter";
	private static final Logger logger = LogManager.getLogger("twitter_logger");
	
	public static void run(String consumerKey, String consumerSecret,
			String token, String secret) throws InterruptedException {

		Properties props = new Properties();
    	props.put("metadata.broker.list", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		ProducerConfig producerConfig = new ProducerConfig(props);
		kafka.javaapi.producer.Producer<String, String> producer 
		= new kafka.javaapi.producer.Producer<String, String>(producerConfig);

		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		//endpoint.trackTerms(Lists.newArrayList("twitterapi",
		//		 "#University"));
		// Add some track terms
		topics tempTopics = new topics();
		tempTopics.loadForTopic("Universities");
		String[] topics = new String[tempTopics.size()];
		
		for(int it=0;it<tempTopics.size();it++){
			topics[it] = tempTopics.get(it).getSearchTerm();
			System.out.print(" " + topics[it]);
		}
		System.out.println();
		
		endpoint.trackTerms(Lists.newArrayList(topics));
	
		
		Authentication auth = new OAuth1(consumerKey, consumerSecret, token,
				secret);
		// Create a new BasicClient. By default gzip is enabled.
		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
				.endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		// Establish a connection
		client.connect();
			
		// Save messages from twitter api to PostgreSQL
		for (int msgRead = 0; msgRead < 1000; msgRead++) {
			KeyedMessage<String, String> message = null;
			try {
				message = new KeyedMessage<String, String>(topic, queue.take());
				producer.send(message);
				//Save data to postgre
				SearchTweets search = new SearchTweets();
				search.processOneTweet(queue.take().toString());
				
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.out.println(e.toString());
			} catch (Exception ex){
				ex.printStackTrace();
				System.out.println(ex.toString());
			}
			
		}
		producer.close();
		client.stop();

	}

	public static void main(String[] args) {
		try {
			TwitterProducer.run("abc","abc"
					,"abc","abc");
		} catch (InterruptedException e) {
			System.out.println(e);
		} catch(Exception ex){
			logger.debug(ex.getMessage().toString());
		}
	}
}
