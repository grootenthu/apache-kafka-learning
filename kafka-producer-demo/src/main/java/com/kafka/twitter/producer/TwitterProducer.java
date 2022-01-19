package com.kafka.twitter.producer;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class TwitterProducer {
	
	Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

	public TwitterProducer() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		new TwitterProducer().run();
	}

	public void run() {
		logger.info("Booting up twitter streaming");
		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

		// create a twitter client
		Client hosebirdClient = createTwitterClient(msgQueue);

		// Attempts to establish a connection.
		hosebirdClient.connect();

		// create a kafka producer

		// loop to send tweets to kafka
		// on a different thread, or multiple different threads....
		while (!hosebirdClient.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				hosebirdClient.stop();
			}
			
			if (msg != null) {
				logger.info(msg);
			}
		}
		
		logger.info("End of application!!!!!!");
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {
		
		String consumerKey = "q6Js0aNYT7BkgeQo1eJyFywT5";
		String consumerSecret = "lk1msh8QcgBi1Gc4C2DdheCLvbOqfiMzbQwbqDMYQioUaG6jru";
		String token = "1483820792488284164-8QgBXNGNyBVbcbqmpMjO28WqzvEKtd";
		String secret = "gejAJeSPBJzLtr8jyys5D3QiOPPU9j0j4orNKPybyjgBX";

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		List<String> terms = Lists.newArrayList("bitcoin");
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder().name("XYZ-Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		return builder.build();
	}

}
