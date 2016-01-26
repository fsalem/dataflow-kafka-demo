package twitter;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import kafka.KafkaProducerObject;
import utils.PropertiesStack;

public class TwitterStream {

	public static void main(String[] args) throws InterruptedException {
		/**
		 * Set up your blocking queues: Be sure to size these properly based on
		 * expected TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
		BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);
		/**
		 * Declare the host you want to connect to, the endpoint, and
		 * authentication (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		List<Long> followings = Lists.newArrayList(1234L, 566788L);
		List<String> terms = Lists.newArrayList("twitter", "api");
		hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(PropertiesStack.getTwitterAPIKey(),
				PropertiesStack.getTwitterAPISecret(), PropertiesStack.getTwitterAccessToken(),
				PropertiesStack.getTwitterAccessTokenSecret());

		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional:
																				// mainly
																				// for
																				// the
																				// logs
				.hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue)).eventMessageQueue(eventQueue); // optional:
																									// use
																									// this
																									// if
																									// you
																									// want
																									// to
																									// process
																									// client
																									// events

		Client hosebirdClient = builder.build();
		// Attempts to establish a connection.
		hosebirdClient.connect();
		String msg, tweet;
		int startInd, endInd;
		KafkaProducerObject kafkaProducerInstance = new KafkaProducerObject();
		int i=0;
		while (!hosebirdClient.isDone()) {
			if (i ==100)break;
			i++;
			msg = msgQueue.take();
			startInd = msg.indexOf(",\"text\":\"");
			endInd = msg.indexOf("\",\"source\":");
			if (startInd == -1 || endInd == -1)
				continue;

			tweet = msg.substring(startInd, endInd).substring(9);
			kafkaProducerInstance.send(tweet,PropertiesStack.getKafkaTopic());
			System.out.println(tweet);
			// profit();
		}
		kafkaProducerInstance.close();

	}

}
