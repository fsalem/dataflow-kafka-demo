package twitter;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.joda.time.Instant;

import utils.PropertiesStack;

import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.CheckpointMark;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.UnboundedReader;
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

public class TwitterReader extends UnboundedReader<String> {

	/**
	 * Set up your blocking queues: Be sure to size these properly based on
	 * expected TPS of your stream
	 */
	BlockingQueue<String> msgQueue;
	BlockingQueue<Event> eventQueue;
	/**
	 * Declare the host you want to connect to, the endpoint, and authentication
	 * (basic auth or oauth)
	 */
	Hosts hosebirdHosts;
	StatusesFilterEndpoint hosebirdEndpoint;
	// Optional: set up some followings and track terms
	List<Long> followings;
	List<String> terms;

	// These secrets should be read from a config file
	Authentication hosebirdAuth;

	ClientBuilder builder;

	Client hosebirdClient;

	private String currentData;

	private void initTwitter() {
		msgQueue = new LinkedBlockingQueue<String>(100000);
		eventQueue = new LinkedBlockingQueue<Event>(1000);

		hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		hosebirdEndpoint = new StatusesFilterEndpoint();
		followings = Lists.newArrayList(1234L, 566788L);
		terms = Lists.newArrayList("twitter", "api");
		hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);
		hosebirdAuth = new OAuth1(PropertiesStack.getTwitterAPIKey(),
				PropertiesStack.getTwitterAPISecret(),
				PropertiesStack.getTwitterAccessToken(),
				PropertiesStack.getTwitterAccessTokenSecret());
		builder = new ClientBuilder().name("Hosebird-Client-01")
				.hosts(hosebirdHosts).authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue))
				.eventMessageQueue(eventQueue);
		hosebirdClient = builder.build();
	}

	private static String clean(final String msg) {
		int ind, ind1, ind2 = -1, ind3 = -1, spaceInd;
		StringBuffer buffer = new StringBuffer(msg);
		while ((ind1 = buffer.indexOf("\\u")) != -1
				|| (ind2 = buffer.indexOf("http:")) != -1
				|| (ind3 = buffer.indexOf("https:")) != -1) {
			ind = ind1 != -1 ? ind1 : (ind2 != -1 ? ind2 : ind3);
			spaceInd = buffer.indexOf(" ", ind);
			if (spaceInd == -1)
				buffer.delete(ind, buffer.length());
			else
				buffer.delete(ind, spaceInd);
		}
		return buffer.toString().trim();
	}

	@Override
	public boolean advance() throws IOException {

		currentData = "";
		String msg;
		int startInd, endInd;
		if (!hosebirdClient.isDone()) {
			try {
				msg = msgQueue.take();
				startInd = msg.indexOf(",\"text\":\"");
				endInd = msg.indexOf("\",\"source\":");
				if (!(startInd == -1 || endInd == -1 || endInd <= startInd || startInd
						- endInd < 9)) {
					currentData = clean(msg.substring(startInd, endInd)
							.substring(9));
					System.out.println(currentData);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return true;

	}

	@Override
	public CheckpointMark getCheckpointMark() {
		return new TwitterCheckpointMark();
	}

	@Override
	public UnboundedSource<String, ?> getCurrentSource() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Instant getWatermark() {
		return new Instant(new java.util.Date().getTime() + 1000L);
	}

	@Override
	public boolean start() throws IOException {
		initTwitter();
		hosebirdClient.connect();
		return advance();
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public String getCurrent() throws NoSuchElementException {
		return currentData;
	}

	@Override
	public byte[] getCurrentRecordId() throws NoSuchElementException {
		return currentData.getBytes();
	}

	@Override
	public Instant getCurrentTimestamp() throws NoSuchElementException {
		return Instant.now();
	}

	class TwitterCheckpointMark implements CheckpointMark {

		public void finalizeCheckpoint() throws IOException {
			// TODO Auto-generated method stub
		}

	}

}
