package dataflow;

import java.io.IOException;
import java.util.Arrays;
import java.util.NoSuchElementException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.Instant;

import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.CheckpointMark;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.UnboundedReader;

import kafka.KafkaConsumerObject;

public class KafkaReader extends UnboundedReader<String> {

	private KafkaConsumerObject consumer;
	private String currentData;

	@Override
	public boolean advance() throws IOException {
		currentData = "";
		ConsumerRecords<String, String> records = consumer.poll(1000);
		if (records != null && !records.isEmpty()) {
			for (ConsumerRecord<String, String> record : records) {
				currentData += " "+record.value();
			}
		}
		return true;
	}

	@Override
	public CheckpointMark getCheckpointMark() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UnboundedSource<String, ?> getCurrentSource() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Instant getWatermark() {
		return new Instant(new java.util.Date().getTime()+1000L);
	}

	@Override
	public boolean start() throws IOException {
		currentData = null;
		consumer = new KafkaConsumerObject();
		TopicPartition partition0 = new TopicPartition("tweets", 0);
		consumer.assign(Arrays.asList(partition0));
		consumer.seekToBeginning(partition0);
		return advance();
	}

	@Override
	public void close() throws IOException {
		if (consumer != null) {
			consumer.close();
		}

	}

	@Override
	public String getCurrent() throws NoSuchElementException {
		return currentData;
	}

	@Override
	public Instant getCurrentTimestamp() throws NoSuchElementException {
		return Instant.now();
	}

}
