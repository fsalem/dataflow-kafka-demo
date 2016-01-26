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
import utils.PropertiesStack;

public class KafkaReader extends UnboundedReader<String> {

	KafkaConsumerObject consumer;
	private String currentData;
	private String currentKey;

	private void initKafkaConsumer() {
		consumer = new KafkaConsumerObject();
		TopicPartition partition0 = new TopicPartition(PropertiesStack.getKafkaTopic(), 0);
		consumer.assign(Arrays.asList(partition0));
	}
	
	@Override
	public boolean advance() throws IOException {
		if (consumer == null){
			initKafkaConsumer();
		}
		currentData = "";
		currentKey = "";
		ConsumerRecords<String, String> records = consumer.poll(1000);
		consumer.close();
		consumer = null;
		if (records != null && !records.isEmpty()) {
			for (ConsumerRecord<String, String> record : records) {
				System.out.println("record key = "+record.key() +" and value = "+record.value());
				currentData += " " + record.value();
				currentKey += " " + record.key();
			}
			return true;
		}else{
			System.out.println("NO MORE TO READ");
			return false;
		}
		
	}

	@Override
	public CheckpointMark getCheckpointMark() {
		return new KafkaCheckpointMark();
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
		consumer = new KafkaConsumerObject();
		TopicPartition partition0 = new TopicPartition(PropertiesStack.getKafkaTopic(), 0);
		consumer.assign(Arrays.asList(partition0));
		consumer.seekToBeginning(partition0);
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
		return currentKey.getBytes();
	}

	@Override
	public Instant getCurrentTimestamp() throws NoSuchElementException {
		return Instant.now();
	}
	
	class KafkaCheckpointMark implements CheckpointMark{

		public void finalizeCheckpoint() throws IOException {
			// TODO Auto-generated method stub
		}
		
	}

}
