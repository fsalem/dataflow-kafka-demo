package dataflow;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.NoSuchElementException;

import org.joda.time.Instant;

import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.BoundedSource.BoundedReader;

public class FileBoundedReader extends BoundedReader<String> {

	//private KafkaConsumerObject consumer;
	private StringBuilder currentData;
	private BoundedSource<String> source;
	
	public FileBoundedReader(BoundedSource<String> source) {
		super();
		this.source = source;
		
	}

	@Override
	public boolean advance() throws IOException {
		currentData = new StringBuilder();
		FileReader fileReader = new FileReader("vanrikki-stool.txt");
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		while (bufferedReader.ready()){
			String line = bufferedReader.readLine().trim();
			if (line.isEmpty())continue;
			currentData.append(line);
			currentData.append("\n");
		}
		bufferedReader.close();
		fileReader.close();
		return false;
	}

	@Override
	public boolean start() throws IOException {
		advance();
//		consumer = new KafkaConsumerObject();
//		TopicPartition partition0 = new TopicPartition("tweets", 0);
//		consumer.assign(Arrays.asList(partition0));
//		consumer.seekToBeginning(partition0);
		return true;
	}

	@Override
	public void close() throws IOException {
//		if (consumer != null) {
//			consumer.close();
//		}

	}

	@Override
	public String getCurrent() throws NoSuchElementException {
		return currentData.toString();
	}

	@Override
	public Instant getCurrentTimestamp() throws NoSuchElementException {
		return Instant.now();
	}

	@Override
	public BoundedSource<String> getCurrentSource() {
		return this.source;
	}

}
