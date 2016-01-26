package kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import utils.PropertiesStack;

public class KafkaConsumerObject extends KafkaConsumer<String, String> {

	private static Properties props = new Properties();

	static {
		props.put("bootstrap.servers", PropertiesStack.getKafkaBootstrapServers());
		props.put("zookeeper.connect", PropertiesStack.getZookeeperConnect());
		props.put("group.id", PropertiesStack.getKafkaGroupId());
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		// props.put("partition.assignment.strategy", "range");
	}

	public KafkaConsumerObject() {
		super(props);
	}

	public static void main(String[] args) throws Exception {
		KafkaConsumerObject consumer = new KafkaConsumerObject();
		List<String> topics = new ArrayList<String>();
		topics.add(PropertiesStack.getKafkaTopic());
		//consumer.subscribe(topics);
		TopicPartition partition0 = new TopicPartition(PropertiesStack.getKafkaTopic(), 0);
		consumer.assign(Arrays.asList(partition0));
		consumer.seekToBeginning(partition0);
		System.out.println(consumer.assignment());
		ConsumerRecords<String, String> records = consumer.poll(1000);
		System.out.println("records are " + records.count());
		for (ConsumerRecord<String, String> record : records)
			System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());

		consumer.close();

	}

}
