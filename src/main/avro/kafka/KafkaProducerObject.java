package kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import utils.PropertiesStack;

public class KafkaProducerObject extends KafkaProducer<String, String> {
	private static Properties props = new Properties();

	static {
		props.put("bootstrap.servers", PropertiesStack.getKafkaBootstrapServers());
		props.put("acks", "all");
		props.put("retries", 1);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	}

	public KafkaProducerObject() {
		super(props);
	}

	public void send(String message, String topic) {
		Future<RecordMetadata> result = send(new ProducerRecord<String, String>(topic, message));

		try {
			System.out.println(result.get().offset());
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

//	public static void main(String[] args) {
//		KafkaProducerObject producerObject = new KafkaProducerObject();
//		Scanner scanner = new Scanner(System.in);
//		while (true) {
//			System.out.println("Enter new statement");
//			String line = scanner.nextLine();
//			producerObject.send(line, "tweets");
//		}
//	}
}
