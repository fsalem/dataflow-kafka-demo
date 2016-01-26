package dataflow;

import com.google.cloud.dataflow.sdk.transforms.DoFn;

import kafka.KafkaProducerObject;
import utils.PropertiesStack;

public class WordCountWriterDoFn extends DoFn<String, Boolean> {

	@Override
	public void processElement(DoFn<String, Boolean>.ProcessContext c) throws Exception {
		new KafkaProducerObject().send(c.element(), PropertiesStack.getKafkaTopicResult());

	}

}
