package dataflow;

import kafka.KafkaProducerObject;
import utils.PropertiesStack;

import com.google.cloud.dataflow.sdk.transforms.DoFn;

public class WordCountWriterDoFn extends DoFn<String, Boolean> {

	@Override
	public void processElement(DoFn<String, Boolean>.ProcessContext c) throws Exception {
		new KafkaProducerObject().send(c.element(), PropertiesStack.getKafkaTopicResult());

	}

}
