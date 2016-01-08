package dataflow;

import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.KV;

public class OrderWordCountTransform implements SerializableFunction<Iterable<KV<String, Long>>, Iterable<KV<String, Long>>>{

	public Iterable<KV<String, Long>> apply(Iterable<KV<String, Long>> input) {
		// TODO Auto-generated method stub
		return null;
	}
	

}
