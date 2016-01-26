package dataflow;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;

public class CombineWordCountFn extends DoFn<KV<String, Long>, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 311222424158594211L;

	@Override
	public void processElement(DoFn<KV<String, Long>, String>.ProcessContext c) throws Exception {
		c.output(c.element().getKey()+" "+c.element().getValue());
	}

}
