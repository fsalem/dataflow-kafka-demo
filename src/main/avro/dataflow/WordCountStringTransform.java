package dataflow;

import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class WordCountStringTransform extends PTransform<PCollection<KV<String, Long>>, PCollection<String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1874122535644822187L;

	@Override
	public PCollection<String> apply(PCollection<KV<String, Long>> lines) {
		PCollection<String> words = lines.apply(ParDo.of(new CombineWordCountFn()));
		return words;
	}
}
