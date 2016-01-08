package dataflow;

import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class WordCountTransform extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1874122535644822187L;

	@Override
	public PCollection<KV<String, Long>> apply(PCollection<String> lines) {
		PCollection<String> words = lines.apply(ParDo.of(new ExtractWordFn()));
		PCollection<KV<String, Long>> wordCounts = words.apply(Count.<String>perElement());
		return wordCounts;
	}
}
