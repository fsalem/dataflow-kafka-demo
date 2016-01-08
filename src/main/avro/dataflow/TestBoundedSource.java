package dataflow;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.Write;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import dataflow.FileBoundedSource;
import dataflow.FileSink;
import dataflow.WordCountTransform;

public class TestBoundedSource {

	public static void main(String[] args) {
		// DataflowPipelineOptions options = PipelineOptionsFactory.create()
		// .as(DataflowPipelineOptions.class);
		// options.setRunner(BlockingDataflowPipelineRunner.class);
		// options.setProject("tuberlin-tublr-99322");
		// options.setStagingLocation("gs://test");
		PipelineOptions options = PipelineOptionsFactory.create();

		Pipeline p = Pipeline.create(options);
		p.begin();
		FileBoundedSource source = new FileBoundedSource();
		PCollection<String> input = p.apply(Read.from(source).named("KafkaUnboundedSource"));
//		PCollection<String> windowInput = input
//				.apply(Window.<String> into(FixedWindows.of(Duration.standardMinutes(1))));
		PCollection<KV<String, Long>> wordCounts = input.apply(new WordCountTransform());
		wordCounts.apply(Write.<KV<String, Long>>to(new FileSink()));
		p.run();
	}

}
