package dataflow;


import org.joda.time.Duration;

import twitter.TwitterUnboundedSource;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class WriteToPubSubPipeline {

	public static void main(String[] args) {
		DataflowPipelineOptions options = PipelineOptionsFactory.create().as(
				DataflowPipelineOptions.class);
		options.setRunner(DataflowPipelineRunner.class);
		options.setProject("rapid-stream-118713");
		options.setStagingLocation("gs://streaming-test");
		options.setStreaming(Boolean.TRUE);
		options.setMaxNumWorkers(1);
		options.setNumWorkers(1);
		// PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(options);
		p.begin();
		PCollection<String> input = p.apply(Read.named("ReadTweets").from(new TwitterUnboundedSource()));
		input.apply(PubsubIO.Write.named("WriteToPubSub").topic(
				"projects/rapid-stream-118713/topics/tweets"));
//		PCollection<String> windowInput = input.apply(Window
//				.<String> into(FixedWindows.of(Duration.standardMinutes(1)))
//				.discardingFiredPanes());
		p.run();

	}

}
