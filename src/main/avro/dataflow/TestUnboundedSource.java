package dataflow;

import java.util.concurrent.TimeUnit;

import org.joda.time.Duration;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.io.Write;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineJob;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterProcessingTime;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class TestUnboundedSource {

	public static void main(String[] args) {
		// DataflowPipelineOptions options = PipelineOptionsFactory.create()
		// .as(DataflowPipelineOptions.class);
		// options.setRunner(BlockingDataflowPipelineRunner.class);
		// options.setProject("tuberlin-tublr-99322");
		// options.setStagingLocation("gs://test");
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(options);
		p.begin();
		// problem: class Read.Bounded has a function
		// registerDefaultTransformEvaluator to register evaluator but
		// Read.Unbounded doesn't
		KafkaUnboundedSource source = new KafkaUnboundedSource();
		PCollection<String> input = p.apply(Read.from(source).named("KafkaUnboundedSource"));
		PCollection<String> windowInput = input
				// .apply(Window.<String>
				// into(FixedWindows.of(Duration.standardMinutes(1))));
				.apply(Window.<String> into(FixedWindows.of(Duration.standardMinutes(1)))
						.triggering(
								AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1)))
						.discardingFiredPanes().withAllowedLateness(Duration.standardMinutes(1)));
		PCollection<KV<String, Long>> wordCounts = windowInput.apply(new WordCountTransform());
		wordCounts.apply(Write.to(new FileSink()));
		p.run();
		
	}

}
