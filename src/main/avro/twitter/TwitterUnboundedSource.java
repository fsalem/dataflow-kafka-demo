package twitter;


import java.util.ArrayList;
import java.util.List;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.CheckpointMark;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

public class TwitterUnboundedSource extends UnboundedSource<String, CheckpointMark> {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7455959980839955166L;
	private List<UnboundedSource<String, CheckpointMark>> parallelSources;
	
	public TwitterUnboundedSource() {
		parallelSources = new ArrayList<UnboundedSource<String, CheckpointMark>>();
	}

	@Override
	public List<? extends UnboundedSource<String, CheckpointMark>> generateInitialSplits(
			int desiredNumSplits, PipelineOptions options) throws Exception {
		parallelSources.clear();
		for (int i=0 ; i<desiredNumSplits ; i++){
			parallelSources.add(new TwitterUnboundedSource());
		}
		return parallelSources;
	}

	@Override
	public UnboundedReader<String> createReader(
			PipelineOptions options, CheckpointMark checkpointMark) {
		return new TwitterReader();
	}

	@Override
	public Coder<CheckpointMark> getCheckpointMarkCoder() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void validate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Coder<String> getDefaultOutputCoder() {
		return StringUtf8Coder.of();
	}

}
