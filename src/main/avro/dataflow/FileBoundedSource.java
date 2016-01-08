package dataflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

public class FileBoundedSource extends BoundedSource<String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7455959980839955166L;
	private List<BoundedSource<String>> parallelSources;

	public FileBoundedSource() {
		parallelSources = new ArrayList<BoundedSource<String>>();
	}

	@Override
	public List<? extends BoundedSource<String>> splitIntoBundles(long desiredBundleSizeBytes, PipelineOptions options)
			throws Exception {
		parallelSources.clear();
		parallelSources.add(new FileBoundedSource());
		return parallelSources;
	}

	@Override
	public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
		return 145L;
	}

	@Override
	public boolean producesSortedKeys(PipelineOptions options) throws Exception {
		return false;
	}

	@Override
	public BoundedReader<String> createReader(PipelineOptions options)
			throws IOException {
		return new FileBoundedReader(this);
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
