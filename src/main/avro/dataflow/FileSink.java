package dataflow;

import com.google.cloud.dataflow.sdk.io.Sink;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.values.KV;

public class FileSink extends Sink<KV<String, Long>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5239763862680663944L;

	@Override
	public void validate(PipelineOptions options) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public WriteOperation<KV<String, Long>, Boolean> createWriteOperation(
			PipelineOptions options) {
		return new FileWriteOperation(this);
	}
	
	

}
