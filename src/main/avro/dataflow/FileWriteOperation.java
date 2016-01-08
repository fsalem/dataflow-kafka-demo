package dataflow;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.Sink;
import com.google.cloud.dataflow.sdk.io.Sink.WriteOperation;
import com.google.cloud.dataflow.sdk.io.Sink.Writer;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.values.KV;

public class FileWriteOperation extends WriteOperation<KV<String, Long>, Boolean> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7990483318869498125L;

	private FileSink sink;
	
	public FileWriteOperation(FileSink sink) {
		super();
		this.sink = sink;
	}
	
	@Override
	public void initialize(PipelineOptions options) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void finalize(Iterable<Boolean> writerResults, PipelineOptions options) throws Exception {
		for (Boolean writerResult:writerResults)
			System.out.println(writerResult);
	}

	@Override
	public Writer<KV<String, Long>, Boolean> createWriter(PipelineOptions options) throws Exception {
		return new FileWrite(this);
	}

	@Override
	public Sink<KV<String, Long>> getSink() {
		return this.sink;
	}
	
	
	@Override
	public Coder<Boolean> getWriterResultCoder() {
		return new BooleanCoder();
	}


}
