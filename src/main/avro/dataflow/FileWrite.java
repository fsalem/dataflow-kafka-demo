package dataflow;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import com.google.cloud.dataflow.sdk.io.Sink.WriteOperation;
import com.google.cloud.dataflow.sdk.io.Sink.Writer;
import com.google.cloud.dataflow.sdk.values.KV;

public class FileWrite extends Writer<KV<String, Long>, Boolean> {

	private BufferedWriter writer;
	private WriteOperation<KV<String, Long>, Boolean> writeOperation;
	
	public FileWrite(WriteOperation<KV<String, Long>, Boolean> writeOperation) {
		super();
		this.writeOperation = writeOperation;
	}
	@Override
	public void open(String uId) throws Exception {
		writer = new BufferedWriter(new FileWriter(new File(uId.toString())));
	}

	@Override
	public void write(KV<String, Long> value) throws Exception {
		writer.write(value.getKey());
		writer.write("\t");
		writer.write(value.getValue().toString());
		writer.write("\n");
		
	}

	@Override
	public Boolean close() throws Exception {
		writer.close();
		return true;
	}

	@Override
	public WriteOperation<KV<String, Long>, Boolean> getWriteOperation() {
		return writeOperation;
	}

}
