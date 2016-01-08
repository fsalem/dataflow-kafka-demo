package dataflow;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.CoderException;

public class BooleanCoder extends AtomicCoder<Boolean> {

	public void encode(Boolean value, OutputStream outStream,
			com.google.cloud.dataflow.sdk.coders.Coder.Context context) throws CoderException, IOException {
		// TODO Auto-generated method stub
		
	}

	public Boolean decode(InputStream inStream, com.google.cloud.dataflow.sdk.coders.Coder.Context context)
			throws CoderException, IOException {
		// TODO Auto-generated method stub
		return null;
	}


}
