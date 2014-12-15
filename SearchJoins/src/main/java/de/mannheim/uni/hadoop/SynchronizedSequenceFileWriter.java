package de.mannheim.uni.hadoop;

import de.mannheim.uni.utils.concurrent.SynchronizedWriter;

public abstract class SynchronizedSequenceFileWriter<T> extends
		SynchronizedWriter<T> {
	private TextSequenceFileWriter writer;

	public SynchronizedSequenceFileWriter(String fileName) throws Exception {
		super(fileName);
	}

	@Override
	public long getApproximateLength() throws Exception {
		return writer.getApproximateLength();
	}
	
	@Override
	protected void createWriter(String file) throws Exception {
		writer = new TextSequenceFileWriter(file);
	}

	@Override
	protected void writeData(T data) throws Exception {
		writer.write(getKey(data), getValue(data));
	}

	@Override
	protected void flushWriter() throws Exception {
	}

	@Override
	protected void closeWriter() throws Exception {
		writer.close();
	}

	protected abstract String getKey(T data);

	protected abstract String getValue(T data);

}
