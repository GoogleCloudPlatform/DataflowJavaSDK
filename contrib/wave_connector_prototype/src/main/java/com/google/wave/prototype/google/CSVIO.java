package com.google.wave.prototype.google;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.NoSuchElementException;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.FileBasedSink;
import com.google.cloud.dataflow.sdk.io.FileBasedSink.FileBasedWriteOperation;
import com.google.cloud.dataflow.sdk.io.FileBasedSink.FileBasedWriter;
import com.google.cloud.dataflow.sdk.io.FileBasedSource;
import com.google.cloud.dataflow.sdk.io.FileBasedSource.FileBasedReader;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Write;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;

/**
 * This is just like LineSource provided as example 
 */
public class CSVIO {

	public static class CSVSource extends FileBasedSource<String> {

		private static final long serialVersionUID = 3652362361L;

		public static Read.Bound<String> readFrom(String fileName) {
			return Read.from(new CSVSource(fileName, 1));
		}
		
		public CSVSource(String fileOrPatternSpec, long minBundleSize) {
			super(fileOrPatternSpec, minBundleSize);
		}

		public CSVSource(String fileName, long minBundleSize, long start,
				long end) {
			super(fileName, minBundleSize, start, end);
		}

		@Override
		public FileBasedSource<String> createForSubrangeOfFile(String fileName,
				long start, long end) {
			return new CSVSource(fileName, getMinBundleSize(), start, end);
		}

		@Override
		public FileBasedReader<String> createSingleFileReader(
				PipelineOptions options, ExecutionContext executionContext) {
			return new CSVReader(this);
		}

		@Override
		public boolean producesSortedKeys(PipelineOptions options)
				throws Exception {
			return false;
		}

		@Override
		public Coder<String> getDefaultOutputCoder() {
			return StringUtf8Coder.of();
		}

	}
	
	public static class CSVReader extends FileBasedReader<String> {
		private final byte boundary;
	    private long nextOffset = 0;
	    private long currentOffset = 0;
	    private ReadableByteChannel channel;
	    private final ByteBuffer buf;
	    private static final int BUF_SIZE = 1024;
	    private boolean isAtSplitPoint = false;
	    private String currentValue = null;

		public CSVReader(CSVSource source) {
			super(source);
			boundary = '\n';
			buf = ByteBuffer.allocate(BUF_SIZE);
			buf.flip();
		}

		@Override
		public String getCurrent() throws NoSuchElementException {
			return currentValue;
		}

		@Override
		protected boolean isAtSplitPoint() {
			return isAtSplitPoint;
		}

		@Override
		protected void startReading(ReadableByteChannel channel)
				throws IOException {
			boolean removeLine = false;
			if (getCurrentSource().getMode() == FileBasedSource.Mode.SINGLE_FILE_OR_SUBRANGE) {
				SeekableByteChannel seekChannel = (SeekableByteChannel) channel;
				// If we are not at the beginning of a line, we should ignore
				// the current line.
				if (seekChannel.position() > 0) {
					// Start from one character back and read till we find a new
					// line.
					seekChannel.position(seekChannel.position() - 1);
					removeLine = true;
				}
				nextOffset = seekChannel.position();
			}
			this.channel = channel;
			if (removeLine) {
				nextOffset += readNextLine(new ByteArrayOutputStream());
			}
			// Done to avoid reading the headers
			nextOffset += readNextLine(new ByteArrayOutputStream());
		}

		@Override
		protected boolean readNextRecord() throws IOException {
		      currentOffset = nextOffset;

		      ByteArrayOutputStream buf = new ByteArrayOutputStream();
		      int offsetAdjustment = readNextLine(buf);
		      if (offsetAdjustment == 0) {
		        // EOF
		        return false;
		      }
		      nextOffset += offsetAdjustment;
		      isAtSplitPoint = true;
		      currentValue = CoderUtils.decodeFromByteArray(StringUtf8Coder.of(), buf.toByteArray());
		      return true;
		}

		@Override
		protected long getCurrentOffset() {
			return currentOffset;
		}
		
		private int readNextLine(ByteArrayOutputStream out) throws IOException {
			int byteCount = 0;
			while (true) {
				if (!buf.hasRemaining()) {
					buf.clear();
					int read = channel.read(buf);
					if (read < 0) {
						break;
					}
					buf.flip();
				}
				byte b = buf.get();
				byteCount++;
				if (b == boundary) {
					break;
				}
				out.write(b);
			}
			return byteCount;
		}
	}
	
	public static class CSVSink extends FileBasedSink<String> {

		public static Write.Bound<String> writeTo(String fileName, String extension) {
			return Write.to(new CSVSink(fileName, extension));
		}

		public CSVSink(String baseOutputFilename, String extension) {
			super(baseOutputFilename, extension);
		}

		private static final long serialVersionUID = 8030053428206934383L;

		@Override
		public com.google.cloud.dataflow.sdk.io.FileBasedSink.FileBasedWriteOperation<String> createWriteOperation(
				PipelineOptions options) {
			return new CSVBasedWriteOperation(this);
		}
	}
	
	public static class CSVBasedWriteOperation extends FileBasedWriteOperation<String> {

		private static final long serialVersionUID = 1657901902080967254L;

		public CSVBasedWriteOperation(FileBasedSink<String> sink) {
			super(sink);
		}

		@Override
		public FileBasedWriter<String> createWriter(PipelineOptions options)
				throws Exception {
			return new CSVWriter(this);
		}
	}
	
	public static class CSVWriter extends FileBasedWriter<String> {
	    private OutputStream os = null;

		public CSVWriter(FileBasedWriteOperation<String> writeOperation) {
			super(writeOperation);
		}

		@Override
		protected void prepareWrite(WritableByteChannel channel)
				throws Exception {
			os = Channels.newOutputStream(channel);
		}

		@Override
		public void write(String value) throws Exception {
			os.write(value.getBytes());
			os.write("\n".getBytes());
		}
		
	}
}
