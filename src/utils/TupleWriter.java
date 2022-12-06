package utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * TupleWriter, writes tuples to a binary file using I/O
 */
public class TupleWriter {
	private FileOutputStream outputStream = null;
	private FileChannel fileChannel;
	private ByteBuffer buffer;
	private int numAttr;
	private int count;
	private int currIndex;
	private int maxlines;
	static int pageSize = 4096;
	private STATE state = STATE.READY;

	private enum STATE {
		READY, WRITING, NEXTPAGE
	}

	/**
	 * Write tuples to a file using output path
	 *
	 * @param outputPath the path for writing the output file
	 */
	public TupleWriter(String outputPath) {
		try {
			outputStream = new FileOutputStream(new File(outputPath));
			fileChannel = outputStream.getChannel();
			buffer = ByteBuffer.allocate(pageSize);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Write tuple to specified output file
	 *
	 * @param tuple the tuple that client writes to the output file
	 */
	public void writeTuple(Tuple tuple) throws IOException {
		boolean written = false;
		while (!written) {
			if (state == STATE.READY) {
				currIndex = 0;
				numAttr = tuple.getAll().size();
				buffer.putInt(0, numAttr);
				buffer.putInt(4, 0);
				count = 0;
				maxlines = (buffer.capacity() - 8) / 4 / numAttr;
				if (count >= maxlines)
					state = STATE.NEXTPAGE;
				else
					state = STATE.WRITING;
				currIndex = 8;
			} else if (state == STATE.WRITING) {
				for (Long num : tuple.getAll()) {
					int value = num.intValue();
					buffer.putInt(currIndex, value);
					currIndex += 4;
				}
				count++;
				if (count == maxlines || currIndex == pageSize)
					state = STATE.NEXTPAGE;
				written = true;
			} else {
				fillPage();
				buffer.putInt(4, count);
				fileChannel.write(buffer);
				buffer.clear();
				buffer.put(new byte[pageSize]);
				buffer.clear();
				state = STATE.READY;
			}
		}
	}

	/** fill remaining space on page with 0s */
	private void fillPage() {
		while (currIndex < pageSize) {
			buffer.putInt(currIndex, 0);
			currIndex += 4;
		}
	}

	/** close the Tuple writer */
	public void close() {
		buffer.putInt(4, count);
		buffer.clear();
		try {
			fileChannel.write(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			fileChannel.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			outputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
