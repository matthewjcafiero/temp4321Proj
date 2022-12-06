package utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * TupleReader, reads tuples from a binary file using I/O
 */
public class TupleReader {

	private File file;
	public String filePath;
	public String tableName;
	private FileInputStream inputStream;
	private FileChannel fileChannel;
	private ByteBuffer buffer;
	private int numAttr;
	private int size;
	private int count;
	private int currIndex;
	private static int pageSize = 4096;
	private int pageNum = -1;
	private List<Integer> tupleCounts = new ArrayList<>();

	/**
	 * Read tuples from a table
	 *
	 * @param tableName name of table that needs reading
	 */
	public TupleReader(String tableName, boolean isSortTable) {
		try {
			this.tableName = tableName;
			if (isSortTable) {
				filePath = tableName;
			} else {
				filePath = Catalog.inputPath + File.separator + "db" + File.separator + "data" + File.separator
						+ tableName;
			}
			file = new File(filePath);
			inputStream = new FileInputStream(file);
			fileChannel = inputStream.getChannel();
			buffer = ByteBuffer.allocate(pageSize);
			tupleCounts.add(0);
			readPage();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Get the next page in the table
	 *
	 * @return true if reader reaches the end of file, false otherwise
	 */
	private boolean readPage() {
		try {
			buffer.clear();
			buffer.put(new byte[pageSize]);
			buffer.clear();
			int bytesRead = fileChannel.read(buffer);
			numAttr = buffer.getInt(0);
			size = buffer.getInt(4);
			currIndex = 8;
			pageNum++;
			if (bytesRead < 0) {
				return false;
			} else {
				int newCount = tupleCounts.get(tupleCounts.size() - 1) + size;
				tupleCounts.add(newCount);
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Return the number of attributes in a tuple
	 *
	 * @return the number of attributes
	 */
	public int getNumAttri() {
		return numAttr;
	}

	/**
	 * Return the number of tuples in a page
	 *
	 * @return the number of tuples in a page
	 */
	public int getSize() {
		return size;
	}

	/**
	 * Returns the file the reader is connected to
	 *
	 * @return the file currently reading from
	 */
	public String getFile() {
		return filePath;
	}

	/**
	 *
	 * get the number of pages in tuple reader
	 *
	 * @return the number of pages in tuple reader currently
	 */
	public int numPages() {
		return pageNum;
	}

	/**
	 *
	 * get the number of tuples of tuple reader
	 *
	 * @return the number of tuples in tuple reader currently
	 */
	public int numTuples() {
		return count - 1;
	}

	/**
	 * Get next tuple from buffer
	 *
	 * @return an array containing the next tuple
	 */
	public String[] nextTuple() {

		String[] ans = new String[numAttr];
		if (count < size && currIndex < buffer.capacity()) {
			for (int i = 0; i < numAttr; i++) {
				ans[i] = String.valueOf(buffer.getInt(currIndex));
				currIndex += 4;
			}
			count++;
			return ans;
		} else if (readPage()) {
			count = 0;
			return nextTuple();
		}
		return null;
	}

	/**
	 * Get next tuple from buffer by rid
	 *
	 * @param rid the int array as tuple identifier: [pageid, tupleid]
	 *
	 * @return an array containing the next tuple
	 */
	public String[] nextTupleByRid(int[] rid) {
		int pageId = rid[0];
		int tupleId = rid[1];

		// buffer setting up
		buffer.clear();
		buffer.put(new byte[pageSize]);
		buffer.clear();

		try {
			fileChannel.position(pageId * pageSize);
		} catch (IOException e) {
			e.printStackTrace();
		}
		readPage();

		count = 0;
		currIndex = numAttr * tupleId * 4 + 8;
		buffer.position(currIndex);

		return nextTuple();
	}

	/** Close the tuple reader */
	public void close() {
		try {
			inputStream.close();
			fileChannel.close();
			numAttr = 0;
			size = 0;
			count = 0;
			currIndex = 0;
			tupleCounts = null;
			buffer.clear();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/** Reset the tuple reader to the beginning */
	public void reset() {
		try {
			close();
			inputStream = new FileInputStream(file);
			fileChannel = inputStream.getChannel();
			buffer = ByteBuffer.allocate(pageSize);
			tupleCounts = new ArrayList<>();
			tupleCounts.add(0);
			readPage();
			tupleCounts.add(0);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Reset the tuple reader to a specified index
	 *
	 * @param index the index we want to reset to
	 */
	public void reset(int index) {
		int pageNum = Collections.binarySearch(tupleCounts, index);
		if (pageNum < 0) {
			pageNum = -pageNum - 2;
		}
		try {
			fileChannel.position(pageNum * pageSize);
		} catch (IOException e) {
			e.printStackTrace();
		}
		List<Integer> copies = tupleCounts.subList(0, pageNum + 1);
		tupleCounts = new ArrayList<>();
		tupleCounts.addAll(copies);
		readPage();
		count = index - tupleCounts.get(pageNum);
		currIndex = numAttr * count * 4 + 8;
		buffer.position(currIndex);
	}

	/**
	 * Resets the tuple reader to a specified rid [pid, tid]
	 *
	 * @param rid the int array as tuple identifier: [pageid, tupleid]
	 */
	public void reset(int[] rid) {
		int pageId = rid[0];
		int tupleId = rid[1];

		// buffer setting up
		buffer.clear();
		buffer.put(new byte[pageSize]);
		buffer.clear();

		try {
			fileChannel.position(pageId * pageSize);
		} catch (IOException e) {
			e.printStackTrace();
		}

		readPage();

		count = tupleId;
		currIndex = numAttr * count * 4 + 8;
		buffer.position(currIndex);
	}

}
