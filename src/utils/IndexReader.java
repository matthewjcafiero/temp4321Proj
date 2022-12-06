package utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class IndexReader {
	private FileInputStream fileInputStream;
	private FileChannel fileChannel;
	private ByteBuffer buffer;
	private String tableName;
	private File indexFile;
	private Integer lower;
	private Integer higher;
	private int root;
	private int leafNum;
	private int currPage;
	private int dataNum;
	private int entryStartIdx;
	private int entryKeyIdx;
	private int entryCountIdx;
	private int nextKeyIdx;
	private int count;
	private static int PAGE_SIZE = 4096;

	/**
	 * Create the IndexReader
	 *
	 * @param tableName the full name of the table
	 * @param lowKey    the lower bound of the key
	 * @param highKey   the upper bound of the key range
	 */
	public IndexReader(String tableName, Integer lowKey, Integer highKey) {
		this.tableName = tableName;
		lower = lowKey == null ? Integer.MIN_VALUE : lowKey;
		higher = highKey == null ? Integer.MAX_VALUE : highKey;
		indexFile = new File(Catalog.indexPath + this.tableName + "." + Catalog.indexMap.get(tableName)[1]);
		try {
			fileInputStream = new FileInputStream(indexFile);
			fileChannel = fileInputStream.getChannel();
			buffer = ByteBuffer.allocate(PAGE_SIZE);
			fileChannel.read(buffer);
			root = buffer.getInt(0);
			leafNum = buffer.getInt(4);
			int leafPage = readPage(root, lower);
			currPage = leafPage;
			readLeafPage(currPage);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 *
	 * Traverse downward to find leaf page
	 *
	 * @param pageNum the page number we're currently on
	 * @param key     the key to look for
	 * @return the final leaf page corresponding to key
	 */
	private int readPage(int pageNum, Integer key) {
		int nextPage = readIndexPage(pageNum, key);
		while (nextPage > leafNum) {
			nextPage = readIndexPage(nextPage, key);
		}
		return nextPage;
	}

	/**
	 * Read index page
	 *
	 * @param pageNum the number of the index page
	 * @param key     the key to look for
	 * @return the child node number
	 */
	private int readIndexPage(int pageNum, Integer key) {
		buffer.clear();
		buffer.put(new byte[PAGE_SIZE]);
		buffer.clear();
		try {
			fileChannel.position(pageNum * PAGE_SIZE);
			fileChannel.read(buffer);
			int keyNum = buffer.getInt(4);
			int childIdx = 4 * keyNum + 8;
			if (buffer.getInt(8) > key) {
				return buffer.getInt(childIdx);
			}
			for (int i = 0; i < keyNum - 1; i++) {
				if (key >= buffer.getInt(8 + i * 4) && key < buffer.getInt(i * 4 + 12)) {
					return buffer.getInt(childIdx + i * 4 + 4);
				}
			}
			if (key >= buffer.getInt(keyNum * 4 + 4)) {
				return buffer.getInt(childIdx + keyNum * 4);
			}
		} catch (IOException e) {
			return -1;
		}
		return -1;
	}

	/**
	 * Read a leaf node
	 *
	 * @param pageIdx the index of the leaf page
	 * @return true if the leaf page reads without fail
	 */
	private boolean readLeafPage(int pageIdx) {
		buffer.clear();
		buffer.put(new byte[PAGE_SIZE]);
		buffer.clear();
		try {
			fileChannel.position(pageIdx * PAGE_SIZE);
			int bytesRead = fileChannel.read(buffer);
			dataNum = buffer.getInt(4);
			entryKeyIdx = 8;
			entryCountIdx = entryKeyIdx + 4;
			entryStartIdx = entryCountIdx + 4;
			nextKeyIdx = entryKeyIdx + (buffer.getInt(entryCountIdx) + 1) * 8;
			count = 1;
			boolean isIndex = buffer.getInt(0) == 1;
			if (bytesRead < 0 || isIndex) {
				return false;
			}
			currPage += 1;
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Returns the rid according to the keyL
	 *
	 * @return found rid in the int array form of [pageid, tupleid]. empty int array
	 *         if not found.
	 */
	public int[] findRidByKeyL(Integer keyL) {
		if (keyL != null) {
			while (buffer.getInt(entryKeyIdx) < keyL) {
				try {
					updateIndexes();
				} catch (Exception e) {
					return null;
				}
			}
		}
		// initially empty
		int[] rid = new int[2];
		rid[0] = buffer.getInt(entryStartIdx);
		rid[1] = buffer.getInt(entryStartIdx + 4);
		entryStartIdx += 8;
		return rid;
	}

	/**
	 * Get next rid from leaf node
	 *
	 * @return the next rid stored in an int array
	 */
	public int[] nextRid() {
		int[] rid = new int[2];
		while (buffer.getInt(entryKeyIdx) < lower) {
			try {
				updateIndexes();
			} catch (Exception e) {
				return null;
			}
		}
		int currKey = buffer.getInt(entryKeyIdx);
		if (count <= dataNum && entryStartIdx < buffer.capacity() && currKey >= lower && currKey <= higher
				&& entryStartIdx < nextKeyIdx) {
			rid[0] = buffer.getInt(entryStartIdx);
			rid[1] = buffer.getInt(entryStartIdx + 4);
			entryStartIdx += 8;
			return rid;
		} else if ((entryStartIdx >= buffer.capacity() || count >= dataNum) && currKey <= higher) {
			if (readLeafPage(currPage)) {
				return nextRid();
			}
		} else if (entryStartIdx < buffer.capacity() && entryStartIdx == nextKeyIdx && count < dataNum) {
			updateIndexes();
			return nextRid();
		}
		return null;
	}

	/**
	 * update indexes to move to the next entry
	 */
	private void updateIndexes() {
		entryCountIdx = entryKeyIdx + 4;
		entryStartIdx = entryCountIdx + 4;
		nextKeyIdx = entryKeyIdx + 8 * (buffer.getInt(entryCountIdx) + 1);
		entryKeyIdx = nextKeyIdx;
		entryCountIdx = entryKeyIdx + 4;
		entryStartIdx = entryCountIdx + 4;
		nextKeyIdx = entryKeyIdx + 8 * (buffer.getInt(entryCountIdx) + 1);
		count++;
	}

	/**
	 * Reset the index tree reader
	 */
	public void reset() {
		try {
			fileChannel.close();
			fileInputStream.close();
			fileInputStream = new FileInputStream(indexFile);
			fileChannel = fileInputStream.getChannel();
			buffer = ByteBuffer.allocate(PAGE_SIZE);
			fileChannel.read(buffer);
			root = buffer.getInt(0);
			leafNum = buffer.getInt(4);
			int leafPage = readPage(root, lower);
			currPage = leafPage;
			readLeafPage(currPage);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
