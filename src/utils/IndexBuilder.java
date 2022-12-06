package utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IndexBuilder {
	int order;
	String outputPath;
	List<Rid> ridList = new ArrayList<>();
	int numKeys;
	int numEntries;
	int numLeaves;
	int key = -1;
	int countPosition = 12;
	int currPosition = 8;
	private static int PAGE_SIZE = 4096;
	List<Integer> keyHead = new ArrayList<>();

	/**
	 * Constructor for IndexBuilder
	 *
	 * @param tupleReader a tuple reader to read from table
	 * @param keyInd      the index of the key to build tree on
	 * @param order       the order of index tree
	 */
	public IndexBuilder(TupleReader tupleReader, int keyInd, int order) {
		this.order = order;
		String tableName = tupleReader.tableName;
		outputPath = Catalog.indexPath + tableName + '.' + Catalog.getSchema(tableName).get(keyInd);

		File file = new File(outputPath);
		try {
			file.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String[] tuple = tupleReader.nextTuple();
		while (tuple != null) {
			ridList.add(new Rid(Integer.valueOf(tuple[keyInd]), tupleReader.numPages(), tupleReader.numTuples()));
			tuple = tupleReader.nextTuple();
		}
		tupleReader.reset();
		Collections.sort(ridList, new RidComparator());
		buildLeafNodes();
		buildIndexNodes();
	}

	/**
	 * Build leaf nodes for this index tree
	 */
	private void buildLeafNodes() {
		numLeaves = 0;
		int index = 0;
		FileChannel outputChannel = null;
		ByteBuffer buffer = null;
		try {
			FileOutputStream outputStream = new FileOutputStream(new File(outputPath));
			outputChannel = outputStream.getChannel();
			buffer = ByteBuffer.allocate(PAGE_SIZE);
			outputChannel.write(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
		buffer = ByteBuffer.allocate(PAGE_SIZE);
		boolean run = true;
		while (run) {
			boolean a = addRid(buffer, index, order);
			if (a)
				index++;
			if (!a || index == ridList.size()) {
				buffer.putInt(4, numKeys);
				buffer.putInt(countPosition, numEntries);
				keyHead.add(buffer.getInt(8));
				try {
					outputChannel.write(buffer);
					numLeaves++;
				} catch (IOException e) {
					e.printStackTrace();
				}
				if (index == ridList.size()) {
					run = false;
				} else {
					buffer = ByteBuffer.allocate(PAGE_SIZE);
					resetVals();
				}
			}
		}
		processTail(buffer, outputChannel);
		try {
			outputChannel.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 *
	 * process last two pages of leaf nodes
	 *
	 * @param buffer        the buffer to copy from
	 * @param outputChannel the output file channel to write to
	 */
	private void processTail(ByteBuffer buffer, FileChannel outputChannel) {
		if (numKeys < order) {
			int half = (numKeys + order * 2) / 2;
			SeekableByteChannel outputCopy = null;
			ByteBuffer bufferCopy = null;
			try {
				buffer = ByteBuffer.allocate(PAGE_SIZE);
				bufferCopy = ByteBuffer.allocate(PAGE_SIZE);
				outputCopy = Files.newByteChannel(Paths.get(outputPath), StandardOpenOption.READ);
				outputCopy.position(outputChannel.position() - PAGE_SIZE * 2);
				outputCopy.read(bufferCopy);
			} catch (IOException e) {
				e.printStackTrace();
			}
			int positionCopy = 8;
			currPosition = 8;
			int tempCopy = 0;
			for (int i = 0; i < order * 2; i++) {
				int key = bufferCopy.getInt(positionCopy);
				int num = bufferCopy.getInt(positionCopy + 4);
				if (i >= half) {
					tempCopy = tempCopy == 0 ? positionCopy : tempCopy;
					buffer.putInt(currPosition, key);
					buffer.putInt(currPosition + 4, num);
					for (int j = 0; j < num; j++) {
						int newVal1 = bufferCopy.getInt(positionCopy + 8 + j * 8);
						buffer.putInt(currPosition + 8 + j * 8, newVal1);
						int newVal2 = bufferCopy.getInt(positionCopy + 8 + j * 8 + 4);
						buffer.putInt(currPosition + 12 + j * 8, newVal2);
					}
					currPosition += 8 * (num + 1);
				}
				positionCopy += num * 8 + 8;
				if (i + 1 == half) {
					keyHead.set(keyHead.size() - 1, bufferCopy.getInt(positionCopy));
				}
			}
			bufferCopy.putInt(4, half);
			while (tempCopy < PAGE_SIZE) {
				bufferCopy.putInt(tempCopy, 0);
				tempCopy += 4;
			}
			try {
				bufferCopy.rewind();
				outputChannel.position(outputChannel.position() - PAGE_SIZE * 2);
				outputChannel.write(bufferCopy);
				outputCopy.position(outputChannel.position());
				bufferCopy = ByteBuffer.allocate(PAGE_SIZE);
				outputCopy.read(bufferCopy);
			} catch (IOException e) {
				e.printStackTrace();
			}
			positionCopy = 8;
			int newSize = bufferCopy.getInt(4);
			for (int i = 0; i < newSize; i++) {
				int key = bufferCopy.getInt(positionCopy);
				int num = bufferCopy.getInt(positionCopy + 4);
				buffer.putInt(currPosition, key);
				buffer.putInt(currPosition + 4, num);
				for (int j = 0; j < num; j++) {
					int newVal1 = bufferCopy.getInt(positionCopy + 8 + j * 8);
					buffer.putInt(currPosition + j * 8 + 8, newVal1);
					int newVal2 = bufferCopy.getInt(positionCopy + 8 + j * 8 + 4);
					buffer.putInt(currPosition + j * 8 + 12, newVal2);
				}
				currPosition += 8 + 8 * num;
				positionCopy += 8 + 8 * num;
			}
			buffer.putInt(4, 2 * order + numKeys - half);
			try {
				outputChannel.write(buffer);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * Build index nodes. When the number of remaining entries is between 2*order+1
	 * and 3*order+2, put each half in one page.
	 */
	private void buildIndexNodes() {
		SeekableByteChannel seekableByteChannel = null;
		ByteBuffer buffer = null;
		try {
			seekableByteChannel = Files.newByteChannel(Paths.get(outputPath), StandardOpenOption.WRITE);
			seekableByteChannel.position((1 + numLeaves) * PAGE_SIZE);
		} catch (IOException e) {
			e.printStackTrace();
		}
		int keyIndex = 0;
		int offset = 1;
		buffer = ByteBuffer.allocate(PAGE_SIZE);
		if (keyHead.size() == 1) {
			buffer.putInt(0, 1);
			buffer.putInt(4, 0);
			buffer.putInt(8, 1);
			try {
				seekableByteChannel.write(buffer);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		while (keyHead.size() > 1) {
			List<Integer> newKeyHead = new ArrayList<>();
			int bufferIndex = 8;
			while (keyHead.size() - keyIndex >= order * 3 + 2) {
				buffer.putInt(0, 1);
				buffer.putInt(4, order * 2);
				buffer.putInt(bufferIndex + order * 8, keyIndex + offset);
				newKeyHead.add(keyHead.get(keyIndex));
				keyIndex += 1;
				int[] children = new int[order * 2];
				for (int i = 0; i < order * 2; i++) {
					buffer.putInt(bufferIndex, keyHead.get(keyIndex));
					bufferIndex += 4;
					buffer.putInt(bufferIndex + order * 8, keyIndex + offset);
					children[i] = keyIndex + offset;
					keyIndex += 1;
				}

				try {
					seekableByteChannel.write(buffer);
					buffer = ByteBuffer.allocate(PAGE_SIZE);
					bufferIndex = 8;
				} catch (IOException e) {
					e.printStackTrace();

				}
			}

			if (keyHead.size() - keyIndex > order * 2) {
				int m = (keyHead.size() - keyIndex) / 2;
				int remains = keyHead.size() - keyIndex - m - 1;
				int k = m - 1;
				buffer.putInt(0, 1);
				buffer.putInt(4, k);
				buffer.putInt(bufferIndex + k * 4, keyIndex + offset);
				newKeyHead.add(keyHead.get(keyIndex));
				keyIndex += 1;
				int[] children = new int[k];
				for (int i = 0; i < k; i++) {
					buffer.putInt(bufferIndex, keyHead.get(keyIndex));
					bufferIndex += 4;
					buffer.putInt(bufferIndex + k * 4, keyIndex + offset);
					children[i] = keyIndex + offset;
					keyIndex += 1;
				}
				try {
					seekableByteChannel.write(buffer);
					buffer = ByteBuffer.allocate(PAGE_SIZE);
					bufferIndex = 8;
				} catch (IOException e) {
					e.printStackTrace();
				}
				buffer.putInt(0, 1);
				buffer.putInt(4, remains);
				buffer.putInt(bufferIndex + remains * 4, keyIndex + offset);
				newKeyHead.add(keyHead.get(keyIndex));
				keyIndex += 1;
				children = new int[remains];
				for (int i = 0; i < remains; i++) {
					buffer.putInt(bufferIndex, keyHead.get(keyIndex));
					bufferIndex += 4;
					buffer.putInt(bufferIndex + remains * 4, keyIndex + offset);
					children[i] = keyIndex + offset;
					keyIndex += 1;
				}
				try {
					seekableByteChannel.write(buffer);
					buffer = ByteBuffer.allocate(PAGE_SIZE);
					bufferIndex = 8;
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				int k = keyHead.size() - keyIndex - 1;
				buffer.putInt(0, 1);
				buffer.putInt(4, k);
				buffer.putInt(bufferIndex + k * 4, keyIndex + offset);
				newKeyHead.add(keyHead.get(keyIndex));
				keyIndex += 1;
				int[] children = new int[k];
				for (int i = 0; i < k; i++) {
					buffer.putInt(bufferIndex, keyHead.get(keyIndex));
					bufferIndex += 4;
					buffer.putInt(bufferIndex + k * 4, keyIndex + offset);
					children[i] = keyIndex + offset;
					keyIndex += 1;
				}
				try {
					seekableByteChannel.write(buffer);
					buffer = ByteBuffer.allocate(PAGE_SIZE);
					bufferIndex = 8;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			offset += keyIndex;
			keyHead = newKeyHead;
			keyIndex = 0;
		}
		buildHeaderNode(offset, seekableByteChannel);
	}

	private void buildHeaderNode(int rootAdr, SeekableByteChannel seekableByteChannel) {
		ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
		buffer.putInt(0, rootAdr);
		buffer.putInt(4, numLeaves);
		buffer.putInt(8, order);
		try {
			seekableByteChannel.position(0);
			seekableByteChannel.write(buffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void resetVals() {
		key = -1;
		numKeys = 0;
		numEntries = 0;
		currPosition = 8;
		countPosition = 12;
	}

	private boolean addRid(ByteBuffer buffer, int index, int order) {
		Rid entry = ridList.get(index);
		if (entry.key == key) {
			numEntries++;
			buffer.putInt(currPosition, entry.pageId);
			currPosition += 4;
			buffer.putInt(currPosition, entry.tupleId);
			currPosition += 4;
		} else {
			buffer.putInt(countPosition, numEntries);
			if (numKeys == order * 2) {
				return false;
			}
			numKeys += 1;
			numEntries = 1;
			key = entry.key;
			buffer.putInt(currPosition, entry.key);
			countPosition = currPosition + 4;
			buffer.putInt(currPosition + 8, entry.pageId);
			buffer.putInt(currPosition + 12, entry.tupleId);
			currPosition += 16;
		}
		return true;
	}
}
