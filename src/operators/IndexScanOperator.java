package operators;

import utils.Catalog;
import utils.IndexReader;
import utils.MyTable;
import utils.Tuple;

/**
 * An index scan is like a file scan, except that it will only retrieve
 * a range (subset) of tuples from a relation file, using a B+ Tree Index.
 *
 * Every index scan operator needs to know: 1. the relation to scan, 2. the
 * index to use, 3. whether the index is clustered or not, and 4. two parameters
 * that set the range of the scan: lowkey and highkey (can be set to null to 
 * indicate the lack of a bound.
 */
public class IndexScanOperator extends ScanOperator {

	private String tableFullName;

	/**
	 * Lower bound and Upper bound of the index range. One of lowkey and highkey can
	 * be set to null, to indicate the lack of a bound.
	 */
	private Integer keyL = null;
	private Integer keyH = null;

	/**
	 * Index Reader that deserializes a number of nodes in the tree; does not
	 * deserialize the whole tree, only the pages needed.
	 *
	 * Create to use later in getNextTuple().
	 */
	private IndexReader indexReader;

	/**
	 * List, storing a table's configuration. e.g. Sailors A 0 10: index on
	 * Sailors.A, unclustered (0), order 10 e.g. Boats E 1 20: index on Boats.E,
	 * clustered (1), with order 20.
	 */
	public String[] tableConfig = {};

	static final int Unclustered = 0;
	static final int Clustered = 1;
	/**
	 * Enum, representing Clustering configuration of index. If 0: unclustered
	 * index. index stores references to data. If 1: clustered index. index stores
	 * data.
	 */
	private Integer indexClustered;

	/**
	 * Constructor that creates new Index Scan Operator object
	 *
	 * @param table      a MyTable object that will be scanned
	 * @param lowerBound lowest index of scan range
	 * @param upperBound highest index of scan range
	 */
	public IndexScanOperator(MyTable table, Integer lowerBound, Integer upperBound) {
		super(table);
		tableFullName = table.getTableFullName();

		keyL = lowerBound;
		keyH = upperBound;

		indexReader = new IndexReader(tableFullName, keyL, keyH);

		tableConfig = Catalog.indexMap.get(table.getTableFullName());
		indexClustered = Integer.parseInt(tableConfig[2]);

		// if index is clustered (i.e. index stores data)
		if (indexClustered == Clustered) {
			// reset the tuple reader for the current table
			// to the address of the first entry we found by keyL
			table.reset(indexReader.findRidByKeyL(keyL));

			// if no upper bound (i.e. unlimited rolling),
			// set keyH to super large value so we will not stop scrolling
			if (null == keyH) {
				keyH = Integer.MAX_VALUE;
			}
		}
	}

	/**
	 * retrieves tuples one at a time using the index.
	 * 
	 * @return next tuple in the table
	 */
	@Override
	public Tuple getNextTuple() {
		// if Unclustered index (i.e. index stores references to data):
		// examines the current data entry, find the next rid, 
		// resolve that rid to a page and a tuple within the data file,
		// retrieve the tuple from the data file, and return it.
		if (indexClustered == Unclustered) {
			int[] next = indexReader.nextRid();
			if (next == null) {
				return null;
			}
			return table.getNextTuple(next);
		}
		// if Clustered index (i.e. index stores data):
		// scan the (sorted) data file itself sequentially
		// rather than going through the index for each tuple.
		else {
			return table.getNextTupleInBound(keyH == Integer.MAX_VALUE ? Integer.MAX_VALUE : keyH + 1);
		}
	}

	/**
	 * Resets entire table by reseting the index tree reader.
	 */
	@Override
	public void reset() {
		indexReader.reset();
	}
}
