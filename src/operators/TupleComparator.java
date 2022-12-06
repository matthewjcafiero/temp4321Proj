package operators;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import utils.Catalog;
import utils.Tuple;

/**
 * Tuple Compartor, compares two tuples for internal sort
 */
public class TupleComparator implements Comparator<Tuple> {

	List<Column> cols = new ArrayList<>();
	HashSet<String> orderKey = new HashSet<>();

	/**
	 * Create a compare object with order
	 *
	 * @param cols the list of schema of order
	 */
	public TupleComparator(List<Column> cols) {
		this.cols = cols;
		for (Column col : cols) {
			orderKey.add(col.getWholeColumnName());
		}
	}

	/**
	 * Create a compare object ordered by index
	 *
	 * @param tableName String of table name
	 */
	public TupleComparator(String tableName) {
		List<Column> res = new ArrayList<>();
		Column col = new Column();
		Table t = new Table();
		t.setName(tableName);
		String[] indexVal = Catalog.indexMap.get(tableName);
		col.setColumnName(indexVal[1]);
		col.setTable(t);
		res.add(col);
		cols = res;
		for (Column col1 : cols) {
			orderKey.add(col1.getWholeColumnName());
		}
	}

	/**
	 * Comparison function for two tuples Compares tuples based on columns in query
	 * If two tuples tie, compare them based on the remaining schema
	 *
	 * @param tuple1 first tuple to compare
	 * @param tuple2 second tuple to compare
	 *
	 * @return the value 0 if tuple1 == tuple2;a value less than 0 if tuple1 is
	 *         smaller; and a value greater than 0 if tuple1 is larger
	 */
	@Override
	public int compare(Tuple tuple1, Tuple tuple2) {
		if (tuple1 == null && tuple2 == null) {
			return 0;
		} else if (tuple1 == null) {
			return -1;
		} else if (tuple2 == null) {
			return 1;
		}

		for (Column col : cols) {
			long val1 = tuple1.getValByCol(col);
			long val2 = tuple2.getValByCol(col);
			int cmp = Long.compare(val1, val2);
			if (cmp != 0)
				return cmp;
		}
		// sort based on remaining schemas
		for (String colName : tuple1.getSchemaNames()) {
			if (isInOrderKey(colName)) {
				continue;
			}
			int idx1 = tuple1.getSchema().get(colName);
			Long val1 = tuple1.getAll().get(idx1);
			int idx2 = tuple2.getSchema().get(colName);
			Long val2 = tuple2.getAll().get(idx2);
			int cmp = Long.compare(val1, val2);
			if (cmp != 0)
				return cmp;
		}
		return 0;
	}

	/**
	 * @param colName check if column name is the specified order keys
	 *
	 * @return true if column name is contained, false otherwise
	 */
	private boolean isInOrderKey(String colName) {
		if (orderKey.contains(colName)) {
			return true;
		}
		if (colName.contains("\\.") && orderKey.contains(colName.split("\\.")[1])) {
			return true;
		}
		return false;
	}

}
