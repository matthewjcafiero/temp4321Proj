package utils;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;

/**
 * MyTable represents a database table
 */
public class MyTable {
	/** tableName should be a unique name across each SQL statement */
	private String tableName;
	private String fullName;
	private TupleReader tupleReader;
	// TODO: columnNames is only used to toString(), do we need it?
	private List<String> columnNames = new ArrayList<>();

	/**
	 * Gets a table from a FromItem
	 *
	 * @param input FromItem representing the table and a potential alias for said
	 *              table
	 * @return a MyTable representation of the data
	 */
	public MyTable(FromItem item) {
		Table table = (Table) item;
		tableName = table.getAlias();
		fullName = table.getWholeTableName();
		if (tableName == null) {
			tableName = fullName;
		}
		tupleReader = Catalog.getTableReader(fullName);
		List<String> cols = Catalog.getSchema(fullName);
		for (String col : cols) {
			columnNames.add(tableName + "." + col);
		}
	}

	/**
	 * get the full name of table
	 *
	 * @return full name of the table
	 */
	public String getTableFullName() {
		return fullName;
	}

	/**
	 * gets alias of the table
	 *
	 * @return alias of the table
	 */
	public String getTableAlias() {
		return tableName;
	}

	/** resets index to point at start of table */
	public void reset() {
		tupleReader.close();
		tupleReader = Catalog.getTableReader(fullName);
	}

	/**
	 * reset the tuple reader for the current table to the address of the first
	 * entry we found by keyL
	 *
	 * @param rid the int array as tuple identifier: [pageid, tupleid]
	 */
	public void reset(int[] rid) {
		tupleReader.reset(rid);
	}

	/**
	 * Displays data of this table as a string
	 *
	 * @return string representation of all data in this table
	 */
	@Override
	public String toString() {
		String result = "table name: " + tableName + "\n";
		result += "column names: ";
		for (int i = 0; i < columnNames.size(); i++) {
			result += columnNames.get(i) + ", ";
		}
		result += "\n";
		return result;
	}

	/**
	 * Gets the next tuple of this table based on this table current tupleIndex
	 *
	 * @return next Tuple in the table
	 */
	public Tuple getNextTuple() {
		String[] ans = tupleReader.nextTuple();
		if (ans == null)
			return null;
		return new Tuple(ans, tableName, Catalog.getSchema(fullName));
	}

	/**
	 * Gets the next tuple of this table based on rid
	 *
	 * @param rid the int array as tuple identifier: [pageid, tupleid]
	 *
	 * @return next Tuple in the table
	 */
	public Tuple getNextTuple(int[] rid) {
		String[] ans = tupleReader.nextTupleByRid(rid);
		if (ans == null)
			return null;

		Tuple t = new Tuple(ans, tableName, Catalog.getSchema(fullName));
		return t;
	}

	/**
	 * Gets the next tuple of this table that is LessThan the upper bound
	 *
	 * @param keyH the upper bound of index range
	 *
	 * @return next Tuple in the table
	 */
	public Tuple getNextTupleInBound(int keyH) {
		String[] ans = tupleReader.nextTuple();
		if (ans == null)
			return null;

		Tuple t = new Tuple(ans, tableName, Catalog.getSchema(fullName));

		// get the column that is indexed on
		Column col = new Column();
		Table table = new Table();
		table.setName(tableName);
		String[] indexVal = Catalog.indexMap.get(fullName);
		col.setColumnName(indexVal[1]);
		col.setTable(table);

		if (t.getValByCol(col) < keyH) {
			return t;
		} else {
			return null;
		}
	}

	/**
	 * Gets the column names of this table
	 *
	 * @return column names of this table
	 */
	public List<String> getColumnNames() {
		return columnNames;
	}

}
