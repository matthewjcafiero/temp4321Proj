package utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.schema.Column;

/**
 * Tuple represents one tuple
 */
public class Tuple {

	private ArrayList<Long> values = new ArrayList<>();
	// Note: the column names are in the form TableName.Column name (except when
	// joins, where the
	// TableName is retained as the joins are put together)
	private HashMap<String, Integer> schemaIndexes = new HashMap<>();
	private List<String> schemaList = new LinkedList<>();
	private String tableName;

	/**
	 * Initializes a new tuple object with a predetermined set of values
	 *
	 * @param values    array of long values to be added to the array
	 * @param tableName name of the table this tuple is in
	 * @param schema    list representing the name of each column in this tuple
	 */
	public Tuple(Long[] values, String tableName, List<String> schema) {
		this.tableName = tableName;
		for (int i = 0; i < values.length; i++) {
			this.values.add(values[i]);
			String col = schema.get(i);
			schemaIndexes.put(col, i);
		}
		setSchemaList();
	}

	/**
	 * Initializes a new tuple object with a predetermined set of values
	 *
	 * @param tupleId   unique id for the tuple
	 * @param values    string array of values (which are ints) to be added to the
	 *                  array
	 * @param tableName name of the table this tuple is in
	 * @param schema    list representing the name of each column in this tuple
	 */
	public Tuple(String[] values, String tableName, List<String> schema) {
		this.tableName = tableName;
		for (int i = 0; i < values.length; i++) {
			this.values.add(Long.valueOf(values[i]));
			schemaIndexes.put(tableName + "." + schema.get(i), i);
		}
		setSchemaList();
	}

	public Tuple(String[] values, String tableName, HashMap<String, Integer> schemaIndexes) {
		this.tableName = tableName;
		for (int i = 0; i < values.length; i++) {
			this.values.add(Long.valueOf(values[i]));
		}
		this.schemaIndexes = schemaIndexes;
		setSchemaList();
	}

	/** Sets the schemaList variable based on schemaIndexes */
	private void setSchemaList() {
		String[] resultArr = new String[schemaIndexes.size()];
		Set<String> keys = schemaIndexes.keySet();
		for (String key : keys) {
			resultArr[schemaIndexes.get(key)] = key;
		}
		schemaList = Arrays.asList(resultArr);
	}

	/**
	 * String representation of this tuple's data
	 *
	 * @returns String of tuple data
	 */
	@Override
	public String toString() {
		return "table name: " + tableName + ", values: " + values.toString() + ", schema: " + getSchema();
	}

	/**
	 * Prints out the values of this tuple in format "x,y,z"
	 *
	 * @returns String of values
	 */
	public String print() {
		String result = "";
		for (Long value : values) {
			result += value + ",";
		}
		if (result.length() != 0) {
			return result.substring(0, result.length() - 1);
		}
		return "";
	}

	/**
	 * Gets a map representation of the schema of this tuple
	 *
	 * @return HashMap representation of the indexes of columns of this tuple
	 */
	public HashMap<String, Integer> getSchema() {
		return schemaIndexes;
	}

	/**
	 * Get a list of the columns of this tuple in order from 0..end
	 *
	 * @return the list of the schema in the tuple
	 */
	public List<String> getSchemaNames() {
		return schemaList;
	}

	/**
	 * Returns the long value at a specific column of the tuple
	 *
	 * @param col The column of a tuple
	 *
	 * @return long value at column
	 */
	public Long getValByCol(Column col) {
		String fullColName = col.toString();
		int index = schemaIndexes.get(fullColName);
		return values.get(index);
	}

	/**
	 * Returns all values in the tuple
	 *
	 * @return ArrayList of all values
	 */
	public ArrayList<Long> getAll() {
		return values;
	}

	/**
	 * Returns name of the table this tuple is in
	 *
	 * @return String of tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/** @return Resets this tuple */
	public void reset() {
	}

	/**
	 * Override equals to check whether two tuples have the same values
	 *
	 * @param the tuple to compare with
	 * @return true if equals, false otherwise
	 */
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Tuple))
			return false;
		Tuple tuple = (Tuple) obj;
		return tuple.values.equals(values);
	}

}
