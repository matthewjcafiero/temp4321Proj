package operators;

import utils.MyTable;
import utils.Tuple;

/**
 * Scan operator, scans table and gets tuples
 */
public class ScanOperator extends Operator {

	MyTable table;

	/**
	 * Constructor that creates new Scan Operator object
	 *
	 * @param table a MyTable object that will be scanned
	 */
	public ScanOperator(MyTable table) {
		this.table = table;
		schemas = this.table.getColumnNames();
	}

	/**
	 * get next tuple in the scan
	 *
	 * @return next tuple from table
	 */
	@Override
	public Tuple getNextTuple() {
		return table.getNextTuple();
	}

	/** resets entire table */
	@Override
	public void reset() {
		table.reset();
	}

}
