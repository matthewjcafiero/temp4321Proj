package logicaloperators;

import expression.PhysicalPlanBuilder;
import utils.MyTable;

/**
 * represents logical scan operator
 */
public class LogicalScanOperator extends LogicalOperator {

	private MyTable table;

	/**
	 * Constructor that creates new Logical Scan Operator object
	 *
	 * @param table a MyTable object that will be scanned
	 */
	public LogicalScanOperator(MyTable table) {
		setTable(table);
	}

	/** Accepts a PhysicalPlanBuilder and has said builder visit this operator */
	@Override
	public void accept(PhysicalPlanBuilder physical_builder) {
		physical_builder.visit(this);
	}

	/**
	 * Gets the table associated with this operator
	 * 
	 * @return MyTable representing the table of this operator
	 */
	public MyTable getTable() {
		return table;
	}

	/**
	 * Sets the table associated with this operator
	 * 
	 * @param table MyTable to be the new table of this operator
	 */
	public void setTable(MyTable table) {
		this.table = table;
	}

}
