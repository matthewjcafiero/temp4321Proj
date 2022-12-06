package logicaloperators;

import expression.PhysicalPlanBuilder;

/**
 * represents a Logical Distinct Operator
 */
public class LogicalDistinctOperator extends LogicalOperator {

	private LogicalOperator child;

	/**
	 * Create new DistinctOperator object from a child operator
	 *
	 * @param op the child operator
	 */
	public LogicalDistinctOperator(LogicalOperator op) {
		setChild(op);
	}

	/** Accepts a PhysicalPlanBuilder and visits this operator */
	@Override
	public void accept(PhysicalPlanBuilder physical_builder) {
		physical_builder.visit(this);
	}

	/**
	 * Gets the child of this operator
	 * 
	 * @return child of this operator
	 */
	public LogicalOperator getChild() {
		return child;
	}

	/**
	 * Sets the child of this operator
	 * 
	 * @param child logicalOperator that will be the new child
	 */
	public void setChild(LogicalOperator child) {
		this.child = child;
	}
}
