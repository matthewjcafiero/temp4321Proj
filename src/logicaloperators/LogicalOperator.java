package logicaloperators;

import expression.PhysicalPlanBuilder;

/**
 * base class for logical operators
 */
public abstract class LogicalOperator {
	/*
	 * Handout 3.2: the logical operators do not need getNextTuple() or reset()
	 * methods since they will never actually â€œrunâ€�. That logic belongs in
	 * physical operators.
	 */

	public abstract void accept(PhysicalPlanBuilder physical_builder);
}
