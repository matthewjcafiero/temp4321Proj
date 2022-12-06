package operators;

import utils.Tuple;

/**
 * Distinct Operator, gets distinct tuple from child
 */
public class DistinctOperator extends Operator {

	Operator child;
	Tuple last;

	/**
	 * Create new DistinctOperator object from a child operator
	 *
	 * @param op the child operator
	 */
	public DistinctOperator(Operator op) {
		if (op instanceof SortOperator) {
			child = op;
		} else {
			SortOperator sortOp = new InternalSortOperator(op, null);
			child = sortOp;
		}
		schemas = op.schemas;
	}

	/** get next tuple that is different than last */
	@Override
	public Tuple getNextTuple() {
		Tuple t = child.getNextTuple();
		while (t != null && t.equals(last)) {
			t = child.getNextTuple();
		}
		last = t;
		return t;
	}

	/** reset child operator */
	@Override
	public void reset() {
		child.reset();
	}

}
