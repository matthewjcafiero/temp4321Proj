package operators;

import expression.SelectExprVisitor;
import net.sf.jsqlparser.expression.Expression;
import utils.Tuple;

/**
 * Select operator, select tuples that satisfies expr
 */
public class SelectOperator extends Operator {

	private ScanOperator child;
	private Expression expr;

	/**
	 * Creates a new SelectOperator object
	 *
	 * @param child ScanOperator that is the child of the current Select
	 * @param expr  Expression to be evaluated against each tuple
	 */
	public SelectOperator(ScanOperator child, Expression expr) {
		schemas = child.schemas;
		this.child = child;
		this.expr = expr;
	}

	/**
	 * Get next tuple from child and checks if it satisfies expression.
	 *
	 * @return the next tuple that meets the expression, null if there are none left
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple currTuple;
		while ((currTuple = child.getNextTuple()) != null) {
			if (expr == null)
				return currTuple;
			SelectExprVisitor selectExps = new SelectExprVisitor();
			selectExps.readOneTuple(currTuple);
			expr.accept(selectExps);
			if (selectExps.getStatus()) {
				return currTuple;
			}
		}
		return null;
	}

	/** resets child operator */
	@Override
	public void reset() {
		child.reset();
	}

}
