package logicaloperators;

import expression.PhysicalPlanBuilder;
import net.sf.jsqlparser.expression.Expression;

/**
 * represents logical select operator
 */
public class LogicalSelectOperator extends LogicalOperator {

	private LogicalOperator child;
	private Expression expr;

	/**
	 * Creates a new SelectOperator object
	 *
	 * @param child ScanOperator that is the child of the current Select
	 * @param expr  Expression to be evaluated against each tuple
	 */
	public LogicalSelectOperator(LogicalScanOperator child, Expression expr) {
		setChild(child);
		setExpr(expr);
	}

	/** Accepts a PhysicalPlanBuilder and has said builder visit this operator */
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

	/**
	 * Gets the select expression of this operator
	 * 
	 * @return Expression representing the select expression of this operator
	 */
	public Expression getExpr() {
		return expr;
	}

	/**
	 * Sets the select expression of this operator
	 * 
	 * @param expr Expression representing the new select expression of this
	 *             operator
	 */
	public void setExpr(Expression expr) {
		this.expr = expr;
	}

}
