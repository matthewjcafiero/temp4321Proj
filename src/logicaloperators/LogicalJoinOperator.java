package logicaloperators;

import expression.PhysicalPlanBuilder;
import net.sf.jsqlparser.expression.Expression;

/**
 * represents a logical join operator
 */
public class LogicalJoinOperator extends LogicalOperator {

	private LogicalOperator leftChild;
	private LogicalOperator rightChild;
	private Expression expr;

	/**
	 * Constructor for JoinOperator object
	 *
	 * @param leftChild  the left child operator
	 * @param rightChild the right child operator
	 * @param expr       the expression used as join condition
	 */
	public LogicalJoinOperator(LogicalOperator leftChild, LogicalOperator rightChild, Expression expr) {
		setLeftChild(leftChild);
		setRightChild(rightChild);
		setExpr(expr);
	}

	/** Accepts a PhysicalPlanBuilder and has said builder visit this operator */
	@Override
	public void accept(PhysicalPlanBuilder physical_builder) {
		physical_builder.visit(this);
	}

	/**
	 * Gets the left child of this operator
	 * 
	 * @return logical operator that is the left child
	 */
	public LogicalOperator getLeftChild() {
		return leftChild;
	}

	/**
	 * Sets the left child of this operator
	 * 
	 * @param leftChild operator that will be the leftChild of this operator
	 */
	public void setLeftChild(LogicalOperator leftChild) {
		this.leftChild = leftChild;
	}

	/**
	 * Gets the right child of this operator
	 * 
	 * @return logical operator that is the right child
	 */
	public LogicalOperator getRightChild() {
		return rightChild;
	}

	/**
	 * Sets the right child of this operator
	 * 
	 * @param rightChild operator that will be the rightChild of this operator
	 */
	public void setRightChild(LogicalOperator rightChild) {
		this.rightChild = rightChild;
	}

	/**
	 * Get the join expression of this operator
	 * 
	 * @return Expression that is the join expr of this operator
	 */
	public Expression getExpr() {
		return expr;
	}

	/**
	 * Sets the join expression of this operator
	 * 
	 * @param expr Expression that will be the join expression of this operator
	 */
	public void setExpr(Expression expr) {
		this.expr = expr;
	}

}
