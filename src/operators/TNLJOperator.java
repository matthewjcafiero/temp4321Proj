package operators;

import expression.JoinExprVisitor;
import net.sf.jsqlparser.expression.Expression;
import utils.Tuple;

/**
 * TNLJOperator, joins items from two tables using tuple nested left join
 */
public class TNLJOperator extends JoinOperator {
	Tuple tuple1;
	Tuple tuple2;

	/**
	 * Constructor for TNLJOperator object
	 *
	 * @param leftChild  the left child operator
	 * @param rightChild the right child operator
	 * @param expr       the expression used as join condition
	 */
	public TNLJOperator(Operator leftChild, Operator rightChild, Expression expr) {
		super(leftChild, rightChild, expr);
		tuple1 = this.leftChild.getNextTuple();
		tuple2 = this.rightChild.getNextTuple();
	}

	/**
	 * loop to find the next joined tuple that meets the join condition and return
	 * it
	 *
	 * @return the next joined tuple that meets join condition, null if none
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple joinedTuple = null;
		JoinExprVisitor joinVisitor = new JoinExprVisitor();
		while (tuple1 != null && tuple2 != null) {
			if (expr == null) {
				joinedTuple = joinTuples(tuple1, tuple2);
			} else {
				joinVisitor.readTwoTuples(tuple1, tuple2);
				expr.accept(joinVisitor);
				if (joinVisitor.getStatus()) {
					joinedTuple = joinTuples(tuple1, tuple2);
				}
			}
			if (tuple1 == null) {
				break;
			}
			Tuple temp = rightChild.getNextTuple();
			if (temp != null) {
				tuple2 = temp;
			} else {
				tuple1 = leftChild.getNextTuple();
				rightChild.reset();
				tuple2 = rightChild.getNextTuple();
			}
			if (joinedTuple != null) {
				return joinedTuple;
			}
		}
		return null;
	}

	/**
	 * reset operator and child operators to initial state
	 */
	@Override
	public void reset() {
		leftChild.reset();
		rightChild.reset();
		tuple1 = leftChild.getNextTuple();
		tuple2 = rightChild.getNextTuple();
	}

}
