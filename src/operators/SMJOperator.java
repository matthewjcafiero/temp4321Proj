package operators;

import java.util.ArrayList;
import java.util.List;

import expression.JoinExprVisitor;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import utils.Tuple;

/**
 * SMJOperator, joins two table items using Sort Merge Join
 */
public class SMJOperator extends JoinOperator {

	Tuple leftTuple;
	Tuple rightTuple;

	// schema for left and right tables
	List<String> leftSchemas;
	List<String> rightSchemas;

	// left and right columns, according to which we compare tuples' values
	List<Column> leftOrderKeys = new ArrayList<>();
	List<Column> rightOrderKeys = new ArrayList<>();

	// object of comparing two tuples according to orderby columns
	SMJTupleComparator tupleCmpr;

	/*
	 * rightResetId used to reset right table partition scan (i.e. reset
	 * rightTuple). rightTuple can be reset according to the rightResetId when done
	 * with current left table partition (i.e. done with current leftTuple), or when
	 * reached end of right table.
	 */
	public int rightResetId;

	/**
	 * Constructor for a new SMJOperator object
	 *
	 * @param leftChild     left child operator
	 * @param rightChild    right child operator
	 * @param expr          join expression to satisfy
	 * @param leftOrderBys  list of left OrderByElement, will be converted to left
	 *                      cols involved in join condition(s)
	 * @param rightOrderBys list of right OrderByElement, will be converted to right
	 *                      cols involved in join condition(s)
	 */
	public SMJOperator(Operator leftChild, Operator rightChild, Expression expr, List<Column> leftOrderBys,
			List<Column> rightOrderBys) {
		super(leftChild, rightChild, expr);

		leftTuple = this.leftChild.getNextTuple();
		rightTuple = this.rightChild.getNextTuple();

		leftSchemas = leftChild.schemas;
		rightSchemas = rightChild.schemas;

		leftOrderKeys = leftOrderBys;
		rightOrderKeys = rightOrderBys;

		tupleCmpr = new SMJTupleComparator(leftOrderKeys, rightOrderKeys);
	}

	/**
	 * gets next tuple that satisfies the join expression condition.
	 *
	 * @return the tuple being read from the table
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple joinedTuple = null;
		JoinExprVisitor joinVisitor = new JoinExprVisitor();

		while (leftTuple != null && rightTuple != null) {
			// leftTuple < rightTuple, go to next left tuple
			if (tupleCmpr.compare(leftTuple, rightTuple) < 0) {
				leftTuple = leftChild.getNextTuple(); // continue scan of left table
				continue; // check whether end of left table, using while condition
			}

			// leftTuple > rightTuple, go to next right tuple, update rightResetId
			if (tupleCmpr.compare(leftTuple, rightTuple) > 0) {
				rightTuple = rightChild.getNextTuple();
				rightResetId++; // continue scan of right table
				continue; // check whether end of right table, using while condition
			}

			if (expr == null) { // no join condition
				joinedTuple = joinTuples(leftTuple, rightTuple);
			}

			// leftTuple == rightTuple
			else {
				joinVisitor.readTwoTuples(leftTuple, rightTuple);
				expr.accept(joinVisitor);
				if (joinVisitor.getStatus()) {
					joinedTuple = joinTuples(leftTuple, rightTuple);// output joined tuples
				}
			}

			// update rightTuple,
			// rightResetId not changed.
			rightTuple = rightChild.getNextTuple();

			// When done with current left table partition
			// or reached the end of right table,
			// go to next left tuple,
			// & go to the rightTuple according to rightResetId
			if (rightTuple == null || tupleCmpr.compare(leftTuple, rightTuple) != 0) {
				leftTuple = leftChild.getNextTuple();
				if (rightChild instanceof InternalSortOperator) {
					((InternalSortOperator) rightChild).reset(rightResetId);
				} else {
					((ExternalSortOperator) rightChild).reset(rightResetId);
				}

				rightTuple = rightChild.getNextTuple();
			}

			if (joinedTuple != null) {
				return joinedTuple;
			}
		}
		return null;
	}

	/**
	 * Resets left & right tables; resets current tuple being read from two tables;
	 * resets rightResetId to 0.
	 */
	@Override
	public void reset() {
		leftChild.reset();
		rightChild.reset();
		leftTuple = leftChild.getNextTuple();
		rightTuple = rightChild.getNextTuple();
		rightResetId = 0;
	}
}
