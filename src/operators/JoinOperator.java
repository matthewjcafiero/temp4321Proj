package operators;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import utils.Tuple;

/**
 * Base class for all physical join operators
 */
public abstract class JoinOperator extends Operator {

	Operator leftChild;
	Operator rightChild;
	Expression expr;

	/**
	 * Constructor for JoinOperator object
	 *
	 * @param leftChild  the left child operator
	 * @param rightChild the right child operator
	 * @param expr       the expression used as join condition
	 */
	public JoinOperator(Operator leftChild, Operator rightChild, Expression expr) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.expr = expr;
		ArrayList<String> newSchema = new ArrayList<>();
		newSchema.addAll(leftChild.schemas);
		newSchema.addAll(rightChild.schemas);
		schemas = newSchema;
	}

	/**
	 * Get the next tuple that meets the join condition
	 *
	 * @return the next joined tuple
	 */
	@Override
	public abstract Tuple getNextTuple();

	/** resets both child operators */
	@Override
	public void reset() {
		leftChild.reset();
		rightChild.reset();
	}

	/**
	 * Join two tuples that meet the join condition
	 *
	 * @param tuple1 the first tuple to be joined
	 * @param tuple2 the second tuple to be joined
	 *
	 * @return the joined tuple
	 */
	protected Tuple joinTuples(Tuple tuple1, Tuple tuple2) {
		List<Long> newValues = new ArrayList<>();
		newValues.addAll(tuple1.getAll());
		newValues.addAll(tuple2.getAll());
		Long[] valuesArr = new Long[newValues.size()];
		valuesArr = newValues.toArray(valuesArr);
		List<String> newSchemas = new ArrayList<>();
		newSchemas.addAll(tuple1.getSchemaNames());
		newSchemas.addAll(tuple2.getSchemaNames());
		String newTableName = tuple1.getTableName() + "+" + tuple2.getTableName();
		return new Tuple(valuesArr, newTableName, newSchemas);
	}

}
