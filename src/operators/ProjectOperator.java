package operators;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import utils.Tuple;

/**
 * Project operator, selects appropriate columns from child
 */
public class ProjectOperator extends Operator {

	private Operator child;
	private List<SelectExpressionItem> selectItems;
	private List<String> resultSchema = new ArrayList<>();

	/**
	 * Instantiates new project operator
	 *
	 * @param child  Operator beneath this operator
	 * @param select PlainSelect expression representing a SQL state4ment
	 */
	public ProjectOperator(Operator child, List<SelectExpressionItem> selectItems) {
		this.child = child;
		this.selectItems = selectItems;
		for (SelectExpressionItem item : selectItems) {
			Expression expr = item.getExpression();
			Column col = (Column) expr;
			resultSchema.add(col.toString());
		}
		schemas = resultSchema;
	}

	/**
	 * Gets next tuple from child, isolates the columns as desired by the SQL
	 * statement, and returns a new tuple with only the data from said columns of
	 * said tuple
	 *
	 * @return new Tuple with data from child's tuple
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple tuple = child.getNextTuple();
		if (tuple == null) {
			return null;
		}

		int size = selectItems.size();
		Long[] resultValues = new Long[size];
		int counter = 0;

		for (SelectExpressionItem item : selectItems) {
			Expression expr = item.getExpression();
			Column col = (Column) expr;
			resultValues[counter++] = tuple.getValByCol(col);
		}
		return new Tuple(resultValues, tuple.getTableName(), resultSchema);
	}

	/** Resets entire table */
	@Override
	public void reset() {
		child.reset();
	}

}
