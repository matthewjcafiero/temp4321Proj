package expression;

import net.sf.jsqlparser.schema.Column;
import utils.Tuple;

/**
 * JoinExprVisitor visits a join expression and contains two tuples
 */
public class JoinExprVisitor extends ExpressionVisit {

	/** The two rows (i.e.tuples) from two tables currently being read */
	private Tuple row1;
	private Tuple row2;

	/** Constructor that creates new SelectExprVisitor object */
	public JoinExprVisitor() {
	}

	/**
	 * Reads two tuples and sets their info to JoinExprVisit object
	 *
	 * @param r1 The 1st row (i.e. left tuple) currently being read
	 * @param r2 The 2nd row (i.e. right tuple) currently being read
	 */
	public void readTwoTuples(Tuple r1, Tuple r2) {
		row1 = r1;
		row2 = r2;

	}

	/**
	 * Sets Value of column in a row to the expression visitor. When left row has
	 * NULL value, sets value from right row to visitor.
	 * 
	 * @param col the column being visited
	 */
	@Override
	public void visit(Column col) {
		Long val;
		try {
			val = row1.getValByCol(col);
			setValue(val);
		} catch (Exception e) {
			setValue(row2.getValByCol(col));
		}
	}
}
