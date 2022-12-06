package expression;

import net.sf.jsqlparser.schema.Column;
import utils.Tuple;

/**
 * SelectExprVisitor visits select expressions and contain one tuple
 */
public class SelectExprVisitor extends ExpressionVisit {

	/** The one row (i.e.tuple) currently being read */
	private Tuple row;

	/** Info about table schema */
//	private List<String> schema;

	/** Constructor that creates new SelectExprVisitor object */
	public SelectExprVisitor() {
	}

	/**
	 * Reads one tuple and sets its information to SelectExprVisitor object
	 *
	 * @param r The row (i.e.tuple) currently being read
	 */
	public void readOneTuple(Tuple r) {
		row = r;
	}

	/**
	 * set Value of column in a row to the expression visitor
	 * 
	 * @param col the column being visited
	 */
	@Override
	public void visit(Column col) {
		setValue(row.getValByCol(col));
	}
}
