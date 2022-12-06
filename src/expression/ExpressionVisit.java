package expression;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

public class ExpressionVisit implements ExpressionVisitor {

	/**
	 * prints when expression has operations other than AND, =, !=, >, >=, <, <=,
	 * AndExpression, Column, LongValue, EqualsTo, NotEqualsTo, GreaterThan,
	 * GreaterThanEquals, MinorThan and MinorThanEquals.
	 */
	private String msg = "Operation not supported";

	/**
	 * The long value in the expression when using accept(this), we will update
	 * longVal in this visitor
	 */
	private long longVal;

	/**
	 * Default: true (when select or join condition is NULL)
	 * Either the status (bool value) appearing in the expression, Or the evaluation
	 * result (bool value) of the the entire expression. When using accept(this), we
	 * will update boolVal in this visitor
	 */
	private boolean boolVal = true;

	/**
	 * sets the long value in the expression
	 * 
	 * @param longVal the value to be set in the visitor
	 */
	public void setValue(long longVal) {
		this.longVal = longVal;
	}

	/**
	 * gets the long value in the expression
	 * 
	 * @return Long value in the expression
	 */
	public long getValue() {
		return longVal;
	}

	/**
	 * gets the bool value in the expression or evaluation of expr
	 * 
	 * @return the bool value (status, or evaluation of expr)
	 */
	public boolean getStatus() {
		return boolVal;
	}

	/**
	 * sets the input long value into this expr visitor
	 * 
	 * @param val the long value to set into this visitor
	 */
	@Override
	public void visit(LongValue val) {
		setValue(val.getValue());
	}

	@Override
	public void visit(NullValue arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(Function arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(InverseExpression arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(JdbcParameter arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(DoubleValue arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(DateValue arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(TimeValue arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(TimestampValue arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(Parenthesis arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(StringValue arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(Addition arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(Division arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(Multiplication arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(Subtraction arg0) {
		System.out.print(msg);
	}

	/**
	 * Evaluates the input And expression,
	 * sets the evaluation result of (left expr && right expr)
	 * into this visitor.
	 * 
	 * @param expr the And expression to evaluate.
	 * Evaluation result will be set into this visitor.
	 */
	@Override
	public void visit(AndExpression expr) {
		boolean left;
		boolean right;
		expr.getLeftExpression().accept(this);
		left = boolVal;
		expr.getRightExpression().accept(this);
		right = boolVal;
		boolVal = left && right;
	}

	@Override
	public void visit(OrExpression arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(Between arg0) {
		System.out.print(msg);
	}

	/**
	 * Evaluates the input EqualsTo expression,
	 * sets the evaluation result of (left expr == right expr)
	 * into this visitor.
	 * 
	 * @param expr the EqualsTo expression to evaluate.
	 * Evaluation result will be set into this visitor.
	 */
	@Override
	public void visit(EqualsTo expr) {
		long left;
		long right;
		expr.getLeftExpression().accept(this);
		left = longVal;
		expr.getRightExpression().accept(this);
		right = longVal;
		boolVal = left == right;
	}

	/**
	 * Evaluates the input GreaterThan expression,
	 * sets the evaluation result of (left expr > right expr)
	 * into this visitor.
	 * 
	 * @param expr the GreaterThan expression to evaluate.
	 * Evaluation result will be set into this visitor.
	 */
	@Override
	public void visit(GreaterThan expr) {
		long left;
		long right;
		expr.getLeftExpression().accept(this);
		left = longVal;
		expr.getRightExpression().accept(this);
		right = longVal;
		boolVal = left > right;
	}

	/**
	 * Evaluates the input GreaterThanEquals expression,
	 * sets the evaluation result of (left expr >= right expr)
	 * into this visitor.
	 * 
	 * @param expr the GreaterThanEquals expression to evaluate.
	 * Evaluation result will be set into this visitor.
	 */
	@Override
	public void visit(GreaterThanEquals expr) {
		long left;
		long right;
		expr.getLeftExpression().accept(this);
		left = longVal;
		expr.getRightExpression().accept(this);
		right = longVal;
		boolVal = left >= right;
	}

	@Override
	public void visit(InExpression arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(IsNullExpression arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(LikeExpression arg0) {
		System.out.print(msg);
	}

	/**
	 * Evaluates the input MinorThan expression,
	 * sets the evaluation result of (left expr < right expr)
	 * into this visitor.
	 * 
	 * @param expr the MinorThan expression to evaluate.
	 * Evaluation result will be set into this visitor.
	 */
	@Override
	public void visit(MinorThan expr) {
		long left;
		long right;
		expr.getLeftExpression().accept(this);
		left = longVal;
		expr.getRightExpression().accept(this);
		right = longVal;
		boolVal = left < right;
	}

	/**
	 * Evaluates the input MinorThanEquals expression,
	 * sets the evaluation result of (left expr <= right expr)
	 * into this visitor.
	 * 
	 * @param expr the MinorThanEquals expression to evaluate.
	 * Evaluation result will be set into this visitor.
	 */
	@Override
	public void visit(MinorThanEquals expr) {
		long left;
		long right;
		expr.getLeftExpression().accept(this);
		left = longVal;
		expr.getRightExpression().accept(this);
		right = longVal;
		boolVal = left <= right;
	}

	/**
	 * Evaluates the input NotEqualsTo expression,
	 * sets the evaluation result of (left expr != right expr)
	 * into this visitor.
	 * 
	 * @param expr the NotEqualsTo expression to evaluate.
	 * Evaluation result will be set into this visitor.
	 */
	@Override
	public void visit(NotEqualsTo expr) {
		long left;
		long right;
		expr.getLeftExpression().accept(this);
		left = longVal;
		expr.getRightExpression().accept(this);
		right = longVal;
		boolVal = left != right;
	}

	/**
	 * Implemented in SelectExprVisitor.java
	 */
	@Override
	public void visit(Column arg0) {
	}

	@Override
	public void visit(SubSelect arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(CaseExpression arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(WhenClause arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(ExistsExpression arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(Concat arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(Matches arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(BitwiseAnd arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(BitwiseOr arg0) {
		System.out.print(msg);
	}

	@Override
	public void visit(BitwiseXor arg0) {
		System.out.print(msg);
	}

}
