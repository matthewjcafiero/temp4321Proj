package expression;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;

/*
 * join map takes in 2 table references, or a table and a list of tables
 * "already used in previous joins" (ie. tables that are now a single table
 * called like Sailors+Boats for example), and returns the join expression
 * that involves only said tables.
 *
 * it allows us to handle edge cases well when we have 3 or more joins,
 * as the changing table names as each join applies means
 * we need to handle changing references (for example if we have
 * a table Sailors+Boats and want to join on reserves,
 * a join condition between reverses and boats and
 * a join condition between sailors and reserves need to be put together,
 * as both reference the same 2 tables; Sailors+Boats and Reserves)
 *
 * Also note that if you wanna use a joinmap
 * use the 2 safeget methods written in query builder
 * They handle allowing us to look for Sailors, Boats and Boats, Sailors
 * and get the same value from 1 key either way,
 * rather than having to double each key
 * (I may go back and change that later tbh, but for now its fine)
 */

/** WhereExprVisitor visits where expressions and extracts joins and selects */
public class WhereExprVisitor extends ExpressionVisit {

	private List<Expression> selectExprs = new LinkedList<>();
	private List<Expression> joinExprs = new LinkedList<>();
	private HashMap<String, Expression> selectMap = new HashMap<>();
	// Join map holds all relations between tables that are joined together. The
	// keys for this map
	// are denoted by the concatenation of the table names, where order of names
	// does not matter (we
	// use safe get methods to handle this functionality)
	private HashMap<String, Expression> joinMap = new HashMap<>();

	/** Constructor to instantiate a new WhereExprVisitorObject */
	public WhereExprVisitor() {
	}

	// ***** LIST TO MAP METHODS *****

	/**
	 * Gets the left side of an expression depending on its type
	 *
	 * @param expr Expression
	 * @return Expression that is on the left side of expr
	 */
	Expression getLeftSide(Expression expr) {
		if (expr instanceof BinaryExpression) {
			return ((BinaryExpression) expr).getLeftExpression();
		}
		return null;
	}

	/**
	 * Gets the right side of an expression depending on its type
	 *
	 * @param expr Expression
	 * @return Expression that is on the right side of expr
	 */
	Expression getRightSide(Expression expr) {
		if (expr instanceof BinaryExpression) {
			return ((BinaryExpression) expr).getRightExpression();
		}
		return null;
	}

	/**
	 * Checks if a given string can be parsed into an int
	 *
	 * @param str String to be checked
	 * @return true if the string can be parsed into an int, false otherwise
	 */
	private boolean isInt(String str) {
		try {
			Integer.parseInt(str);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * Returns the name of a table given a string representing a column name within
	 * a table
	 *
	 * @param str string in the following format "TableName.ColumnName"
	 * @return TableName of a column
	 */
	private String getTableName(String str) {
		String result = "";
		for (int i = 0; i < str.length(); i++) {
			char c = str.charAt(i);
			if (c == '.') {
				return result;
			}
			result += c;
		}
		return result;
	}

	/**
	 * If selectMap is empty, convert selectExprs to a HashMap<Associated TableName,
	 * Expr> and store it in this object
	 *
	 * @return a non-empty selectMap (given joinsExpr is not empty)
	 */
	public HashMap<String, Expression> getSelectMap() {
		if (selectMap.size() == 0) {
			for (int i = 0; i < selectExprs.size(); i++) {
				Expression expr = selectExprs.get(i);
				String right = getRightSide(expr).toString();
				String left = getLeftSide(expr).toString();
				String rightTableName = getTableName(right);
				String leftTableName = getTableName(left);
				if (!isInt(right) && selectMap.get(rightTableName) == null) {
					selectMap.put(rightTableName, expr);
				} else if (!isInt(right)) {
					Expression temp = selectMap.get(rightTableName);
					selectMap.put(rightTableName, new AndExpression(temp, expr));
				}
				if (!isInt(left) && selectMap.get(leftTableName) == null) {
					selectMap.put(leftTableName, expr);
				} else if (!isInt(left)) {
					Expression temp = selectMap.get(leftTableName);
					selectMap.put(leftTableName, new AndExpression(temp, expr));
				}

			}
		}
		return selectMap;
	}

	/**
	 * If joinMap is empty, convert joinExprs to a
	 * HashMap<leftTableName+rightTableName, Expr> and store it in this object,
	 * where leftTableName and rightTableName are the associated table names of each
	 * entry in the joinExprs
	 *
	 * @return a non-empty joinMap (given joinsExpr is not empty)
	 */
	public HashMap<String, Expression> getJoinMap() {
		if (joinMap.size() == 0) {
			for (int i = 0; i < joinExprs.size(); i++) {
				Expression expr = joinExprs.get(i);
				String right = getRightSide(expr).toString();
				String left = getLeftSide(expr).toString();
				String rightTableName = getTableName(right);
				String leftTableName = getTableName(left);
				if (joinMap.get(rightTableName + leftTableName) == null
						&& joinMap.get(leftTableName + rightTableName) == null) {
					joinMap.put(leftTableName + rightTableName, expr);
				} else if (joinMap.get(rightTableName + leftTableName) != null) {
					Expression temp = joinMap.get(rightTableName + leftTableName);
					joinMap.put(rightTableName + leftTableName, new AndExpression(temp, expr));
				} else {
					Expression temp = joinMap.get(leftTableName + rightTableName);
					joinMap.put(leftTableName + rightTableName, new AndExpression(temp, expr));
				}
			}
		}
		return joinMap;
	}

	// ***** HELPER METHODS FOR VISIT METHODS *****

	/**
	 * Helper method for checkJoin() that checks if a left and right expression
	 * contain an int, where either one of them containing an int implies the
	 * relationship between the expressions is not a join.
	 *
	 * @param left  left expression to be checked
	 * @param right right expresson to be checked
	 * @return true if neither expression is an parsable to an int, false otherwise
	 *         ie. true if left and right are a join and false otherwise
	 */
	private boolean checkJoinHelper(Expression left, Expression right) {
		try {
			// Checks if the left side is an integer
			Integer.parseInt(left.toString());
			return false;
		} catch (Exception e) {
		}
		try {
			// Checks if the right side is an integer
			Integer.parseInt(right.toString());
			return false;
		} catch (Exception e) {
		}
		return true;
	}

	/**
	 * Checks if a given expression is related to a join operation by checking if
	 * either side of the expression is an integer, as we assume if one side is an
	 * integer than it cannot be a join.
	 *
	 * @param expr Expression to be checked
	 * @return true if the expression is related to a join operation, false if
	 *         otherwise
	 */
	private boolean checkJoin(Expression expr) {
		if (expr.getClass() == EqualsTo.class) {
			EqualsTo temp = (EqualsTo) expr;
			return checkJoinHelper(temp.getLeftExpression(), temp.getRightExpression());
		} else if (expr.getClass() == NotEqualsTo.class) {
			NotEqualsTo temp = (NotEqualsTo) expr;
			return checkJoinHelper(temp.getLeftExpression(), temp.getRightExpression());
		} else if (expr.getClass() == MinorThan.class) {
			MinorThan temp = (MinorThan) expr;
			return checkJoinHelper(temp.getLeftExpression(), temp.getRightExpression());
		} else if (expr.getClass() == MinorThanEquals.class) {
			MinorThanEquals temp = (MinorThanEquals) expr;
			return checkJoinHelper(temp.getLeftExpression(), temp.getRightExpression());
		} else if (expr.getClass() == GreaterThan.class) {
			GreaterThan temp = (GreaterThan) expr;
			return checkJoinHelper(temp.getLeftExpression(), temp.getRightExpression());
		} else if (expr.getClass() == GreaterThanEquals.class) {
			GreaterThanEquals temp = (GreaterThanEquals) expr;
			return checkJoinHelper(temp.getLeftExpression(), temp.getRightExpression());
		} else {
			return false;
		}
	}

	// ***** VISIT METHODS *****

	/**
	 * Visits an expression and processes it as either a join or a select expression
	 *
	 * @param AndExpression to visit
	 */
	@Override
	public void visit(AndExpression expr) {
		Expression left = expr.getLeftExpression();
		Expression right = expr.getRightExpression();

		if (checkJoin(right)) {
			joinExprs.add(0, right);
		} else {
			selectExprs.add(0, right);
		}
		if (left instanceof AndExpression) {
			left.accept(this);
		} else if (checkJoin(left)) {
			joinExprs.add(0, left);
		} else {
			selectExprs.add(0, left);
		}
	}

	/**
	 * Visits an expression and processes it as either a join or a select expression
	 *
	 * @param expr expression to visit
	 */
	@Override
	public void visit(EqualsTo expr) {
		if (checkJoin(expr)) {
			joinExprs.add(0, expr);
		} else {
			selectExprs.add(0, expr);
		}
	}

	/**
	 * Visits an expression and processes it as either a join or a select expression
	 *
	 * @param expr expression to visit
	 */
	@Override
	public void visit(NotEqualsTo expr) {
		if (checkJoin(expr)) {
			joinExprs.add(0, expr);
		} else {
			selectExprs.add(0, expr);
		}
	}

	/**
	 * Visits an expression and processes it as either a join or a select expression
	 *
	 * @param expr expression to visit
	 */
	@Override
	public void visit(MinorThan expr) {
		if (checkJoin(expr)) {
			joinExprs.add(0, expr);
		} else {
			selectExprs.add(0, expr);
		}
	}

	/**
	 * Visits an expression and processes it as either a join or a select expression
	 *
	 * @param expr expression to visit
	 */
	@Override
	public void visit(MinorThanEquals expr) {
		if (checkJoin(expr)) {
			joinExprs.add(0, expr);
		} else {
			selectExprs.add(0, expr);
		}
	}

	/**
	 * Visits an expression and processes it as either a join or a select expression
	 *
	 * @param expr expression to visit
	 */
	@Override
	public void visit(GreaterThan expr) {
		if (checkJoin(expr)) {
			joinExprs.add(0, expr);
		} else {
			selectExprs.add(0, expr);
		}
	}

	/**
	 * Visits an expression and processes it as either a join or a select expression
	 *
	 * @param expr expression to visit
	 */
	@Override
	public void visit(GreaterThanEquals expr) {
		if (checkJoin(expr)) {
			joinExprs.add(0, expr);
		} else {
			selectExprs.add(0, expr);
		}
	}

	/**
	 * Visits a where expression where the class of said expression is unknown, used
	 * as a wrapper for all other visits based on the class type of the whereExpr
	 *
	 * @param whereExpr expression to visit
	 */
	public void visit(Expression whereEpr) {
		if (whereEpr != null) {
			Class<? extends Expression> check = whereEpr.getClass();
			if (check == AndExpression.class) {
				visit((AndExpression) whereEpr);
			} else if (check == EqualsTo.class) {
				visit((EqualsTo) whereEpr);
			} else if (check == NotEqualsTo.class) {
				visit((NotEqualsTo) whereEpr);
			} else if (check == MinorThan.class) {
				visit((MinorThan) whereEpr);
			} else if (check == MinorThanEquals.class) {
				visit((MinorThanEquals) whereEpr);
			} else if (check == GreaterThan.class) {
				visit((GreaterThan) whereEpr);
			} else if (check == GreaterThanEquals.class) {
				visit((GreaterThanEquals) whereEpr);
			}
		}
	}

	public List<Expression> getJoinExprs() {
		return joinExprs;
	}

	/**
	 * Returns select expressions not covered by an index, null if there are none
	 *
	 * @param colName the column to index on
	 * @return an expression representing all select expressions not covered by an
	 *         index
	 */
	public Expression getNonIndexSelectExpr(String colName) {
		List<Expression> temp = new LinkedList<>();
		for (Expression expr : selectExprs) {
			String right = getRightSide(expr).toString();
			String left = getLeftSide(expr).toString();
			if (expr.getClass() == NotEqualsTo.class && right != colName && left != colName) {
				temp.add(expr);
			}
		}
		if (temp.size() == 0) {
			return null;
		}
		Expression result = temp.get(0);
		for (Expression expr : temp) {
			result = new AndExpression(result, expr);
		}
		return result;
	}

	/**
	 *
	 * Gets lower and upper bound for a column in an expression
	 *
	 * @param colName the column to index on
	 * @return Integer array in the form of [lowerbound, upperbound]
	 */
	public Integer[] getIndexBounds(String colName) {
		Integer[] result = new Integer[2];
		for (Expression expr : selectExprs) {
			String right = getRightSide(expr).toString();
			String left = getLeftSide(expr).toString();
			String tempColName;
			int value;
			try {
				value = Integer.parseInt(right);
				tempColName = left;
				if (tempColName.contains(".")) {
					tempColName = tempColName.split("\\.")[1];
				}
			} catch (Exception e) {
				value = Integer.parseInt(left);
				tempColName = right;
			}

			if (expr.getClass() != NotEqualsTo.class && tempColName.equals(colName)) {
				Class<? extends Expression> check = expr.getClass();
				if (check == EqualsTo.class) {
					// in this case, no other expr matters as it is equals
					result[0] = value;
					result[1] = value;
					return result;
				} else if (check == MinorThan.class) {
					if (result[1] == null || value > result[1]) {
						result[1] = value - 1;
					}
				} else if (check == MinorThanEquals.class) {
					if (result[1] == null || value > result[1]) {
						result[1] = value;
					}
				} else if (check == GreaterThan.class) {
					if (result[0] == null || value < result[0]) {
						result[0] = value + 1;
					}
				} else if (check == GreaterThanEquals.class) {
					if (result[0] == null || value < result[0]) {
						result[0] = value;
					}
				}
			}

		}
		return result;
	}

}
