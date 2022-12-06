package entry;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import expression.WhereExprVisitor;
import logicaloperators.LogicalDistinctOperator;
import logicaloperators.LogicalJoinOperator;
import logicaloperators.LogicalOperator;
import logicaloperators.LogicalProjectOperator;
import logicaloperators.LogicalScanOperator;
import logicaloperators.LogicalSelectOperator;
import logicaloperators.LogicalSortOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import utils.Catalog;
import utils.MyTable;

/**
 * QueryBuilder is used for parsing SQL statements, picking out expressions, and
 * building a query plan
 *
 */
public class QueryBuilder {

	// ***** HELPER METHODS *****

	/**
	 * Gets the an alias from a FromItem if it exists, else gets the name of that
	 * table (meaning there is no alias applied)
	 *
	 * @param item FromItem expression
	 * @return alias of item if alias exists, otherwise the name of item
	 */
	private static String getAliasFromFromItem(FromItem item) {
		if (item.getAlias() != null) {
			return item.getAlias();
		} else {
			return item.toString();
		}
	}

	/**
	 * Wrapper for getAliasFromFromItem for Joins (which we can treat as FromItems)
	 *
	 * @param join Join expression
	 * @return alias of item if alias exists, otherwise the name of item
	 */
	private static String getAliasFromFromItem(Join join) {
		return getAliasFromFromItem(join.getRightItem());
	}

	/**
	 * Safe implementation of getting values from the join map. Used so that get(AB)
	 * is equivalent to get (BA), as in the joinMap either AB or BA refer to the
	 * same join condition
	 *
	 * @param joinMap    map of joins where keys are the tables names put together
	 *                   (if tables are A and B, the key is either AB or BA, however
	 *                   it does not matter which it actually is)
	 * @param table0Name name of one table in the join expression
	 * @param table1Name name of the other table in the join expression
	 * @return expression for joining the 2 tables
	 */
	private static Expression safeGetJoin(HashMap<String, Expression> joinMap, String table0Name, String table1Name) {
		if (joinMap.get(table0Name + table1Name) != null) {
			return joinMap.get(table0Name + table1Name);
		} else if (joinMap.get(table1Name + table0Name) != null) {
			return joinMap.get(table1Name + table0Name);
		} else {
			return null;
		}
	}

	/**
	 * Safe implementation of getting values from the join map. Used to correctly
	 * assign join conditions from the join map to nested joins (ie. joins onto
	 * other joined tables).
	 *
	 * @param joinMap    map of joins where keys are the tables names put together
	 * @param tableName  name of table involved in the join condition being searched
	 *                   for
	 * @param prevTables list of all previous tables that have been used in joins
	 *                   deeper in the join tree
	 * @return expression for joining a table with the previously joined tables
	 */
	private static Expression safeGetJoin(HashMap<String, Expression> joinMap, String tableName,
			List<String> prevTables) {
		List<Expression> resultList = new LinkedList<>();
		for (String entry : prevTables) {
			if (joinMap.get(tableName + entry) != null) {
				resultList.add(joinMap.get(tableName + entry));
			} else if (joinMap.get(entry + tableName) != null) {
				resultList.add(joinMap.get(entry + tableName));
			}
		}
		Expression result = null;
		for (Expression expr : resultList) {
			if (result == null) {
				result = expr;
			} else {
				result = new AndExpression(result, expr);
			}
		}
		return result;
	}

	/**
	 * Builds a tree of operators with a join as a root. Ensures that any select or
	 * scan operations are done before joins in order to increase efficiency of join
	 * operations
	 *
	 * @param select PlainSelect expression representing a SQL statement
	 * @return LogicalOperator representing a tree with a LogicalJoinOperator at its
	 *         roots, with all select and scan operations further down in the tree
	 */
	@SuppressWarnings("unchecked")
	private static LogicalOperator buildJoinTree(PlainSelect select) {

		// This section visits the WHERE clause of the select expression, and breaks it
		// down into a
		// map of joins and a map of selects where we can search for them
		WhereExprVisitor visitor = new WhereExprVisitor();
		visitor.visit(select.getWhere());

		// Sets up list of things to use
		List<Join> joinsList = select.getJoins();
		HashMap<String, Expression> selectMap = visitor.getSelectMap();
		HashMap<String, Expression> joinMap = visitor.getJoinMap();

		// Names of the tables we will use initially, note we can assume at least 2
		// tables exist in
		// this select statement
		String table0 = getAliasFromFromItem(select.getFromItem());
		String table1 = getAliasFromFromItem(joinsList.get(0));
		Expression joinExpr = safeGetJoin(joinMap, table0, table1);

		// Builds a tree with the 2 given tables
		MyTable leftTable = new MyTable(select.getFromItem());
		LogicalOperator leftOp = new LogicalSelectOperator(new LogicalScanOperator(leftTable), selectMap.get(table0));
		MyTable rightTable = Catalog.getMyTable(joinsList.get(0));
		LogicalOperator rightOp = new LogicalSelectOperator(new LogicalScanOperator(rightTable), selectMap.get(table1));
		LogicalJoinOperator topJoin = new LogicalJoinOperator(leftOp, rightOp, joinExpr);

		// This saves the previously used tables
		List<String> prevUsedTables = new LinkedList<>();
		prevUsedTables.add(table0);
		prevUsedTables.add(table1);

		// Takes that tree, and builds it into a tree of n other join connections in the
		// same way
		for (int i = 1; i < joinsList.size(); i++) {
			Expression selectExpr = selectMap.get(getAliasFromFromItem(joinsList.get(i)));
			MyTable newTable = Catalog.getMyTable(joinsList.get(i));
			LogicalOperator newOp = new LogicalSelectOperator(new LogicalScanOperator(newTable), selectExpr);
			Expression newExpr = joinMap.get(getAliasFromFromItem(joinsList.get(i)));
			String tableN = getAliasFromFromItem(joinsList.get(i));
			newExpr = safeGetJoin(joinMap, tableN, prevUsedTables);
			prevUsedTables.add(tableN);
			topJoin = new LogicalJoinOperator(topJoin, newOp, newExpr);
		}
		return topJoin;
	}

	// ***** DECISION TREE METHODS *****

	/**
	 * Decides whether a ScanOperator or a SelectOperator with a ScanOperator child
	 * is needed based on the statement made
	 *
	 * @param select PlainSelect expression representing the SQL statement
	 * @return LogicalOperator representing a query tree with either a ScanOperator
	 *         or a SelectOperator at the root of the tree
	 */
	private static LogicalOperator hasSelect(PlainSelect select) {
		MyTable table = new MyTable(select.getFromItem());
		Expression expr = select.getWhere();
		if (expr != null) {
			return new LogicalSelectOperator(new LogicalScanOperator(table), expr);
		} else {
			return new LogicalScanOperator(table);
		}
	}

	/**
	 * Checks if the given SQL statement requires the use of a ProjectOperator, and
	 * applies that to the top of the query tree according
	 *
	 * @param select PlainSelect expression representing the SQL statement
	 * @param child  and operator representing a tree that the Project will be
	 *               applied to
	 * @return LogicalOperator at the root of a tree
	 */
	@SuppressWarnings("unchecked")
	private static LogicalOperator hasProject(PlainSelect select, LogicalOperator child) {
		List<SelectItem> selectItems = select.getSelectItems();
		if (selectItems.size() <= 1 && selectItems.get(0).toString() == "*") {
			return child;
		} else {
			return new LogicalProjectOperator(child, select);
		}
	}

	/**
	 * Checks if a given SQL statement requires the use of any join operators, and
	 * if so builds for a tree for the joins that is added to the query tree
	 *
	 * @param select PlainSelect expression representing the SQL statement
	 * @return LogicalOperator at the root of the tree
	 */
	private static LogicalOperator hasJoins(PlainSelect select) {
		LogicalOperator result;
		if (select.getJoins() == null) {
			result = hasProject(select, hasSelect(select));
		} else {
			result = hasProject(select, buildJoinTree(select));
		}
		return result;
	}

	/**
	 * Checks it the SQL statement contains a sort, and adds the sort to the root of
	 * the tree if necessary
	 *
	 * @param select PlainSelect expression representing the SQL statement
	 * @return LogicalOperator at the root of the tree
	 */
	private static LogicalOperator hasSort(PlainSelect select) {
		@SuppressWarnings("unchecked")
		List<OrderByElement> orderByList = select.getOrderByElements();
		if (orderByList != null) {
			// TODO: review sorting and make sure that sorting is happening correctly; ie.
			// isn't happening twice and is happening after projections
			return new LogicalSortOperator(hasJoins(select), orderByList);
		} else {
			return hasJoins(select);
		}
	}

	/**
	 * Checks if the SQL statement has a distinct expression, and adds that to the
	 * root of the tree if necessary
	 *
	 * @param select PlainSelect expression representing the SQL statement
	 * @return LogicalOperator at the root of the tree
	 */
	private static LogicalOperator hasDistinct(PlainSelect select) {
		if (select.getDistinct() != null) {
			// TODO: careful with implementation here, may be sorting twice (I think its
			// fine for now but come back to in the future and review)
			// TODO: also do we need this part? Or is this handled in the distinct operator
			// constructor, ideally I'd want it handled here
			return new LogicalDistinctOperator(hasSort(select));
		} else {
			return hasSort(select);
		}
	}

	/**
	 * Builds a logic tree of operators to represent SQL statements
	 *
	 * @param unCastedSelect statement representing the SQL code
	 * @return LogicalOperator at the root of the tree that represents the given SQL
	 *         statement
	 */
	public static LogicalOperator buildTree(Select unCastedSelect) {
		PlainSelect select = (PlainSelect) unCastedSelect.getSelectBody();
		return hasDistinct(select);
	}

}
