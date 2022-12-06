package expression;

import java.util.ArrayList;
import java.util.List;

import logicaloperators.LogicalDistinctOperator;
import logicaloperators.LogicalJoinOperator;
import logicaloperators.LogicalOperator;
import logicaloperators.LogicalProjectOperator;
import logicaloperators.LogicalScanOperator;
import logicaloperators.LogicalSelectOperator;
import logicaloperators.LogicalSortOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import operators.BNLJOperator;
import operators.DistinctOperator;
import operators.ExternalSortOperator;
import operators.IndexScanOperator;
import operators.InternalSortOperator;
import operators.Operator;
import operators.ProjectOperator;
import operators.SMJOperator;
import operators.ScanOperator;
import operators.SelectOperator;
import operators.TNLJOperator;
import utils.Catalog;
import utils.Catalog.JoinMethod;
import utils.Catalog.SortMethod;
import utils.MyTable;

/** PhysicalPlanBuilder constructs a physical plan with the visitor pattern */
public class PhysicalPlanBuilder {

	Operator physicalOp = null;

	private Catalog.JoinMethod joinMethod;
	private int outerRelationBuffer = -1;
	private Catalog.SortMethod sortMethod;
	private int sortBuffer = -1;

	/** Instantiates a new instance of PhysicalPlanBuilder() */
	public PhysicalPlanBuilder() {
		// TODO: instead of getting this from catalog, should we do the search in here?
		// It doesn't
		// really matter either way does it, but still something to consider
		joinMethod = Catalog.getJoinMethod();
		outerRelationBuffer = Catalog.getOuterRelationBuffer();
		sortMethod = Catalog.getSortMethod();
		sortBuffer = Catalog.getSortBuffer();
	}

	/**
	 * Returns the physical plan currently stored in this PhysicalPlanBuilder if it
	 * exists, else it returns null (if null, it is possible that this builder has
	 * not visited anything yet)
	 *
	 * @return The base of a tree of physical operators
	 */
	public Operator getPlan() {
		return physicalOp;
	}

	/**
	 * Visits a logical scan operator
	 *
	 * @param logicalScanOp logical operator to visit
	 */
	public void visit(LogicalScanOperator logicalScanOp) {
		physicalOp = new ScanOperator(logicalScanOp.getTable());
	}

	/**
	 * Visits a logical select operator
	 *
	 * @param logicalSelectOperator logical operator to visit
	 */
	public void visit(LogicalSelectOperator logicalSelectOperator) {
		Expression expr = logicalSelectOperator.getExpr();
		if (expr == null) {
			logicalSelectOperator.getChild().accept(this);
			physicalOp = new SelectOperator((ScanOperator) physicalOp, expr);
			return;
		}
		WhereExprVisitor whereVisitor = new WhereExprVisitor();
		expr.accept(whereVisitor);
		MyTable table = ((LogicalScanOperator) logicalSelectOperator.getChild()).getTable();
		String tableName = table.getTableFullName();
		String colName = Catalog.indexMap.get(tableName)[1];
		// processIndex returns an array of length 2, where the array looks as
		// such[lowerBound,
		// upperBound]
		Integer[] bounds = whereVisitor.getIndexBounds(colName);
		Expression newSelectExpr = whereVisitor.getNonIndexSelectExpr(colName);

		if (Catalog.useIndex && (bounds[0] != null || bounds[1] != null)) {
			if (newSelectExpr != null) {
				// need a select op with an indexscan as a child
				IndexScanOperator child = new IndexScanOperator(table, bounds[0], bounds[1]);
				physicalOp = new SelectOperator(child, newSelectExpr);
			} else {
				// needs only an index scan
				physicalOp = new IndexScanOperator(table, bounds[0], bounds[1]);
			}
		} else {
			// No index at all for this table
			logicalSelectOperator.getChild().accept(this);
			physicalOp = new SelectOperator((ScanOperator) physicalOp, expr);
		}
	}

	/**
	 * Visits a logical project operator
	 *
	 * @param logicalProjectOperator logical operator to visit
	 */
	public void visit(LogicalProjectOperator logicalProjectOperator) {
		logicalProjectOperator.getChild().accept(this);
		physicalOp = new ProjectOperator(physicalOp, logicalProjectOperator.getSelectItems());
	}

	/**
	 * Visits a logical join operator
	 *
	 * @param logicalJoinOperator logical operator to visits
	 */
	public void visit(LogicalJoinOperator logicalJoinOperator) {
		logicalJoinOperator.getLeftChild().accept(this);
		Operator leftChild = physicalOp;
		logicalJoinOperator.getRightChild().accept(this);
		Operator rightChild = physicalOp;
		if (Catalog.getJoinMethod() == JoinMethod.TNLJ) {
			physicalOp = new TNLJOperator(leftChild, rightChild, logicalJoinOperator.getExpr());
		} else if (Catalog.getJoinMethod() == JoinMethod.BNLJ) {
			int BNLJBufferSize = Catalog.getOuterRelationBuffer();
			physicalOp = new BNLJOperator(leftChild, rightChild, logicalJoinOperator.getExpr(), BNLJBufferSize);
		} else if (Catalog.getJoinMethod() == JoinMethod.SMJ) {

			Expression origJoinExpr = logicalJoinOperator.getExpr();
			WhereExprVisitor whereVisitor = new WhereExprVisitor();
			origJoinExpr.accept(whereVisitor);
			List<Expression> joinConditions = whereVisitor.getJoinExprs();

			List<String> leftSchemas = leftChild.schemas;
			List<String> rightSchemas = rightChild.schemas;
			List<Column> leftOrderBys = new ArrayList<>();
			List<Column> rightOrderBys = new ArrayList<>();

			for (Expression condition : joinConditions) {

				Column leftOrderBy = (Column) whereVisitor.getLeftSide(condition);
				Column rightOrderBy = (Column) whereVisitor.getRightSide(condition);

				// if we can find this orderby column in left table;
				if (leftSchemas.contains(leftOrderBy.toString())) {
					leftOrderBys.add(leftOrderBy);
					rightOrderBys.add(rightOrderBy);
				}
				// or, we can find this orderby column in right table
				else if (rightSchemas.contains(rightOrderBy.toString())) {
					leftOrderBys.add(rightOrderBy);
					rightOrderBys.add(leftOrderBy);
				}
			}

			// sort each of the inputs to the join
			if (Catalog.getSortMethod() == SortMethod.inmemorysort) {
				leftChild = new InternalSortOperator(leftChild, leftOrderBys);
				rightChild = new InternalSortOperator(rightChild, rightOrderBys);
			} else if (Catalog.getSortMethod() == SortMethod.externalsort) {
				leftChild = new ExternalSortOperator(leftChild, leftOrderBys);
				rightChild = new ExternalSortOperator(rightChild, rightOrderBys);
			}

			// Sort Merge Join
			physicalOp = new SMJOperator(leftChild, rightChild, logicalJoinOperator.getExpr(), leftOrderBys,
					rightOrderBys);
		}
	}

	/**
	 * Visits a logical sort operator
	 *
	 * @param logicalSortOperator logical operator to visit
	 */
	public void visit(LogicalSortOperator logicalSortOperator) {
		logicalSortOperator.getChild().accept(this);
		if (Catalog.getSortMethod() == SortMethod.inmemorysort) {
			physicalOp = new InternalSortOperator(physicalOp, logicalSortOperator.getOrderByItems());
		} else {
			physicalOp = new ExternalSortOperator(physicalOp, logicalSortOperator.getOrderByItems());

		}
	}

	/**
	 * Visits a logical distinct operator
	 *
	 * @param logicalDistinctOperator logical operator to visit
	 */
	public void visit(LogicalDistinctOperator logicalDistinctOperator) {
		logicalDistinctOperator.getChild().accept(this);
		physicalOp = new DistinctOperator(physicalOp);
	}

	/**
	 * Visits a logical operator. Used as a wrapper when we are unsure what type of
	 * logical operator will be given to this builder. Accordingly checks the class
	 * and visits accordingly, if the class of the logicalOperator given is not
	 * visitable, the builder will not visit it.
	 *
	 * @param logicalOperator logical operator to visit
	 */
	public void visit(LogicalOperator logicalOperator) {
		if (logicalOperator != null) {
			Class<? extends LogicalOperator> check = logicalOperator.getClass();
			if (check == LogicalDistinctOperator.class) {
				visit((LogicalDistinctOperator) logicalOperator);
			} else if (check == LogicalJoinOperator.class) {
				visit((LogicalJoinOperator) logicalOperator);
			} else if (check == LogicalSortOperator.class) {
				visit((LogicalSortOperator) logicalOperator);
			} else if (check == LogicalProjectOperator.class) {
				visit((LogicalProjectOperator) logicalOperator);
			} else if (check == LogicalSelectOperator.class) {
				visit((LogicalSelectOperator) logicalOperator);
			} else if (check == LogicalScanOperator.class) {
				visit((LogicalScanOperator) logicalOperator);
			}
		}
	}

}
