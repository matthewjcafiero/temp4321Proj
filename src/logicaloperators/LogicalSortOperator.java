package logicaloperators;

import java.util.List;

import expression.PhysicalPlanBuilder;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * represents logical sort operator
 */
public class LogicalSortOperator extends LogicalOperator {

	private LogicalOperator child;
	private List<OrderByElement> orderByItems;

	/**
	 * Create a SortOperator object with OrderByElements
	 *
	 * @param obe the list of OrderByElement
	 * @param op  the child operator
	 */
	public LogicalSortOperator(LogicalOperator operator, List<OrderByElement> orderBy) {
		setChild(operator);
		setOrderByItems(orderBy);
	}

	/** Accepts a PhysicalPlanBuilder and has said builder visit this operator */
	@Override
	public void accept(PhysicalPlanBuilder physical_builder) {
		physical_builder.visit(this);
	}

	/**
	 * Gets the child of this operator
	 *
	 * @return child of this operator
	 */
	public LogicalOperator getChild() {
		return child;
	}

	/**
	 * Sets the child of this operator
	 *
	 * @param child logicalOperator that will be the new child
	 */
	public void setChild(LogicalOperator child) {
		this.child = child;
	}

	/**
	 * Gets the order by items of this operator
	 *
	 * @return List of OrderByElements representing the order by items of this
	 *         operator
	 */
	public List<OrderByElement> getOrderByItems() {
		return orderByItems;
	}

	/**
	 * Sets the order by items of this operator
	 *
	 * @param orderByItems List of OrderByElements representing the new order by
	 *                     elements of this operator
	 */
	public void setOrderByItems(List<OrderByElement> orderByItems) {
		this.orderByItems = orderByItems;
	}

}
