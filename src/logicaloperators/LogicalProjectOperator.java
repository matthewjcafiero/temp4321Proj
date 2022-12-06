package logicaloperators;

import java.util.List;

import expression.PhysicalPlanBuilder;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

/**
 * represents logical project operator
 */
public class LogicalProjectOperator extends LogicalOperator {

	private LogicalOperator child;
	private List<SelectExpressionItem> selectItems;

	/**
	 * Instantiates new project operator
	 *
	 * @param child  Operator beneath this operator
	 * @param select PlainSelect expression representing a SQL state4ment
	 */
	@SuppressWarnings("unchecked")
	public LogicalProjectOperator(LogicalOperator child, PlainSelect select) {
		setChild(child);
		setSelectItems(select.getSelectItems());
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
	 * Get the select items associated with this operator
	 *
	 * @return List of SelectExpressionItems associated with this operator
	 */
	public List<SelectExpressionItem> getSelectItems() {
		return selectItems;
	}

	/**
	 * Sets the select items associated with this operator
	 *
	 * @param selectItems
	 */
	public void setSelectItems(List<SelectExpressionItem> selectItems) {
		this.selectItems = selectItems;
	}

}
