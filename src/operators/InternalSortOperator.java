package operators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import utils.Tuple;

/**
 * Internal Sort operator, sorts tuples in memory
 */
public class InternalSortOperator extends SortOperator {

	Column col;
	Operator child;
	List<Tuple> tuples = new ArrayList<>();
	List<Column> orderKey = new ArrayList<>();
	private int index = 0;

	/**
	 * Create a InternalSortOperator object with OrderByElement or Columns
	 *
	 * @param op  the child operator
	 * @param obe the list of OrderByElement or Column
	 */
	public InternalSortOperator(Operator operator, List<?> orderBy) {
		child = operator;
		schemas = operator.schemas;
		try {
			Tuple t = null;
			while ((t = child.getNextTuple()) != null) {
				tuples.add(t);
			}

			if (orderBy != null) {
				for (Object ele : orderBy) {
					Column col;
					if (ele instanceof OrderByElement) {
						col = (Column) ((OrderByElement) ele).getExpression();
					} else {
						col = (Column) ele;
					}
					orderKey.add(col);
				}

			} else {
				// System.out.println(tuples);
				// This is a bit of a patchwork fix to handle situations where there is no
				// orderBy
				// list (ie. we order by every part of the tuple in order of columns from left
				// to
				// right)
				Tuple baseTuple = tuples.get(0);
				for (String ele : baseTuple.getSchemaNames()) {
					Table table = new Table();
					table.setName("");
					Column col = new Column(table, ele);
					orderKey.add(col);
				}
			}

			Collections.sort(tuples, new TupleComparator(orderKey));

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/** get next tuple within a list of sorted tuples */
	@Override
	public Tuple getNextTuple() {
		if (index >= tuples.size())
			return null;
		return tuples.get(index++);
	}

	/** reset operator index to start of list */
	@Override
	public void reset() {
		index = 0;
	}

	/** Reset the inner operator to a particular tuple or tuple index */
	@Override
	public void reset(int index) {
		this.index = index;
	}

}
