package operators;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import utils.Tuple;

/**
 * This Tuple Comparator compares 2 tuples from 2 tables for sort merge join
 */
public class SMJTupleComparator implements Comparator<Tuple> {

	List<Column> leftCols = new ArrayList<>();
	List<Column> rightCols = new ArrayList<>();

	/**
	 * Constructor of a SMJ tuple comparator object
	 *
	 * @param leftCols  list of left columns, involved in join condition(s)
	 * @param rightCols list of right columns, involved in join condition(s)
	 */
	public SMJTupleComparator(List<Column> leftCols, List<Column> rightCols) {
		this.leftCols = leftCols;
		this.rightCols = rightCols;
	}

	/**
	 * Compares two tuples according to the columns
	 *
	 * @param t1 tuple1
	 * @param t2 tuple2
	 *
	 * @return the value 0 if t1 == t2; a value less than 0 if t1 is smaller; and a
	 *         value greater than 0 if tuple1 is larger
	 */
	@Override
	public int compare(Tuple t1, Tuple t2) {
		for (int i = 0; i < leftCols.size(); i++) {
			long val1 = t1.getValByCol(leftCols.get(i));
			long val2 = t2.getValByCol(rightCols.get(i));
			int cmp = Long.compare(val1, val2);
			if (cmp != 0) // there is inequality
				return cmp;
		}
		return 0; // 2 tuples are same at the designated column(s)
	}

}
