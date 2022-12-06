package operators;

import java.util.ArrayList;
import java.util.List;

import expression.JoinExprVisitor;
import net.sf.jsqlparser.expression.Expression;
import utils.Tuple;

/**
 * Block nested loop join operator, joins two tables using block-nested loop
 * join
 */
public class BNLJOperator extends JoinOperator {

	List<Tuple> block;
	int blockSize;
	int counter;
	Tuple tuple1, tuple2;
	private static int pageSize = 4096;

	/**
	 * Constructor for a new BNLJOperator object
	 *
	 * @param leftChild  left child operator
	 * @param rightChild right child operator
	 * @param expr       join expression to satisfy
	 * @param pages      number of pages in the buffer
	 */
	public BNLJOperator(Operator leftChild, Operator rightChild, Expression expr, int pages) {
		super(leftChild, rightChild, expr);
		int tupleSize = leftChild.schemas.size() * 4;
		blockSize = pages * (pageSize / tupleSize);
		block = new ArrayList<>();
		readBlock();
		tuple2 = rightChild.getNextTuple();
	}

	/**
	 * clear current block and read the tuples from left child into block
	 */
	private void readBlock() {
		block = new ArrayList<>();
		int idx = 0;
		Tuple tuple = null;
		while (idx < blockSize && (tuple = leftChild.getNextTuple()) != null) {
			block.add(tuple);
			idx += 1;
		}
		resetBlock();
	}

	/**
	 * get the Tuple with index counter from block
	 *
	 * @return the Tuple with index counter in block
	 */
	private Tuple getTuple() {
		return counter < block.size() ? block.get(counter) : null;
	}

	/**
	 * get next tuple that satisfies the join expression condition
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple t = null;
		while (tuple1 != null && tuple2 != null) {
			if (expr == null)
				t = joinTuples(tuple1, tuple2);
			else {
				JoinExprVisitor joinVisitor = new JoinExprVisitor();
				joinVisitor.readTwoTuples(tuple1, tuple2);
				expr.accept(joinVisitor);
				if (joinVisitor.getStatus()) {
					t = joinTuples(tuple1, tuple2);
				}
			}
			updateTuples();
			if (t != null)
				return t;
		}
		return null;
	}

	/**
	 * Update tuples to check the next pair of tuples from two tables
	 */
	private void updateTuples() {
		if (tuple2 != null) {
			tuple1 = getTuple();
			counter += 1;
			if (tuple1 != null) {
				return;
			}
			tuple2 = rightChild.getNextTuple();
			if (tuple2 != null) {
				resetBlock();
				return;
			}
		}
		readBlock();
		rightChild.reset();
		tuple2 = rightChild.getNextTuple();

	}

	/**
	 * reset block to start at first entry
	 */
	private void resetBlock() {
		counter = 0;
		tuple1 = getTuple();
		counter += 1;
	}

	/**
	 * reset operator to its initial state
	 */
	@Override
	public void reset() {
		leftChild.reset();
		rightChild.reset();
		readBlock();
		tuple2 = rightChild.getNextTuple();
	}

}
