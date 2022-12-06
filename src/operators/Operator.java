package operators;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import utils.Tuple;
import utils.TupleWriter;

/**
 * Base class for operators
 */
public abstract class Operator {
	public List<String> schemas;

	/** Gets next tuple from table */
	public abstract Tuple getNextTuple();

	/** Reset entire table */
	public abstract void reset();

	/**
	 * Prints tuples using a TupleWriter implementation
	 *
	 * @param binaryFilePath path to the output of the tupleWriter
	 */
	public void dump(String binaryFilePath, PrintStream printStream) {
		TupleWriter tupleWriter = new TupleWriter(binaryFilePath);
		try {
			Tuple curr = getNextTuple();
			while (curr != null) {
				tupleWriter.writeTuple(curr);
				printStream.println(curr.print());
				curr = getNextTuple();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		tupleWriter.close();
	}
}
