package operators;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import utils.Catalog;
import utils.Tuple;
import utils.TupleReader;
import utils.TupleWriter;

/**
 * External sort operator, sorts tuples using an external directory and
 * temporary files
 */
public class ExternalSortOperator extends SortOperator {

	Operator child;
	private TupleReader reader;
	private final int pageSize = 4096;
	private int B;
	private int numTuplesPerPage;
	private String scratchFilePath;
	private int runCount;
	List<?> orderBy;
	List<Column> orderKey;
	String tableName;
	HashMap<String, Integer> usableSchema;

	/**
	 * Create a ExternalSortOperator object with OrderByElements or Columns
	 *
	 * @param op  the child operator
	 * @param obe the list of OrderByElement or Column
	 */
	public ExternalSortOperator(Operator operator, List<?> orderBy) {
		child = operator;
		schemas = operator.schemas;
		B = Catalog.sortBuffer;
		numTuplesPerPage = pageSize / (schemas.size() * 4);
		runCount = 0;
		scratchFilePath = Catalog.tempdirPath + File.separator + "scratch" + Catalog.scratchIndex;
		this.orderBy = orderBy;
		orderKey = new ArrayList<>();

		File f;
		f = new File(scratchFilePath);
		f.mkdir();
		Catalog.scratchIndex++;

		step1();

		Queue<Integer> runs = new LinkedList<>();
		for (int i = 1; i <= runCount; i++) {
			runs.add(i);
		}

		while (runs.size() > 1) {
			List<Integer> temp = new LinkedList<>();
			for (int i = 1; i < B; i++) {
				if (runs.size() >= 1) {
					temp.add(runs.poll());
				}
			}
			int newRun = merge(temp, runCount);
			if (newRun != -1) {
				runs.add(newRun);
			}
		}
		reader = new TupleReader(scratchFilePath + File.separator + runCount, true);
	}

	/** Step1 of external sorting */
	private void step1() {
		try {
			Tuple t = null;
			while ((t = child.getNextTuple()) != null) {
				usableSchema = t.getSchema();
				tableName = t.getTableName();
				runCount++;
				File file1 = new File(scratchFilePath + File.separator + runCount);
				file1.createNewFile();

				TupleWriter writer = new TupleWriter(scratchFilePath + File.separator + runCount);
				List<Tuple> tuples = new ArrayList<>();
				tuples.add(t);
				int count = 0;
				while (count <= numTuplesPerPage * B && (t = child.getNextTuple()) != null) {
					tuples.add(t);
					count++;
				}

				if (orderBy != null && orderKey.size() == 0) {
					for (Object ele : orderBy) {
						Column col;
						if (ele instanceof OrderByElement) {
							col = (Column) ((OrderByElement) ele).getExpression();
						} else {
							col = (Column) ele;
						}
						orderKey.add(col);
					}
					if (orderKey.size() < tuples.get(0).getSchema().size()) {
						// need to add all tuples that arent already in it
						for (String ele : tuples.get(0).getSchemaNames()) {
							Table table = new Table();
							table.setName("");
							Column col = new Column(table, ele);
							if (!orderKey.contains(col)) {
								orderKey.add(col);
							}
						}
					}

				} else if (orderKey.size() == 0) {
					Tuple baseTuple = tuples.get(0);
					for (String ele : baseTuple.getSchemaNames()) {
						Table table = new Table();
						table.setName("");
						Column col = new Column(table, ele);
						orderKey.add(col);
					}
				}

				Collections.sort(tuples, new TupleComparator(orderKey));
				for (Tuple tuple : tuples) {
					writer.writeTuple(tuple);
				}
				writer.close();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Lower bound should start at 1, these bounds are inclusive
	 *
	 * @param runs  a list of Integer
	 * @param runId int
	 *
	 * @return int
	 */
	private int merge(List<Integer> runs, int runId) {
		if (runs.size() <= 1) {
			return runId;
		}

		try {
			TupleReader[] buffer = new TupleReader[runs.size()];
			int i = 0;
			for (int run : runs) {
				buffer[i] = new TupleReader(scratchFilePath + File.separator + run, true);
				i++;
			}

			runCount++;
			new File(scratchFilePath + File.separator + runCount).createNewFile();
			TupleWriter writer = new TupleWriter(scratchFilePath + File.separator + runCount);

			Tuple[] tupleSource = new Tuple[runs.size()];
			List<Tuple> tuples = new LinkedList<>();
			for (int j = 0; j < runs.size(); j++) {
				Tuple t = new Tuple(buffer[j].nextTuple(), tableName, usableSchema);
				tuples.add(t);
				tupleSource[j] = t;
			}

			while (tuples.size() != 0) {
				Collections.sort(tuples, new TupleComparator(orderKey));
				writer.writeTuple(tuples.get(0));

				int index = -1;
				for (int j = 0; j < tupleSource.length; j++) {
					if (tupleSource[j] == tuples.get(0)) {
						index = j;
					}
				}

				tuples.remove(0);

				String[] tupleData = buffer[index].nextTuple();
				if (tupleData != null) {
					Tuple t = new Tuple(tupleData, tableName, usableSchema);
					tuples.add(t);
					tupleSource[index] = t;
				}
			}
			for (int j = 0; j < buffer.length; j++) {
				buffer[j].close();
			}
			writer.close();
			return runCount;
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

	}

	/**
	 * gets next tuple that satisfies the join expression condition.
	 *
	 * @return the tuple being read from the table
	 */
	@Override
	public Tuple getNextTuple() {
		String[] temp = reader.nextTuple();
		if (temp != null) {
			return new Tuple(temp, tableName, usableSchema);
		} else {
			return null;
		}

	}

	/** reset operator to initial state */
	@Override
	public void reset() {
		reader.reset();
		// Seems to cause an exception when I try to reset the reader
	}

	/** Reset the inner operator to a particular tuple or tuple index */
	@Override
	public void reset(int index) {
		reader.reset(index);
	}
}
