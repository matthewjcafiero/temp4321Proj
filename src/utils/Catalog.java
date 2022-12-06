package utils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import net.sf.jsqlparser.statement.select.Join;
import operators.TupleComparator;

/**
 * Catalog, singleton class that reads config files and tables, and sets up
 * output directory
 */
public class Catalog {
	private static Catalog instance = null;
	public static String inputPath;
	public static String outputPath;
	public static String tempdirPath;
	public static Boolean doIndex;
	public static String indexPath;
	private static HashMap<String, List<String>> tableSchemas = new HashMap<>();
	public static HashMap<String, String[]> indexMap = new HashMap<>();

	public enum JoinMethod {
		TNLJ, BNLJ, SMJ
	}

	public enum SortMethod {
		inmemorysort, externalsort
	}

	private static int joinMethod = -1;
	private static int outerRelationBuffer = -1;
	private static int sortMethod = -1;
	public static int sortBuffer = -1;
	public static boolean useIndex = false;

	public static int scratchIndex = 0;

	/**
	 * Initialize new catalog instance, process plan builder config, index config,
	 * and schemas
	 */
	private Catalog() {
		processPlanBuilderConfigFile(inputPath + File.separator + "plan_builder_config.txt");
		String schemasPath = inputPath + File.separator + "db" + File.separator + "schema.txt";
		indexPath = inputPath + File.separator + "db" + File.separator + "indexes" + File.separator;
		File file = new File(schemasPath);
		Scanner scanner = null;
		List<String> tableNames = new LinkedList<>();
		try {
			scanner = new Scanner(file);
			while (scanner.hasNextLine()) {
				String[] schema = scanner.nextLine().split(" ");
				if (schema.length > 1) {
					String tableName = schema[0];
					tableNames.add(tableName);
					List<String> val = new ArrayList<>();
					for (int i = 1; i < schema.length; i++) {
						val.add(schema[i]);
					}
					tableSchemas.put(tableName, val);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (scanner != null)
			scanner.close();
		processIndexConfig();

	}

	/**
	 *
	 * process the builder config file to get appropriate settings for methods and
	 * buffers
	 *
	 * @param path path to the plan builder config file
	 */
	private void processPlanBuilderConfigFile(String path) {
		File file = new File(path);
		Scanner scanner = null;
		try {
			scanner = new Scanner(file);
			while (scanner.hasNextLine()) {
				String[] line = scanner.nextLine().split(" ");
				if (line.length == 1) {
					if (joinMethod == -1) {
						joinMethod = Integer.parseInt(line[0]);
					} else if (sortMethod == -1) {
						sortMethod = Integer.parseInt(line[0]);
					} else {
						useIndex = Integer.parseInt(line[0]) == 1;
					}
				} else if (line.length > 1) {
					if (joinMethod == -1) {
						joinMethod = Integer.parseInt(line[0]);
						outerRelationBuffer = Integer.parseInt(line[1]);
					} else {
						sortMethod = Integer.parseInt(line[0]);
						sortBuffer = Integer.parseInt(line[1]);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (scanner != null)
			scanner.close();
	}

	/**
	 * Process index configuration to get the appropriate settings for index trees
	 * and index files
	 */
	private void processIndexConfig() {
		String indexConfigPath = Catalog.inputPath + File.separator + "db" + File.separator + "index_info.txt";
		try (Scanner indexScanner = new Scanner(new File(indexConfigPath))) {
			while (indexScanner.hasNextLine()) {
				String[] line = indexScanner.nextLine().split(" ");
				String table = line[0];
				String col = line[1];
				String cluster = line[2];
				String order = line[3];
				if (line.length > 3) {
					Catalog.indexMap.put(table, line);
				}
				if (doIndex) {
					if (Integer.valueOf(cluster) == 1) {
						sortTableByIndex(table);
					}
					int index = tableSchemas.get(table).indexOf(col);
					IndexBuilder indexBuilder = new IndexBuilder(Catalog.getTableReader(line[0]), index,
							Integer.valueOf(order));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Creates one instance of Catalog object to be used across other classes
	 *
	 * @return Catalog object that is created
	 */
	public static synchronized Catalog getInstance() {
		if (instance == null)
			instance = new Catalog();
		return instance;
	}

	/**
	 * Initialize Catalog with input directory and output directory.
	 *
	 * @param inputDir  input directory supplied by client
	 * @param outputDir output directory supplied by client
	 * @param tempdir   temporary directory used for sorting
	 * @param doIndex   whether to build indexes
	 */
	public static void init(String inputDir, String outputDir, String tempdir) {
		inputPath = inputDir;
		outputPath = outputDir;
		tempdirPath = tempdir;
	}

	/**
	 * Find the file that holds queries and construct a reader for that file
	 *
	 * @return a FileReader reading the query file
	 */
	public static FileReader getQueryReader() {
		String queryPath = inputPath + File.separator + "queries.sql";
		try {
			return new FileReader(queryPath);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Find the file that holds a table and construct a reader for that file
	 *
	 * @param tableName name of the table to read from
	 *
	 * @return a TupleReader reading the table file
	 */
	public static TupleReader getTableReader(String tableName) {
		return new TupleReader(tableName, false);
	}

	/**
	 * Wrapper to get table from a join item
	 *
	 * @param item Join item that represents the table
	 * @return a MyTable representation of the data
	 */
	public static MyTable getMyTable(Join item) {
		return new MyTable(item.getRightItem());
	}

	/**
	 * @param fullName full name of the table for looking up schema
	 *
	 * @return corresponding schema of the input table name
	 */
	public static List<String> getSchema(String fullName) {
		return tableSchemas.get(fullName);
	}

	/**
	 * processes the join method from plan builder config into an enum
	 *
	 * @return enum representing join method to use
	 */
	public static JoinMethod getJoinMethod() {
		if (joinMethod == 0) {
			return JoinMethod.TNLJ;
		} else if (joinMethod == 1) {
			return JoinMethod.BNLJ;
		} else if (joinMethod == 2) {
			return JoinMethod.SMJ;
		}
		return null;
	}

	/**
	 * gets outerRelationBuffer
	 *
	 * @return int representing size of the outer relation buffer
	 */
	public static int getOuterRelationBuffer() {
		return outerRelationBuffer;
	}

	/**
	 * processes the sort method from plan builder config into an enum
	 *
	 * @return an enum representing the sort method to use
	 */
	public static SortMethod getSortMethod() {
		if (sortMethod == 0) {
			return SortMethod.inmemorysort;
		} else if (sortMethod == 1) {
			return SortMethod.externalsort;
		}
		return null;
	}

	/**
	 * gets sortBuffer
	 *
	 * @return int representing size of the sort buffer
	 */
	public static int getSortBuffer() {
		return sortBuffer;
	}

	/**
	 * Sort table with tableName using index
	 *
	 * @param tableName the full name of the table
	 */
	private static void sortTableByIndex(String tableName) {
		List<Tuple> tuples = new ArrayList<>();
		TupleReader tupleReader = getTableReader(tableName);
		String[] t = null;
		while ((t = tupleReader.nextTuple()) != null) {
			tuples.add(new Tuple(t, tableName, getSchema(tableName)));
		}
		tupleReader.close();
		Collections.sort(tuples, new TupleComparator(tableName));

		TupleWriter tupleWriter = new TupleWriter(tupleReader.getFile());
		try {
			for (Tuple tuple : tuples) {
				tupleWriter.writeTuple(tuple);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			tupleWriter.close();
		}
	}
}
