package entry;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Scanner;

import expression.PhysicalPlanBuilder;
import logicaloperators.LogicalOperator;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import operators.Operator;
import utils.Catalog;

/**
 * A class that takes in input path, output path, and temp path to execute SQL
 * queries. This is the entry point of the SQLparser.
 */
public class Interpreter {
	private static String runSQL;

	/**
	 * Takes in input and out directory and parses SQL statements. Each query output
	 * is printed into separate files
	 */
	public static void main(String[] args) {
		executeQuery(args[0]);
	}

	/**
	 * Takes in a config file for the interpreter and parses input, output, temp
	 * directory, and runs SQL commands if needed
	 *
	 * @param config the path to the interpreter config file
	 */
	private static void executeQuery(String config) {
		try (Scanner configScanner = new Scanner(new File(config))) {
			String input = configScanner.nextLine();
			String output = configScanner.nextLine();
			String tempdir = configScanner.nextLine();
			Catalog.init(input, output, tempdir);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}

		Catalog.getInstance();
		if (runSQL.equals("0")) {
			return;
		}
		CCJSqlParser jsqlParser = new CCJSqlParser(Catalog.getQueryReader());
		int id = 1;
		Statement statement;
		try {
			while ((statement = jsqlParser.Statement()) != null) {
				PrintStream ps = null;
				Select select = (Select) statement;
				try {
					LogicalOperator logOp = QueryBuilder.buildTree(select);
					PhysicalPlanBuilder builder = new PhysicalPlanBuilder();
					builder.visit(logOp);
					Operator operator = builder.getPlan();
					String binaryPath = Catalog.outputPath + File.separator + "query" + String.valueOf(id);
					ps = new PrintStream(new File(binaryPath + "_humanreadable"));
					long startTime = System.currentTimeMillis();
					operator.dump(binaryPath, ps);
					long endTime = System.currentTimeMillis();
					System.out.println(endTime - startTime);
					operator.reset();
					id++;
					clearTempdir();
				} catch (Exception e) {
					System.out.println(id + " FAILED: " + select + " " + e);
					e.printStackTrace();
					id++;
				}
			}
		} catch (ParseException e) {
			System.out.println("PARSE FAILED");
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * clear temp directory used for external sort between each query
	 */
	private static void clearTempdir() {
		File folder = new File(Catalog.tempdirPath);
		if (folder.list() != null && folder.list().length != 0) {
			for (File outerFile : folder.listFiles()) {
				for (File innerFile : outerFile.listFiles()) {
					innerFile.delete();
				}
			}
			folder.delete();

		}
		Catalog.scratchIndex = 0;
	}

}
