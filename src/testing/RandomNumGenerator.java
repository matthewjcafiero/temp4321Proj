package testing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import utils.Tuple;
import utils.TupleWriter;

public class RandomNumGenerator {
	public static void main(String[] args) throws IOException {
		int large = Integer.parseInt(args[0]);
		ArrayList<String> schema = new ArrayList<>();
		schema.add("A");
		schema.add("B");
		schema.add("C");
		for (int dbID = 1; dbID < 4; dbID++) {
			TupleWriter tw = new TupleWriter("src/testing/random_input/db" + dbID);
			for (int i = 0; i < 10000; i++) {
				ArrayList<Long> values = new ArrayList<>();
				for (int e = 0; e < 3; e++) {
					Random ran = new Random();
					long x = ran.nextInt(large) + 1;
					values.add(x);
				}
				Long[] tupleValues = values.toArray(new Long[0]);
				Tuple t = new Tuple(tupleValues, "db_" + dbID, schema);
				tw.writeTuple(t);
			}
			tw.close();
		}

	}

}
