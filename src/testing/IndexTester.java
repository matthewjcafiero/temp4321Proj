package testing;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class IndexTester {

	public static void main(String[] args) throws IOException {
		String indexes = args[0];
		String expected = args[1];
		File folder = new File(indexes);
		File[] listOfFiles = folder.listFiles();
		for (File f : listOfFiles) {
			File expected_f = new File(expected + File.separator + f.getName());
			FileInputStream in_f = new FileInputStream(f);
			FileInputStream in_e = new FileInputStream(expected_f);
			int c = 0;
			while ((c = in_f.read()) != -1) {
				int e = in_e.read();
				if (c != e) {
					System.out.println(f.getName() + e + " != " + c);
				}
			}
			in_f.close();
			in_e.close();
		}

	}

}
