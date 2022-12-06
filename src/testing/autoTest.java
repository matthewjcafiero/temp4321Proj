package testing;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import entry.Interpreter;

public class autoTest {

    static LinkedList<String> sqlLines= new LinkedList<>();

    public static void main(String[] args) throws IOException {
        destroyContent("src/testing/computedOutputs");
        Interpreter.main(new String[] { "src/testing/interpreter_config_file" });
        String fileLocation= "src/testing/";
        int numQueries= new File(fileLocation + "computedOutputs").list().length / 2;
        String[] sqlQueries= processSQlLines(fileLocation + "input/queries.sql");
        boolean allPassed= true;
        int check;
        for (int i= 1; i <= numQueries; i++ ) {
            if (sqlQueries[i - 1].contains("ORDER")) {
                check= compareLines(
                    new File(fileLocation + "computedOutputs/query" + i + "_humanreadable"),
                    new File(fileLocation + "expectedOutputs/query" + i + "_humanreadable"));
            } else {
                check= compareContents(
                    new File(fileLocation + "computedOutputs/query" + i + "_humanreadable"),
                    new File(fileLocation + "expectedOutputs/query" + i + "_humanreadable"));
            }
            if (check == -1) {
                System.out.println("FAILED: Query " + i + " -- " + sqlQueries[i - 1] + ".");
                allPassed= false;
            } else if (check == 0) {
                System.out.println("FAILED: Expected Output \"query" + i + "\" does not exist.");
                allPassed= false;
            }
        }
        if (allPassed) {
            System.out.println("SUCCESS: All " + numQueries + " test cases passed.");
        } else {
            System.out.println("All tests finshed.  Atleast 1 test failed.");
        }
    }

    public static String[] processSQlLines(String SQLpath) throws IOException {
        BufferedReader reader= new BufferedReader(new FileReader(SQLpath));
        List<String> result= new LinkedList<>();
        String currLine= reader.readLine();
        while (currLine != null) {
            String[] splitLine= currLine.split("--");
            if (splitLine[0].contains("SELECT")) {
                result.add(splitLine[0]);
            }
            currLine= reader.readLine();
        }
        reader.close();
        String[] resultArr= new String[result.size()];
        int counter= 0;
        for (String ele : result) {
            resultArr[counter++ ]= ele;
        }
        return resultArr;
    }

    public static int compareLines(File produced, File expected) throws IOException {
        try {
            BufferedInputStream producedBIS= new BufferedInputStream(new FileInputStream(produced));
            BufferedInputStream expectedBIS= new BufferedInputStream(new FileInputStream(expected));
            int result;
            if (Arrays.equals(producedBIS.readAllBytes(), expectedBIS.readAllBytes())) {
                result= 1;
            } else {
                result= -1;
            }
            producedBIS.close();
            expectedBIS.close();
            return result;
        } catch (Exception e) {
            return 0;
        }
    }

    public static int compareContents(File produced, File expected) throws IOException {
        try {
            int result;
            Scanner scannerProduced= new Scanner(produced);
            Set<String> producedSet= new HashSet<>();
            while (scannerProduced.hasNextLine()) {
                String line= scannerProduced.nextLine();
                producedSet.add(line);
            }
            Scanner scannerExpected= new Scanner(expected);
            Set<String> expectedSet= new HashSet<>();
            while (scannerExpected.hasNextLine()) {
                String line= scannerExpected.nextLine();
                expectedSet.add(line);
            }
            if (expectedSet.equals(producedSet)) {
                result= 1;
            } else {
                result= -1;
            }
            scannerProduced.close();
            scannerExpected.close();
            return result;
        } catch (Exception e) {
            return 0;
        }
    }

    public static void destroyContent(String path) {
        File folder= new File(path);
        if (folder != null && folder.listFiles() != null) {
            for (File file : folder.listFiles()) {
                file.delete();
            }
            if (folder.listFiles().length == 0) {
                System.out.println("Successfully cleared folder");
            } else {
                System.out.println("Folder content destruction failed");
            }
        }
        System.out.println("Nothing to clear");

    }

}
