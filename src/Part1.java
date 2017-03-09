import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

public class Part1 {
    public static void main(String[] args) {
        HashMap<String, String> fileContents = new HashMap<>();

        fileContents = extractTextFileContents("C://Software_Development/College/CT414/MapReduceAssignment/src/");
        // fileContents = getTextFileContents(args[0]);
    }

    // Method: Used to extract the contents of all the text files in a directory
    private static HashMap<String, String> extractTextFileContents(String fileDirectory){
        HashMap<String, String> textFileContents = new HashMap<>();

        // Get list of files in the file directory
        File folder = new File(fileDirectory);
        File[] files = folder.listFiles();

        if (files != null) {
            for (File file : files) {
                // Check if the file is a text file
                if (file.isFile() && file.getName().endsWith(".txt")) {
                    try {
                        Scanner in = new Scanner(file);
                        StringBuilder fileContents = new StringBuilder();

                        // Read contents of the file
                        while (in.hasNextLine()) {
                            fileContents.append(in.nextLine());
                        }
                        // Add the file name (key) and the its contents (value) to the HashMap
                        textFileContents.put(file.getName(), fileContents.toString());
                        // System.out.println(fileContents);
                    } catch (FileNotFoundException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
        return textFileContents;
    }
}