import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MapReduce {

    public static void main(String[] args) {
        // Command line arguments
        // String fileDirectory = args[0];
        //int numberThreadPools = Integer.valueOf(args[1]);
        int numberThreadPools = 10;
        long approach3;

        //Map<String, String> input = extractTextFileContents(fileDirectory);
        Map<String, String> input = extractTextFileContents("C://Software_Development/College/CT414/MapReduceAssignment/src/");


        long startTime = System.currentTimeMillis();
        // APPROACH #3: Distributed MapReduce
        {
            final Map<String, Map<String, Integer>> output = new HashMap<>();

            // MAP:

            final List<MappedItem> mappedItems = new LinkedList<>();

            final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                @Override
                public synchronized void mapDone(String file, List<MappedItem> results) {
                    mappedItems.addAll(results);
                }
            };

            ExecutorService executor = Executors.newFixedThreadPool(numberThreadPools);

            for (Map.Entry<String, String> entry : input.entrySet()) {
                final String file = entry.getKey();
                final String contents = entry.getValue();

                executor.execute(() -> map(file, contents, mapCallback));
            }

            executor.shutdown();
            while (!executor.isTerminated()) {}

            // GROUP:

            Map<String, List<String>> groupedItems = new HashMap<>();

            for (MappedItem item : mappedItems) {
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.computeIfAbsent(word, k -> new LinkedList<>());
                list.add(file);
            }

            // REDUCE:

            final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                @Override
                public synchronized void reduceDone(String k, Map<String, Integer> v) {
                    output.put(k, v);
                }
            };

            executor = Executors.newFixedThreadPool(numberThreadPools);

            for (Map.Entry<String, List<String>> entry : groupedItems.entrySet()) {
                final String word = entry.getKey();
                final List<String> list = entry.getValue();

                executor.execute(() -> reduce(word, list, reduceCallback));
            }

            executor.shutdown();
            while (!executor.isTerminated()){}
            System.out.println(output);
        }

        long endTime = System.currentTimeMillis();
        approach3 = endTime - startTime;

        System.out.println("Time taken #3: " + approach3 + " (mS)");
        System.out.println("Number of threads: " + numberThreadPools);
    }

    public interface MapCallback<E, V> {

        void mapDone(E key, List<V> values);
    }

    private static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        List<MappedItem> results = new ArrayList<>(words.length);
        for(String word: words) {
            // Map using first letter instead of entire word
            String firstLetter = String.valueOf(word.charAt(0)).toUpperCase();
            results.add(new MappedItem(firstLetter, file));
        }
        callback.mapDone(file, results);
    }

    public interface ReduceCallback<E, K, V> {

        void reduceDone(E e, Map<K,V> results);
    }

    private static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {
        Map<String, Integer> reducedList = new HashMap<>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences + 1);
            }
        }
        callback.reduceDone(word, reducedList);
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
                        System.exit(0);
                    }
                }
            }
        }
        return textFileContents;
    }


    private static class MappedItem {

        private final String word;
        private final String file;

        MappedItem(String word, String file) {
            this.word = word;
            this.file = file;
        }

        String getWord() {
            return word;
        }

        String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + word + "\",\"" + file + "\"]";
        }
    }
}
