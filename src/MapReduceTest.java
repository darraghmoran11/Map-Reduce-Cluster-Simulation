import java.io.File;
import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class MapReduceTest {

    static int numberThreadPools = 100;
    static int numberOfTimes = 25;
    static ArrayList<Long> approach3Part3 = new ArrayList<Long>();

    public static void main(String[] args) {
        // Command line arguments
        // String fileDirectory = args[0];
        // int numberThreadPools = Integer.valueOf(args[1]);
        long startTime;
        long endTime;

        //Map<String, String> input = extractTextFileContents(fileDirectory);
        Map<String, String> input = extractTextFileContents("C://Software_Development/College/CT414/MapReduceAssignment/src/");


        // APPROACH #3: Distributed MapReduce
        for (int i = 0; i<=numberOfTimes; i++)
        {
            startTime = System.currentTimeMillis();
            final Map<String, Map<String, Integer>> output = new HashMap<>();

            //////////////////////////// MAP /////////////////////////////
            final List<MapReduce.MappedItem> mappedItems = new LinkedList<>();

            final MapReduce.MapCallback<String, MapReduce.MappedItem> mapCallback = new MapReduce.MapCallback<String, MapReduce.MappedItem>() {
                @Override
                public synchronized void mapDone(String file, List<MapReduce.MappedItem> results) {
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
            //////////////////////////////////////////////////////////////

            //////////////////////// GROUP ///////////////////////////////
            Map<String, List<String>> groupedItems = new HashMap<>();

            for (MapReduce.MappedItem item : mappedItems) {
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.computeIfAbsent(word, k -> new LinkedList<>());
                list.add(file);
            }
            //////////////////////////////////////////////////////////////

            // REDUCE:

            final MapReduce.ReduceCallback<String, String, Integer> reduceCallback = new MapReduce.ReduceCallback<String, String, Integer>() {
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
            while (!executor.isTerminated()) {}
            //System.out.println(output);
            endTime = System.currentTimeMillis();
            approach3Part3.add(endTime - startTime);
        }

        System.out.println("Time taken approach 3: " + approach3Part3 + " (mS)");
        System.out.println("Avg. Time taken #3 Part 3: " + round(calculateAverage(approach3Part3), 2) + " (mS)");
        System.out.println("Number of threads: " + numberThreadPools);
    }

    public interface MapCallback<E, V> {

        void mapDone(E key, List<V> values);
    }

    private static void map(String file, String contents, MapReduce.MapCallback<String, MapReduce.MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        List<MapReduce.MappedItem> results = new ArrayList<>(words.length);
        for(String word: words) {
            // Map using first letter instead of entire word
            if (Character.isLetter(word.charAt(0))) {
                String firstLetter = String.valueOf(word.charAt(0)).toUpperCase();
                results.add(new MapReduce.MappedItem(firstLetter, file));
            }
        }
        callback.mapDone(file, results);
    }

    public interface ReduceCallback<E, K, V> {

        void reduceDone(E e, Map<K,V> results);
    }

    private static void reduce(String word, List<String> list, MapReduce.ReduceCallback<String, String, Integer> callback) {
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

    public static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        BigDecimal bd = new BigDecimal(value);
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }

    public static double calculateAverage(List <Long> times) {
        long sum = 0;
        if(!times.isEmpty()) {
            for (long time : times) {
                sum += time;
            }
            return (double) sum / times.size();
        }
        return sum;
    }
}
