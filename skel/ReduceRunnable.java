

import java.io.File;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RecursiveAction;

/**
*  Class that executes the Reduce Operations
*  for each Reduce Task in the list using workers
*/
public class ReduceRunnable extends RecursiveAction {
    /**
    * Index of the Reduce Task 
    */
    private final int reduceIndex;
    /**
    * List of Reduce Tasks
    */
    private final ArrayList<TaskReduce> listReduce;
    /**
    * Output HashMap with the rank of the file as key and 
    * the String to be written to output as value
    */
    private ConcurrentHashMap<Double, String> outMap;
    /**
    * HashMap which combines the maps created for the Map Operations
    */
    private HashMap<Integer, ArrayList<String>> combineMap;
    /**
    * Total words in the file
    */
    private int totalWords = 0;
    /**
    * The maximum length of a word found in the file
    */
    private int maxLength = 0;
    /**
    * The number of words with the maximum length
    */
    private int maxLengthCount = 0;
    /**
    * Set of all words in the file
    */
    private Set<String> set;

    public ReduceRunnable(int reduceIndex, ArrayList<TaskReduce> listReduce, ConcurrentHashMap<Double, String> outMap) {
        this.reduceIndex = reduceIndex;
        this.listReduce =  listReduce;
        this.outMap = outMap;
    }

    @Override
    protected void compute()  {
        TaskReduce reduce = listReduce.get(reduceIndex);

        combineMap = new HashMap<>();
        set = new HashSet<>();

        combineWordMaps(reduce);

        double rank = computeFileRank();

        // Calculate the number of words which are the maximum length
        maxLengthCount = combineMap.entrySet().stream()
                .filter( e -> e.getKey() == maxLength)
                .map(Map.Entry::getValue)
                .findFirst()
                .get()
                .size();


        // Add entry to output map
        File file = new File(reduce.getFileName());
        String name = file.getName();
        DecimalFormat df = new DecimalFormat("0.00");
        outMap.putIfAbsent(rank, name + "," + df.format(rank) + "," + maxLength + ","  + maxLengthCount);

        List<ReduceRunnable> tasks = new ArrayList<>();
        if (reduceIndex + 1 < listReduce.size()) {
            ReduceRunnable t = new ReduceRunnable(reduceIndex + 1, listReduce, outMap);
            tasks.add(t);
            t.fork();
        }

        for (ReduceRunnable task : tasks) {
            task.join();
        }
    }

     /**
     * Combines the maps created during the Map Operations
     * for each file, and also calculates the total number of words
     * and computes the maximum length found in the file
     * @param reduce the Reduce Task which contains a list of Map Tasks
     */
    public void combineWordMaps(TaskReduce reduce) {
        // Parse the list of Map Tasks for the file
        for (TaskMap map : reduce.getTaskMapList()) {

            if (map.getFragmentMap() != null) {

                // Parse the map of the Map Task
                for (Map.Entry<Integer, ArrayList<String>> entry : map.getFragmentMap().entrySet()) {

                    // Finds the maximum length
                    if (entry.getKey() > maxLength) {
                        maxLength = entry.getKey();
                    }

                    // Adds the number of words to total size
                    totalWords += entry.getValue().size();
                    // Adds the words to the set
                    set.addAll(entry.getValue());

                    // If the key is not found in the combineMap
                    if (!combineMap.containsKey(entry.getKey())) {
                        // Add the entry to the map
                        combineMap.put(entry.getKey(), entry.getValue());
                    } else {
                        // Else add the words to the existing entry
                        ArrayList<String> list = combineMap.get(entry.getKey());
                        list.addAll(entry.getValue());
                        
                        combineMap.put(entry.getKey(), list);
                    }
                }
            }
        }
    }

     /**
     * Computes the file rank
     * @return the rank of the file
     */
    public double computeFileRank() {
        double rank = 0;

        for (String word : set) {
            // Finds the entry which contains the word's length
            ArrayList<String> list = combineMap.entrySet().stream()
                    .filter( e -> e.getKey() == word.length())
                    .map(Map.Entry::getValue)
                    .findFirst().get();

            // Computes the frequency of the word in the list
            int wordCount = Collections.frequency(list, word);

            rank += computeFib(word.length() + 1) * wordCount;
        }

        return rank / totalWords;
    }

     /**
     * Computes the value of the word's length using the
     * fibonacci sequence
     * @param position the length of the word incremented by 1
     * @return value of the length
     */
    public double computeFib(int position) {
        if (position <= 1)
            return position;
        return computeFib(position - 1) + computeFib(position - 2);
    }
}
