import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

/**
 * Class that manages the Map-Reduce operations
 * and coordinates the worker threads
*/
public class Coordinator {
    /**
    * list of files to process
    */
    private ArrayList<String> fileList;
    /**
    * Default size of fragments that the files will be split into
    */
    private long fragmentSize;
    /**
    * Number of active workers
    */
    private int nWorkers;
    /**
    * Output file which will contain the rank of each file
    */
    private String outFile;
    /**
    * List of Map Tasks
    */
    private ArrayList<TaskMap> mapList;
    /**
    * List of Reduce Tasks 
    */
    private ArrayList<TaskReduce> reduceList;

    public Coordinator(ArrayList<String> fileList, int fragmentSize, int nWorkers, String outFile) {
        this.fileList = fileList;
        this.fragmentSize = fragmentSize;
        this.nWorkers = nWorkers;
        this.outFile = outFile;
    }

    /**
     * Create Map and Reduce Tasks and assigns them to workers,
     * and writes the output for each string in descending order
     * by the rank of each file
     * @throws IOException in case of exceptions to reading/writing
     */
    public void execute() throws IOException {
        ForkJoinPool fjp = new ForkJoinPool(nWorkers);

        ConcurrentHashMap<Double, String> outMap  = new ConcurrentHashMap<>();

        doMap(fjp);

        doReduce(fjp, outMap);

        fjp.shutdown();

        Map<Double, String> sortedMap = computeResults(outMap);

        writeOutput(sortedMap, outFile);
    }

    /**
     * Write output to file
     * @param map the resulting map after the Reduce operation 
     *            sorted in descending order by rank
     * @param outName name of the output file
     * @throws IOException in case of exceptions to reading/writing
     */
    public void writeOutput(Map<Double, String> map, String outName) throws IOException {
        File out = new File(outName);
        FileOutputStream fos = new FileOutputStream(out);

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

        for (Map.Entry<Double, String> entry : map.entrySet()) {
            bw.write(entry.getValue());
            bw.newLine();
        }

        bw.close();
    }

     /**
     * Create the Map Tasks and assigns them to
     * workers to execute the Map operations
     * @param fjp the pool of workers that do the Map operations
     */
    public void doMap(ForkJoinPool fjp) {
        mapList = createMapTasks(fileList, fragmentSize);

        fjp.invoke(new MapRunnable(0, mapList));
    }

     /**
     * Create the Reduce Tasks and assigns them to
     * workers to execute the Reduce operations
     * @param fjp the pool of workers that do the Reduce operations
     */
    public void doReduce(ForkJoinPool fjp, ConcurrentHashMap<Double, String> outMap) {
        reduceList = createReduceTasks();

        fjp.invoke(new ReduceRunnable(0, reduceList, outMap));
    }

    /**
     * Sort the map resulted from the Reduce operations by rank of each file
     * @param outMap the map which contains the output strings for each file
     * @return the sorted map
     */
    public  Map<Double, String> computeResults(ConcurrentHashMap<Double, String> outMap) {
        Map<Double, String> sortedMap = new TreeMap<Double, String>(Collections.reverseOrder());

        sortedMap.putAll(outMap);

        return sortedMap;
    }

    /**
     * Parse the list of files and split each file in fragments by the
     * size given in the input and add the data to a Map Task
     * @param fileList the list of files to be processed
     * @param fragmentSize the default size of each fragment
     * @return the list of Map Tasks
     */
    public ArrayList<TaskMap> createMapTasks(ArrayList<String> fileList, long fragmentSize) {
        ArrayList<TaskMap> mapList = new ArrayList<>();

        int id = 0;
        // Traverse the fileList
        for (String fileName : fileList) {
            File file = new File(fileName);
            long fileSize = file.length();
            long offset = 0;
            long size;
            boolean endOfFile = false;

            // Split the file into fragments
            // until the endOfFile is reached
            do {
                if (offset + fragmentSize > fileSize) {
                    size = fileSize - offset;
                    endOfFile = true;
                } else {
                    size = fragmentSize;
                }

                // Add the data to a Map Task
                TaskMap task = new TaskMap(id, fileName, offset, size, endOfFile);
                mapList.add(task);

                id++;
                offset += fragmentSize;
            } while (!endOfFile);
        }

        return mapList;
    }

     /**
     * Create the Reduce Tasks by assigning them
     * the Map Tasks processed for each file
     * @return the list of Reduce Tasks
     */
    public ArrayList<TaskReduce> createReduceTasks() {
        ArrayList<TaskReduce> reduceList = new ArrayList<>();
        int id = 0;
        for (String file : fileList) {
            // Add to each Reduce Task the list of Map Tasks
            // that corresponds with the file
            List<TaskMap> taskMapsList = mapList.stream()
                    .filter(t -> t.getFileName().equals(file))
                    .collect(Collectors.toList());

            reduceList.add(new TaskReduce(id, file, taskMapsList));
            id++;
        }

        return reduceList;
    }
}
