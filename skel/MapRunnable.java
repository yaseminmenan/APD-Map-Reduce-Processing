
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.RecursiveAction;

/**
*  Class that executes the Map Operations
*  for each Map Task in the list using workers
*/
public class MapRunnable extends RecursiveAction {
    /**
    * Index of the Map Task 
    */
    private final int mapIndex;
    /**
    * List of Map Tasks
    */
    private final ArrayList<TaskMap> listMap;
    /**
    * Separators used to split the files
    */
    private final String separators = ";:/?~\\.,><`[]{}()!@#$%^&-_+'=*\"| \t\r\n";

    public MapRunnable(int mapIndex, ArrayList<TaskMap> listMap) {
        this.mapIndex = mapIndex;
        this.listMap = listMap;
    }

    @Override
    protected void compute() {

        TaskMap map = listMap.get(mapIndex);

        adjustFragment(map);

        List<MapRunnable> tasks = new ArrayList<>();

        if (mapIndex + 1 < listMap.size()) {
            MapRunnable t = new MapRunnable(mapIndex + 1, listMap);
            tasks.add(t);
            t.fork();
        }

        for (MapRunnable task : tasks) {
            task.join();
        }

    }

    /**
     * Check if the fragment begins or ends in the middle of a word
     * and adjusts the size and offset, and finally creates a hashmap
     * of all the words the fragment contains
     * @param map the Map Task that the operations are being applied on
     */
    public void adjustFragment(TaskMap map) {

        // Check if the map is the beginning of the file,
        // in which case it doesn't need to be adjusted
         if (map.getOffset() != 0) {
            
            TaskMap prevMap = listMap.get(mapIndex - 1);

            // Check if previous Task was modified
            // to adjust size and offset 
            if (prevMap.isModified()) {
                map.setOffset(map.getOffset() + prevMap.getShiftedBytes());
                map.setSize(map.getSize() - prevMap.getShiftedBytes());
            }
        }


        File file = new File(map.getFileName());

        try {
            // Check if it is the final Map Task for the file,
            // else verify it doesn't end on the middle of the word
            if (!map.isEndOfFile()) {

                String lastChar = getChar(file, (int) map.getSize(), map.getOffset());

                // If the final character is not a separator, then
                // check the next character until a separator is found
                while (!separators.contains(lastChar)) {
                    String nextChar = getChar(file, (int) (map.getSize() + 1), map.getOffset());

                    // If the next character is a separator
                    // then the fragment can be split
                    if (separators.contains(nextChar)) {
                        break;
                    }

                    // Set current Map Task as modified
                    // to alert the next task
                    map.setModified(true);
                    
                    // Used for the next task
                    map.incrementShiftedBytes();
                    // Adjust size of fragment
                    map.incrementSize();

                    // If the end of file is reached, break the loop
                    if (map.getOffset() + map.getSize() == file.length()) {
                        break;
                    }

                    lastChar = nextChar;
                }
            }

            // Convert the string and create the map
            if (map.getSize() > 0) {
                String fragmentString = getFragmentString(map);

                map.setFragmentMap(createWordMap(fragmentString));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Get the last character in the string
     * @param file from where the string is read
     * @param size of the string to be read
     * @param offset from which the string is read
     * @throws IOException in case of exceptions to reading/writing
     * @return the last character in the string
     */
    public String getChar(File file, int size, long offset) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file, "rw");

        byte[] magic = new byte[size];
        int lastByte = (int) size - 1;

        raf.seek(offset);
        raf.readFully(magic);
        raf.close();

        return String.valueOf((char) magic[lastByte]);
    }
    
    /**
     * Reads the fragment and converts it to a String
     * @param map which contains the size and offset to read the fragment
     * @throws IOException in case of exceptions to reading/writing
     * @return the String of the fragment read from file
     */
    public String getFragmentString(TaskMap map) throws IOException{
        byte[] fragmentByte = new byte[(int) (map.getSize())];
                
        RandomAccessFile raf = new RandomAccessFile(map.getFileName(), "rw");
        raf.seek(map.getOffset());
        raf.readFully(fragmentByte);

        return new String(fragmentByte);
    }

    /**
     * Splits the fragment into words using separators
     * and collects them in a map using each word's length
     * as key
     * @param fragmentString
     * @return the map with word length as key and the list of words as value
     */
    public HashMap<Integer, ArrayList<String>> createWordMap(String fragmentString) {
        String splitSeparators =
        ";|:|/|\\?|~|\\.|,|>|<|`|\\[|]|\\{|}|\\(|\\)|!|@|#|\\$|%\\^|&|-|_|\\+|'|=|\\*|\"|\\|| |\t|\r|\n";
        
        // Split the fragment into an array of Strings
        String[] arrOfStr = fragmentString.split(splitSeparators);

        HashMap<Integer, ArrayList<String>> fragmentMap = new HashMap<>();

        // Parse every string in fragment
        for (String word : arrOfStr){

            // If the string is not a separator
            if (!separators.contains(word)) {

                // Check if map contains the word's length
                if (fragmentMap.containsKey(word.length())) {

                    // Add the word to the list found at the key
                    fragmentMap.get(word.length()).add(word);
                } else {
                    // Create a list that contains the word
                    ArrayList<String> list = new ArrayList<>();
                    list.add(word);

                    // Insert the value and key to map
                    fragmentMap.put(word.length(), list);
                }
            }
         }

        return fragmentMap;
    }
}
