
import java.util.ArrayList;
import java.util.HashMap;

public class TaskMap {
    /**
    * Map Task id
    */
    private int id;
    /**
    * Name of the file
    */
    private String fileName;
    /**
    * Offset from where the fragment begins
    */
    private long offset;
    /**
    * Size of the fragment
    */
    private long size;
    /**
    * States if this is the final fragment of the file
    */
    private boolean endOfFile;
    /**
    * States if the task was modified
    */
    private boolean modified;
    /**
    * Number of bytes that the fragment is modified with
    */
    private long shiftedBytes;
    /**
    * Map which contains the length of the words found in fragment
    * and the list of words which are that length
    */
    private HashMap<Integer, ArrayList<String>> fragmentMap;

    public TaskMap(int id, String fileName, long fragmentOffset, long fragmentSize, boolean endOfFile) {
        this.id = id;
        this.fileName = fileName;
        this.offset = fragmentOffset;
        this.size = fragmentSize;
        this.endOfFile = endOfFile;
        this.modified = false;
        this.shiftedBytes = 0;
    }

    public HashMap<Integer, ArrayList<String>> getFragmentMap() {
        return fragmentMap;
    }

    public void setFragmentMap(HashMap<Integer, ArrayList<String>> fragmentMap) {
        this.fragmentMap = fragmentMap;
    }

    public long getShiftedBytes() {
        return shiftedBytes;
    }

    public void setShifteBytes(long shiftedBytes) {
        this.shiftedBytes = shiftedBytes;
    }

    public int getId() {
        return id;
    }

    public String getFileName() {
        return fileName;
    }

    public long getOffset() {
        return offset;
    }

    public long getSize() {
        return size;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public boolean isEndOfFile() {
        return endOfFile;
    }

    public void setEndOfFile(boolean endOfFile) {
        this.endOfFile = endOfFile;
    }

    public boolean isModified() {
        return modified;
    }

    public void setModified(boolean modified) {
        this.modified = modified;
    }

    public void incrementShiftedBytes() {
        this.shiftedBytes++;
    }

    public void incrementSize() {
        this.size++;
    }
}
