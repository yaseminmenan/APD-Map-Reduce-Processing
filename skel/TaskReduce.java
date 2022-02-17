import java.util.List;

public class TaskReduce {
    /**
    * Reduce Task id
    */
    private int id;
    /**
    * Name of the file
    */
    private String fileName;
    /**
    * List of Map Tasks which correspond to the file
    */
    private List<TaskMap> taskMapList;

    public TaskReduce(int id, String fileName, List<TaskMap> taskMapList) {
        this.id = id;
        this.fileName = fileName;
        this.taskMapList = taskMapList;
    }

    public int getId() {
        return id;
    }

    public String getFileName() {
        return fileName;
    }

    public List<TaskMap> getTaskMapList() {
        return taskMapList;
    }
}
