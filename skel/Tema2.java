import java.io.*;
import java.util.*;


public class Tema2 {

    /**
     * Receive a set of files to process and call the
     * coordinator to execute the Map-Reduce operations
     * @param args from command line
     * @throws IOException in case of exceptions to reading/writing
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Usage: Tema2 <workers> <in_file> <out_file>");
            return;
        }

        int nWorkers, fragmentSize = 0, nFiles;

        // Read input from command line
        nWorkers = Integer.parseInt(args[0]);
        String inFile = args[1];
        String outFile = args[2];

        ArrayList<String> fileList = new ArrayList<>();

        // Read data from inFile
        try {
            Scanner scanner = new Scanner(new File(inFile));

            fragmentSize = Integer.parseInt(scanner.nextLine());

            nFiles = Integer.parseInt(scanner.nextLine());

            for (int i = 0; i < nFiles; i++) {
                fileList.add(scanner.nextLine());
            }

            scanner.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        // Create coordinator that manages the threads
        Coordinator coordinator = new Coordinator(fileList, fragmentSize, nWorkers, outFile);

        // Execute the Map-Reduce operations
        coordinator.execute();
    }
}
