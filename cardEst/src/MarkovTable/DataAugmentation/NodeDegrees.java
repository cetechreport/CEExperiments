package MarkovTable.DataAugmentation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class NodeDegrees {
    public Map<Integer, Integer> node2inDeg;
    public Map<Integer, Integer> node2outDeg;

    public NodeDegrees(String csvFilePath) throws Exception {
        this.node2inDeg = new HashMap<>();
        this.node2outDeg = new HashMap<>();

        BufferedReader csvReader = new BufferedReader(new FileReader(csvFilePath));
        int[] line;
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            line = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt).toArray();
            node2inDeg.put(line[2], node2inDeg.getOrDefault(line[2], 0) + 1);
            node2outDeg.put(line[0], node2outDeg.getOrDefault(line[0], 0) + 1);
            tripleString = csvReader.readLine();
        }
    }

    public void listDegrees() {
        System.out.println("NodeID,InDegree");
        for (Map.Entry<Integer, Integer> entry : node2inDeg.entrySet()) {
            System.out.println(entry.getKey() + "," + entry.getValue());
        }

        System.out.println("NodeID,OutDegree");
        for (Map.Entry<Integer, Integer> entry : node2outDeg.entrySet()) {
            System.out.println(entry.getKey() + "," + entry.getValue());
        }
    }
}
