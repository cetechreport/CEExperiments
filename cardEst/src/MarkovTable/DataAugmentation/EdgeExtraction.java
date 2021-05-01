package MarkovTable.DataAugmentation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EdgeExtraction {
    public static void main(String[] args) throws Exception {
        Map<Integer, Map<Integer, List<Integer>>> edge2src2dest = new HashMap<>();

        BufferedReader csvReader = new BufferedReader(new FileReader(args[0]));

        int[] tripleList;
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            tripleList = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt).toArray();

            // add the triple into the triple store
            edge2src2dest.putIfAbsent(tripleList[1], new HashMap<>());
            edge2src2dest.get(tripleList[1]).putIfAbsent(tripleList[0], new ArrayList<>());
            edge2src2dest.get(tripleList[1]).get(tripleList[0]).add(tripleList[2]);

            tripleString = csvReader.readLine();
        }

        int[] requestedPath = {
            Integer.parseInt(args[1]),
            Integer.parseInt(args[2]),
            Integer.parseInt(args[3]),
        };

        for (int requestedEdge : requestedPath) {
            for (int src : edge2src2dest.get(requestedEdge).keySet()) {

            }
        }
    }
}
