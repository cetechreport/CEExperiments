package MarkovTable.DataAugmentation;

import Common.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class EdgeDegrees {
    Map<Integer, Map<Integer, List<Integer>>> src2dest2edges;
    String csvFilePath;

    public EdgeDegrees(String csvFilePath) {
        this.csvFilePath = csvFilePath;
    }

    public void getAllTriples() throws Exception {
        src2dest2edges = new HashMap<>();

        BufferedReader csvReader = new BufferedReader(new FileReader(csvFilePath));

        int[] tripleList;
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            tripleList = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt).toArray();

            src2dest2edges.putIfAbsent(tripleList[0], new HashMap<>());
            src2dest2edges.get(tripleList[0]).putIfAbsent(tripleList[2], new ArrayList<>());
            src2dest2edges.get(tripleList[0]).get(tripleList[2]).add(tripleList[1]);

            tripleString = csvReader.readLine();
        }
    }

    public void removeHighHighDegreeNodesBridge(int numTopNodes)
        throws Exception {

        NodeDegrees nodeDegrees = new NodeDegrees(csvFilePath);

        PriorityQueue<Pair<Integer, Integer>> topInDegreeNodes =
            new PriorityQueue<>(1, new NodeDegComparator());
        for (Map.Entry<Integer, Integer> entry : nodeDegrees.node2inDeg.entrySet()) {
            if (topInDegreeNodes.size() < numTopNodes) {
                topInDegreeNodes.add(new Pair<>(entry.getKey(), entry.getValue()));
            } else {
                if (entry.getValue() > topInDegreeNodes.peek().getValue()) {
                    topInDegreeNodes.poll();
                    topInDegreeNodes.add(new Pair<>(entry.getKey(), entry.getValue()));
                }
            }
        }

        PriorityQueue<Pair<Integer, Integer>> topOutDegreeNodes =
            new PriorityQueue<>(1, new NodeDegComparator());
        for (Map.Entry<Integer, Integer> entry : nodeDegrees.node2outDeg.entrySet()) {
            if (topOutDegreeNodes.size() < numTopNodes) {
                topOutDegreeNodes.add(new Pair<>(entry.getKey(), entry.getValue()));
            } else {
                if (entry.getValue() > topOutDegreeNodes.peek().getValue()) {
                    topOutDegreeNodes.poll();
                    topOutDegreeNodes.add(new Pair<>(entry.getKey(), entry.getValue()));
                }
            }
        }

        int count = 0;
        List<Integer> removedEdges;
        for (Pair<Integer, Integer> nodeInDegree : new ArrayList<>(topInDegreeNodes)) {
            for (Pair<Integer, Integer> nodeOutDegree : new ArrayList<>(topOutDegreeNodes)) {
                if (src2dest2edges.containsKey(nodeInDegree.getKey())) {
                    // remove all the edges between a high in-deg node and a high out-deg node
                    removedEdges = src2dest2edges.get(nodeInDegree.getKey()).remove(nodeOutDegree.getKey());
                    if (removedEdges != null) {
                        count += removedEdges.size();
                    }
                }
            }
        }
        System.err.println(count + " edges removed");

        for (Integer src : src2dest2edges.keySet()) {
            for (Integer dest : src2dest2edges.get(src).keySet()) {
                for (Integer edge : src2dest2edges.get(src).get(dest)) {
                    System.out.println(src + "," + edge + "," + dest);
                }
            }
        }
    }

    class NodeDegComparator implements Comparator<Pair<Integer, Integer>> {
        @Override
        public int compare(Pair<Integer, Integer> nodeDeg1, Pair<Integer, Integer> nodeDeg2) {
            if (nodeDeg1.getValue() < nodeDeg2.getValue()) return -1;
            if (nodeDeg1.getValue() > nodeDeg2.getValue()) return 1;
            else return 0;
        }
    }
}
