package PartitionedEstimation;

import org.apache.commons.lang3.time.StopWatch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Partitioner {
    protected Map<Integer, Map<Integer, List<Integer>>> label2src2dest = new HashMap<>();
    protected Map<Integer, Map<Integer, List<Integer>>> label2dest2src = new HashMap<>();
    protected Map<Integer, Integer> numPartitions;

    /**
     *
     * @param vListString
     * @param labelSeqs
     * @return vListEdge -> (hashID of src and dest) OR deg bucket start and end -> subgraph
     */
    public abstract Map<String, Map<String, Subgraph>> partition(
        String vListString, List<String> labelSeqs);

    public abstract Map<String, Map<String, Subgraph>> partition(
        String vListString, List<String> labelSeqs, Map<Integer, Integer> numPartitions);

    public abstract void determineNumPartitions(String vListString, int budget);

    protected void readGraph(String graphFile) throws Exception {
        StopWatch watch = new StopWatch();
        watch.start();

        BufferedReader csvReader = new BufferedReader(new FileReader(graphFile));
        int[] line;
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            line = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt).toArray();

            label2src2dest.putIfAbsent(line[1], new HashMap<>());
            label2src2dest.get(line[1]).putIfAbsent(line[0], new ArrayList<>());
            label2src2dest.get(line[1]).get(line[0]).add(line[2]);

            label2dest2src.putIfAbsent(line[1], new HashMap<>());
            label2dest2src.get(line[1]).putIfAbsent(line[2], new ArrayList<>());
            label2dest2src.get(line[1]).get(line[2]).add(line[0]);

            tripleString = csvReader.readLine();
        }

        watch.stop();
        System.out.println("Graph Loading: " + (watch.getTime() / 1000.0) + " sec");

        csvReader.close();
    }

    protected Partitioner(String graphFile, String vList, int budget) {
        try {
            readGraph(graphFile);
            determineNumPartitions(vList, budget);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
