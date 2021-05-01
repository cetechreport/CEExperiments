package Pessimistic;

import Graphflow.Constants;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StatsExtractor {
    private Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    private Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();

    private Map<Integer, Long> maxOutDeg = new HashMap<>();
    private Map<Integer, Long> maxInDeg = new HashMap<>();
    private Map<Integer, Long> labelCount = new HashMap<>();

    private void readGraph(String graphFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader csvReader = new BufferedReader(new FileReader(graphFile));
        int[] line;
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            line = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt).toArray();

            src2label2dest.putIfAbsent(line[0], new HashMap<>());
            src2label2dest.get(line[0]).putIfAbsent(line[1], new ArrayList<>());
            src2label2dest.get(line[0]).get(line[1]).add(line[2]);

            dest2label2src.putIfAbsent(line[2], new HashMap<>());
            dest2label2src.get(line[2]).putIfAbsent(line[1], new ArrayList<>());
            dest2label2src.get(line[2]).get(line[1]).add(line[0]);

            labelCount.put(line[1], labelCount.getOrDefault(line[1], 0L) + 1);

            tripleString = csvReader.readLine();
        }

        endTime = System.currentTimeMillis();
        System.out.println("Graph Loading: " + ((endTime - startTime) / 1000.0) + " sec");

        csvReader.close();
    }

    private void saveStats(String maxDegFile, String labelCountFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        for (Map<Integer, List<Integer>> label2dest : src2label2dest.values()) {
            for (Integer label : label2dest.keySet()) {
                maxOutDeg.put(label,
                    Math.max(maxOutDeg.getOrDefault(label, 0L), label2dest.get(label).size()));
            }
        }

        for (Map<Integer, List<Integer>> label2src : dest2label2src.values()) {
            for (Integer label : label2src.keySet()) {
                maxInDeg.put(label,
                    Math.max(maxInDeg.getOrDefault(label, 0L), label2src.get(label).size()));
            }
        }

        BufferedWriter writer = new BufferedWriter(new FileWriter(maxDegFile));
        for (Integer label : maxOutDeg.keySet()) {
            writer.write(Constants.FORWARD + "," + label + "," + maxOutDeg.get(label) + "\n");
        }

        for (Integer label : maxInDeg.keySet()) {
            writer.write(Constants.BACKWARD + "," + label + "," + maxInDeg.get(label) + "\n");
        }
        writer.close();

        writer = new BufferedWriter(new FileWriter(labelCountFile));
        for (Integer label : labelCount.keySet()) {
            writer.write(label + "," + labelCount.get(label) + "\n");
        }
        writer.close();

        endTime = System.currentTimeMillis();
        System.out.println("Saving Stats: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println("maxDegFile: " + args[1]);
        System.out.println("labelCountFile: " + args[2]);
        System.out.println();

        StatsExtractor extractor = new StatsExtractor();
        extractor.readGraph(args[0]);
        extractor.saveStats(args[1], args[2]);
    }
}
