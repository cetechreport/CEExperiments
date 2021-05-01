package Pessimistic;

import Common.Pair;
import Common.Query;
import Common.Topology;
import Common.Util;
import Graphflow.Constants;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Pessimistic {
    private Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    private Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();

    private Map<Integer, Long> maxOutDeg = new HashMap<>();
    private Map<Integer, Long> maxInDeg = new HashMap<>();

    // query topology -> decomVListString (defining decom topology) -> edge label seq -> count
    private List<Map<String, Map<String, Long>>> catalogue = new ArrayList<>();

    private void computeStat() {
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

        endTime = System.currentTimeMillis();
        System.out.println("Computing MaxDeg: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void persistStat(String destFile) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(destFile));

        for (Integer label : maxOutDeg.keySet()) {
            writer.write(Constants.FORWARD + "," + label + "," + maxOutDeg.get(label) + "\n");
        }

        for (Integer label : maxInDeg.keySet()) {
            writer.write(Constants.BACKWARD + "," + label + "," + maxInDeg.get(label) + "\n");
        }

        writer.close();
    }

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

            tripleString = csvReader.readLine();
        }

        endTime = System.currentTimeMillis();
        System.out.println("Graph Loading: " + ((endTime - startTime) / 1000.0) + " sec");

        csvReader.close();
    }

    private void readCatalogue(String catalogueFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader catalogueReader = new BufferedReader(new FileReader(catalogueFile));
        String[] info;
        String vList, labelSeq;
        String line = catalogueReader.readLine();
        while (null != line) {
            info = line.split(",");
            int queryType = Integer.parseInt(info[0]);
            while (catalogue.size() <= queryType) {
                catalogue.add(new HashMap<>());
            }

            vList = info[1];
            labelSeq = info[2];
            Long count = Long.parseLong(info[3]);
            catalogue.get(queryType).putIfAbsent(vList, new HashMap<>());
            catalogue.get(queryType).get(vList).put(labelSeq, count);

            line = catalogueReader.readLine();
        }
        catalogueReader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Loading Catalogue: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void readMaxDeg(String maxDegFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader reader = new BufferedReader(new FileReader(maxDegFile));
        String line = reader.readLine();
        while (line != null) {
            String[] info = line.split(",");
            Integer dir = Integer.parseInt(info[0]);
            if (dir.equals(Constants.FORWARD)) {
                maxOutDeg.put(Integer.parseInt(info[1]), Long.parseLong(info[2]));
            } else {
                maxInDeg.put(Integer.parseInt(info[1]), Long.parseLong(info[2]));
            }
            line = reader.readLine();
        }
        reader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Loading MaxDeg: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    /**
     *
     * @param topology
     * @param vListString
     * @return list of uncovered labels in the form of (dir, label)
     */
    private List<Pair<Integer, Integer>> getUncoveredLabels(Topology topology, String vListString) {
        List<Pair<Integer, Integer>> uncoveredLabels = new ArrayList<>();

        Pair<String, String> query = Util.topologyToVListAndLabelSeq(topology);
        Integer[] queryVList = Util.toVList(query.key);
        Integer[] queryLabelSeq = Util.toLabelSeq(query.value);

        String[] vList = vListString.split(";");
        Set<String> vListSet = new HashSet<>(Arrays.asList(vList));

        Set<Integer> coveredV = new HashSet<>(Arrays.asList(Util.toVList(vListString)));

        for (int i = 0; i < queryVList.length; i += 2) {
            Integer src = queryVList[i];
            Integer dest = queryVList[i + 1];

            if (vListSet.contains(src + "-" + dest)) continue;

            if (coveredV.contains(src)) {
                uncoveredLabels.add(new Pair<>(Constants.FORWARD, queryLabelSeq[i / 2]));
            } else {
                uncoveredLabels.add(new Pair<>(Constants.BACKWARD, queryLabelSeq[i / 2]));
            }
        }

        return uncoveredLabels;
    }

    private double computeSubmod(Topology topology, int patternType, Integer[][] decoms) {
        double submodBound = 1.0;

        for (Integer[] decom : decoms) {
            String labelSeq = Util.extractPath(topology, decom);
            String vListString = Util.toVListString(decom);
            submodBound *= Math.sqrt(catalogue.get(patternType).get(vListString).get(labelSeq));
        }

        return submodBound;
    }

    public double estimate(Query query, int patternType, boolean submod) {
        String startLabelSeq, vListString;
        List<Pair<Integer, Integer>> uncovered;

        List<Double> bounds = new ArrayList<>();

        for (Integer[] startingVList : Constants.DECOMPOSITIONS4[patternType]) {
            startLabelSeq = Util.extractPath(query.topology, startingVList);
            vListString = Util.toVListString(startingVList);
            double est;
            try {
                est = catalogue.get(patternType).get(vListString).get(startLabelSeq);
            } catch (NullPointerException e) {
                System.out.println(patternType);
                System.out.println(vListString);
                System.out.println(startLabelSeq);
                return -1;
            }

            uncovered = getUncoveredLabels(query.topology, vListString);

            // TODO: need to update covered vertices
            for (Pair<Integer, Integer> dirAndLabel : uncovered) {
                if (dirAndLabel.key.equals(Constants.FORWARD)) {
                    est *= maxOutDeg.get(dirAndLabel.value);
                } else {
                    est *= maxInDeg.get(dirAndLabel.value);
                }
            }

            bounds.add(est);
        }

        // with sub-modularity constraint
        // TODO: re-write this function, should be the product of sqrt of "decom for one formula"
        // TODO: for each constraint
        if (submod) {
            bounds.add(computeSubmod(
                query.topology, patternType, Constants.DECOMPOSITIONS4[patternType]));
        }

        // get the tightest bound
        Double minEst = Double.MAX_VALUE;
        for (Double est : bounds) {
            minEst = Math.min(est, minEst);
        }

        return minEst;
    }

    public Pessimistic() {}

    public Pessimistic(String catalogueFile, String maxDegFile) throws Exception {
        readCatalogue(catalogueFile);
        readMaxDeg(maxDegFile);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println("destFile: " + args[1]);
        System.out.println();

        Pessimistic pessimistic = new Pessimistic();
        pessimistic.readGraph(args[0]);
        pessimistic.computeStat();
        pessimistic.persistStat(args[1]);
    }
}
