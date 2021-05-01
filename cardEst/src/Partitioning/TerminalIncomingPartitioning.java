package Partitioning;

import Common.Edge;
import Common.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TerminalIncomingPartitioning extends Partitioning {
    // label -> logPartitions
    private static Map<Integer, Map<Integer, Set<Edge>>> label2logPartitions = new HashMap<>();
    // terminal in_label -> logPartitionLabels
    private static Map<Integer, Set<Integer>> label2partitionLabels = new HashMap<>();

    // log_in_deg -> (in_labels, the_other_labels)
    private static Map<Integer, Pair<Set<Integer>, Set<Integer>>> logInDeg2labels = new HashMap<>();
    private static Map<Integer, Set<Edge>> logInDeg2buckets = new HashMap<>();

    private static String graphDataName;

    private static void saveAllPartitions() throws Exception {
        Map<Integer, Set<Integer>> logInDeg2allLabels = new HashMap<>();
        Map<Integer, String> logInDeg2inLabels = new HashMap<>();
        String inLabels;
        boolean first;

        for (int logInDeg : logInDeg2labels.keySet()) {
            Set<Integer> allLabels = new HashSet<>(logInDeg2labels.get(logInDeg).getKey());
            allLabels.addAll(logInDeg2labels.get(logInDeg).getValue());
            logInDeg2allLabels.put(logInDeg, allLabels);

            inLabels = "";
            first = true;
            for (int inLabel : logInDeg2labels.get(logInDeg).getKey()) {
                if (!first) {
                    inLabels += "-";
                }

                inLabels += inLabel;
                first = false;
            }
            logInDeg2inLabels.put(logInDeg, inLabels);
        }

        savePartitions(
            "./data/" + graphDataName + "_term_inc/",
            logInDeg2inLabels,
            logInDeg2buckets,
            logInDeg2allLabels
        );
    }

    private static void mergePartitionsForLabels() {
        for (int inLabel : label2logPartitions.keySet()) {
            Map<Integer, Set<Edge>> logPartition = label2logPartitions.get(inLabel);
            for (int logInDeg : logPartition.keySet()) {
                if (logInDeg2buckets.containsKey(logInDeg)) {
                    logInDeg2buckets.get(logInDeg).addAll(logPartition.get(logInDeg));
                    logInDeg2labels.get(logInDeg).getKey().add(inLabel);
                    logInDeg2labels.get(logInDeg).getValue().addAll(label2partitionLabels.get(inLabel));
                } else {
                    logInDeg2buckets.put(logInDeg, logPartition.get(logInDeg));
                    Set<Integer> inLabelSet = new HashSet<>();
                    inLabelSet.add(inLabel);
                    logInDeg2labels.put(
                        logInDeg,
                        new Pair<>(inLabelSet, label2partitionLabels.get(inLabel))
                    );
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String graphFilePath = args[0];
        int pathLength = Integer.parseInt(args[1]);
        graphDataName = args[2];

        // label->src->list_of_dest
        Map<Integer, Map<Integer, List<Integer>>> label2src2dest = new HashMap<>();
        // src->label->list_of_dest
        Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();

        int[] tripleList;
        BufferedReader csvReader = new BufferedReader(new FileReader(graphFilePath));
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            tripleList = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt).toArray();

            label2src2dest.putIfAbsent(tripleList[1], new HashMap<>());
            label2src2dest.get(tripleList[1]).putIfAbsent(tripleList[0], new ArrayList<>());
            label2src2dest.get(tripleList[1]).get(tripleList[0]).add(tripleList[2]);

            src2label2dest.putIfAbsent(tripleList[0], new HashMap<>());
            src2label2dest.get(tripleList[0]).putIfAbsent(tripleList[1], new ArrayList<>());
            src2label2dest.get(tripleList[0]).get(tripleList[1]).add(tripleList[2]);

            tripleString = csvReader.readLine();
        }

        for (int label : label2src2dest.keySet()) {
            System.out.print(".");

            // terminal -> in_deg
            Map<Integer, Integer> inDegree = new HashMap<>();
            // terminal -> set of labels contained
            Map<Integer, Set<Integer>> partitionLabels = new HashMap<>();
            // terminal -> edges
            Map<Integer, Set<Edge>> partitions = new HashMap<>();

            Map<Integer, List<Integer>> allFirstEdges = label2src2dest.get(label);
            for (Map.Entry<Integer, List<Integer>> firstSrc2Dests : allFirstEdges.entrySet()) {
                for (int firstDest : firstSrc2Dests.getValue()) {
                    Set<Edge> partition = new HashSet<>();
                    Set<Integer> labels = new HashSet<>();

                    partition.add(new Edge(firstSrc2Dests.getKey(), label, firstDest));

                    Set<Integer> prevDests = new HashSet<>();
                    prevDests.add(firstDest);

                    boolean hasLongEnoughPath = true;

                    for (int pathStep = 1; pathStep < pathLength; ++pathStep) {
                        Set<Integer> currentDests = new HashSet<>();
                        for (int prevDest : prevDests) {
                            if (!src2label2dest.containsKey(prevDest)) continue;

                            Map<Integer, List<Integer>> nextEdges = src2label2dest.get(prevDest);
                            for (int nextLabel : nextEdges.keySet()) {
                                for (int nextDest : nextEdges.get(nextLabel)) {
                                    partition.add(new Edge(prevDest, nextLabel, nextDest));
                                    labels.add(nextLabel);
                                    currentDests.add(nextDest);
                                }
                            }
                        }

                        prevDests = currentDests;

                        if (prevDests.isEmpty()) {
                            hasLongEnoughPath = false;
                            break;
                        }
                    }

                    if (hasLongEnoughPath) {
                        if (partitions.containsKey(firstDest)) {
                            partitions.get(firstDest).addAll(partition);
                            partitionLabels.get(firstDest).addAll(labels);
                        } else {
                            partitions.put(firstDest, partition);
                            partitionLabels.put(firstDest, labels);
                        }

                        inDegree.put(firstDest, inDegree.getOrDefault(firstDest, 0) + 1);
                    }
                }
            }

            Map<Integer, Set<Edge>> logInDeg2partitions = new HashMap<>();
            Set<Integer> labels = new HashSet<>();
            int logInDeg;
            for (int terminal : inDegree.keySet()) {
                logInDeg = (int) Math.floor(Math.log(inDegree.get(terminal)));
                logInDeg2partitions.putIfAbsent(
                    logInDeg,
                    new HashSet<>()
                );

                logInDeg2partitions.get(logInDeg).addAll(partitions.get(terminal));
                labels.addAll(partitionLabels.get(terminal));
            }

            label2logPartitions.put(label, logInDeg2partitions);
            label2partitionLabels.put(label, labels);
        }

        mergePartitionsForLabels();
        saveAllPartitions();
    }
}
