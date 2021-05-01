package Partitioning;

import Common.Edge;
import Common.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TerminalPartitioning extends Partitioning {
    private static List<Set<Integer>> id2labels = new ArrayList<>();
    private static List<Map<Integer, Set<Edge>>> id2buckets = new ArrayList<>();

    private static String graphDataName;

    private static void saveMergedPartitions() throws Exception {
        for (int i = 0; i < id2buckets.size(); ++i) {
            savePartitions(
                "./data/" + graphDataName + "_term/",
                Integer.toString(i),
                id2buckets.get(i),
                id2labels.get(i)
            );
        }
    }

    private static void mergeBucketsForPaths(Map<Path, Map<Integer, Set<Edge>>> path2buckets) {
        boolean merged;
        for (Path path : path2buckets.keySet()) {
            merged = false;
            for (int i = 0; i < id2labels.size(); ++i) {
                Set<Integer> pathEdges = new HashSet<>(path.getEdgeLabelList());
                if (Collections.disjoint(pathEdges, id2labels.get(i))) {
                    id2labels.get(i).addAll(pathEdges);
                    for (int logIn : path2buckets.get(path).keySet()) {
                        if (!id2buckets.get(i).containsKey(logIn)) {
                            id2buckets.get(i).put(logIn, new HashSet<>());
                        }
                        id2buckets.get(i).get(logIn).addAll(path2buckets.get(path).get(logIn));
                    }
                    merged = true;
                    break;
                }
            }

            if (!merged) {
                Set<Integer> newPart = new HashSet<>();
                newPart.addAll(path.getEdgeLabelList());
                id2labels.add(newPart);
                id2buckets.add(path2buckets.get(path));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String graphFilePath = args[0];
        graphDataName = args[3];
        String fileOrPath = args[1];
        String[][] targetPaths;
        if (fileOrPath.equals("path")) {
            targetPaths = new String[1][];
            targetPaths[0] = args[2].split(",");
        } else {
            BufferedReader pathsReader = new BufferedReader(new FileReader(args[2]));
            List<List<String>> paths = new ArrayList<>();
            String pathString = pathsReader.readLine();
            while (null != pathString) {
                paths.add(new ArrayList<>(Arrays.asList(pathString.trim().split(","))));
                pathString = pathsReader.readLine();
            }
            pathsReader.close();

            targetPaths = new String[paths.size()][];
            for (int i = 0; i < targetPaths.length; ++i) {
                targetPaths[i] = paths.get(i).toArray(new String[0]);
            }
        }

        int[][] queryPaths = new int[targetPaths.length][];
        for (int i = 0; i < queryPaths.length; ++i) {
            queryPaths[i] = Arrays.stream(targetPaths[i]).mapToInt(Integer::parseInt).toArray();
        }

        // label->src->list_of_dest
        Map<Integer, Map<Integer, List<Integer>>> label2src2dest = new HashMap<>();

        int[] tripleList;
        BufferedReader csvReader = new BufferedReader(new FileReader(graphFilePath));
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            tripleList = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt).toArray();

            label2src2dest.putIfAbsent(tripleList[1], new HashMap<>());
            label2src2dest.get(tripleList[1]).putIfAbsent(tripleList[0], new ArrayList<>());
            label2src2dest.get(tripleList[1]).get(tripleList[0]).add(tripleList[2]);

            tripleString = csvReader.readLine();
        }

        Map<Path, Map<Integer, Set<Edge>>> path2buckets = new HashMap<>();

        int progress = 0;
        for (int[] queryPath : queryPaths) {
            Map<Integer, Set<Edge>> partitions = new HashMap<>();
            // terminal vertex in-degree distribution
            Map<Integer, Integer> termVertex2InDeg = new HashMap<>();

            Map<Integer, List<Integer>> allFirstEdges = label2src2dest.get(queryPath[0]);
            for (Map.Entry<Integer, List<Integer>> firstSrc2Dests : allFirstEdges.entrySet()) {
                for (int firstDest : firstSrc2Dests.getValue()) {
                    Set<Edge> partition = new HashSet<>();
                    partition.add(new Edge(firstSrc2Dests.getKey(), queryPath[0], firstDest));

                    List<Integer> prevDests = new ArrayList<>();
                    prevDests.add(firstDest);

                    boolean hasPath = false;

                    for (int pathStep = 1; pathStep < queryPath.length; ++pathStep) {
                        Map<Integer, List<Integer>> nextEdges = label2src2dest.get
                            (queryPath[pathStep]);
                        hasPath = false;

                        List<Integer> currentDests = new ArrayList<>();
                        for (int prevDest : prevDests) {
                            if (!nextEdges.containsKey(prevDest)) continue;
                            hasPath = true;
                            for (int nextDestFromPrevDest : nextEdges.get(prevDest)) {
                                partition.add(new Edge(prevDest, queryPath[pathStep],
                                    nextDestFromPrevDest));
                                currentDests.add(nextDestFromPrevDest);
                            }
                        }
                        if (!hasPath) break;

                        prevDests = currentDests;
                    }

                    if (hasPath) {
                        if (partitions.containsKey(firstDest)) {
                            partitions.get(firstDest).addAll(partition);
                        } else {
                            partitions.put(firstDest, partition);
                        }
                        termVertex2InDeg.put(firstDest,
                            termVertex2InDeg.getOrDefault(firstDest, 0) + 1);
                    }
                }
            }

            Map<Integer, Set<Edge>> logInDeg2partitions = new HashMap<>();
            int logInDeg;
            for (int terminalVertex : termVertex2InDeg.keySet()) {
                logInDeg = (int) Math.floor(Math.log(termVertex2InDeg.get(terminalVertex)));
                logInDeg2partitions.putIfAbsent(
                    logInDeg,
                    new HashSet<>()
                );

                logInDeg2partitions.get(logInDeg).addAll(partitions.get(terminalVertex));
            }

            List<Integer> queryPathList = new ArrayList<>();
            for (int i = 0; i < queryPath.length; ++i) {
                queryPathList.add(queryPath[i]);
            }
            path2buckets.put(new Path(queryPathList), logInDeg2partitions);

            System.out.print(".");
            progress++;
            if (progress % 1000 == 0) System.out.println();
        }

        mergeBucketsForPaths(path2buckets);
        saveMergedPartitions();
    }
}
