package MarkovTable;

import Common.Pair;
import Common.Path;

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
import java.util.Random;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;
import static java.lang.Math.toIntExact;

public class AcyclicMarkovTable {
    private Map<Integer, Map<Integer, List<Integer>>> src2label2dest;
    private Map<Integer, Map<Integer, List<Integer>>> dest2label2src;

    // centralVertex -> (half_path -> list of vertex paths)
    private Map<Integer, Map<Path, List<String>>> backwardPart13 = new HashMap<>();
    private Map<Integer, Map<Path, List<String>>> forwardPart124 = new HashMap<>();
    private Map<Integer, Map<Path, List<String>>> backwardPart2 = new HashMap<>();
    private Map<Integer, Map<Path, List<String>>> backwardPart4 = new HashMap<>();
    private Map<Integer, Map<Path, List<String>>> forwardPart3 = new HashMap<>();

    // due to the out-of-memory of backwardPart2, the following is used to see
    // if there are 1) too many MT entries, or 2) too large for each entry
    // centralVertex -> (half_path -> count of vertex paths)
//    private Map<Integer, Map<Path, Integer>> backwardCount2 = new HashMap<>();
//    private Map<Integer, Map<Path, Integer>> forwardCount24 = new HashMap<>();
//    private Map<Integer, Map<Path, Integer>> backwardCount4 = new HashMap<>();

    // path -> cumulative counts on each central vertex
    //   1) a->b->c->d
    //   2) a->b<-c->d
    //   3) a->b->c<-d
    //   4) a<-b->c->d
    private Map<Path, List<Pair<Integer, Long>>> path2cumCounts1 = new HashMap<>();
    private Map<Path, List<Pair<Integer, Long>>> path2cumCounts2 = new HashMap<>();
    private Map<Path, List<Pair<Integer, Long>>> path2cumCounts3 = new HashMap<>();
    private Map<Path, List<Pair<Integer, Long>>> path2cumCounts4 = new HashMap<>();

    // used in Forward/Backward
    private Map<Integer, List<String>> currentStepNodes2suffixNodes13 = new HashMap<>();
    private Map<Integer, List<String>> currentStepNodes2suffixEdges13 = new HashMap<>();
    private Map<Integer, List<String>> nextStepNodes2suffixNodes13 = new HashMap<>();
    private Map<Integer, List<String>> nextStepNodes2suffixEdges13 = new HashMap<>();
    private Map<Integer, List<String>> currentStepNodes2prefixNodes124 = new HashMap<>();
    private Map<Integer, List<String>> currentStepNodes2prefixEdges124 = new HashMap<>();
    private Map<Integer, List<String>> nextStepNodes2prefixNodes124 = new HashMap<>();
    private Map<Integer, List<String>> nextStepNodes2prefixEdges124 = new HashMap<>();
    private Map<Integer, List<String>> currentStepNodes2suffixNodes2 = new HashMap<>();
    private Map<Integer, List<String>> currentStepNodes2suffixEdges2 = new HashMap<>();
    private Map<Integer, List<String>> nextStepNodes2suffixNodes2 = new HashMap<>();
    private Map<Integer, List<String>> nextStepNodes2suffixEdges2 = new HashMap<>();
    private Map<Integer, List<String>> currentStepNodes2suffixNodes4 = new HashMap<>();
    private Map<Integer, List<String>> currentStepNodes2suffixEdges4 = new HashMap<>();
    private Map<Integer, List<String>> nextStepNodes2suffixNodes4 = new HashMap<>();
    private Map<Integer, List<String>> nextStepNodes2suffixEdges4 = new HashMap<>();
    private Map<Integer, List<String>> currentStepNodes2prefixNodes3 = new HashMap<>();
    private Map<Integer, List<String>> currentStepNodes2prefixEdges3 = new HashMap<>();
    private Map<Integer, List<String>> nextStepNodes2prefixNodes3 = new HashMap<>();
    private Map<Integer, List<String>> nextStepNodes2prefixEdges3 = new HashMap<>();
    private List<String> emptyStringList;

    private String mtFile;
    private String sampleFile;
    private int numSamples;
    private int mtLen;

    private final double RATIO = 0.01;

    // a->b->c->d
    private final int ENTRY_TYPE_1 = 1;
    // a->b<-c->d
    private final int ENTRY_TYPE_2 = 2;
    // a->b->c<-d
    private final int ENTRY_TYPE_3 = 3;
    // a<-b->c->d
    private final int ENTRY_TYPE_4 = 4;

    private String appendWithComma(Integer newValue, String suffix) {
        if (suffix.isEmpty()) return newValue.toString();

        return newValue + "," + suffix;
    }

    private String appendWithComma(String prefix, Integer newValue) {
        if (prefix.isEmpty()) return newValue.toString();

        return prefix + "," + newValue;
    }

    private List<Integer> csvStringToList(String csvString) {
        String[] values = csvString.split(",");
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < values.length; ++i) {
            result.add(Integer.parseInt(values[i]));
        }
        return result;
    }

    private void backward(int centralVertex) {
        currentStepNodes2suffixNodes13.clear();
        currentStepNodes2suffixEdges13.clear();
        currentStepNodes2suffixNodes2.clear();
        currentStepNodes2suffixEdges2.clear();
        currentStepNodes2suffixNodes4.clear();
        currentStepNodes2suffixEdges4.clear();

        boolean canBackward13 = dest2label2src.containsKey(centralVertex);
        boolean canBackward2 = src2label2dest.containsKey(centralVertex);
        boolean canBackward4 = dest2label2src.containsKey(centralVertex);

        int pathStep = mtLen / 2 + mtLen % 2;

        currentStepNodes2suffixNodes13.putIfAbsent(centralVertex, emptyStringList);
        currentStepNodes2suffixEdges13.putIfAbsent(centralVertex, emptyStringList);
        currentStepNodes2suffixNodes2.putIfAbsent(centralVertex, emptyStringList);
        currentStepNodes2suffixEdges2.putIfAbsent(centralVertex, emptyStringList);
        currentStepNodes2suffixNodes4.putIfAbsent(centralVertex, emptyStringList);
        currentStepNodes2suffixEdges4.putIfAbsent(centralVertex, emptyStringList);

        nextStepNodes2suffixNodes13.clear();
        nextStepNodes2suffixEdges13.clear();
        nextStepNodes2suffixNodes2.clear();
        nextStepNodes2suffixEdges2.clear();
        nextStepNodes2suffixNodes4.clear();
        nextStepNodes2suffixEdges4.clear();
        String vertexPath, edgePath;

        Map<Integer, Map<Integer, List<Integer>>> v2label2v;

        // going backward
        while (pathStep > 0) {
            if (canBackward13) {
                for (Integer dest : currentStepNodes2suffixNodes13.keySet()) {
                    if (dest2label2src.containsKey(dest)) {
                        for (Integer nextLabel : dest2label2src.get(dest).keySet()) {
                            for (Integer src : dest2label2src.get(dest).get(nextLabel)) {
                                for (String suffixNodes : currentStepNodes2suffixNodes13.get(dest)) {
                                    vertexPath = appendWithComma(dest, suffixNodes);
                                    nextStepNodes2suffixNodes13.putIfAbsent(src, new ArrayList<>());
                                    nextStepNodes2suffixNodes13.get(src).add(vertexPath);
                                }

                                for (String suffixEdges : currentStepNodes2suffixEdges13.get
                                    (dest)) {
                                    edgePath = appendWithComma(nextLabel, suffixEdges);
                                    nextStepNodes2suffixEdges13.putIfAbsent(src, new ArrayList<>());
                                    nextStepNodes2suffixEdges13.get(src).add(edgePath);
                                }
                            }
                        }
                    }
                }

                currentStepNodes2suffixNodes13 = new HashMap<>(nextStepNodes2suffixNodes13);
                currentStepNodes2suffixEdges13 = new HashMap<>(nextStepNodes2suffixEdges13);
                nextStepNodes2suffixNodes13.clear();
                nextStepNodes2suffixEdges13.clear();
            }

            if (canBackward2) {
                v2label2v = pathStep % 2 == 0 ? src2label2dest : dest2label2src;
                for (Integer dest : currentStepNodes2suffixNodes2.keySet()) {
                    if (v2label2v.containsKey(dest)) {
                        for (Integer nextLabel : v2label2v.get(dest).keySet()) {
                            for (Integer src : v2label2v.get(dest).get(nextLabel)) {
                                for (String suffixNodes : currentStepNodes2suffixNodes2.get(dest)) {
                                    vertexPath = appendWithComma(dest, suffixNodes);
                                    nextStepNodes2suffixNodes2.putIfAbsent(src, new ArrayList<>());
                                    nextStepNodes2suffixNodes2.get(src).add(vertexPath);
                                }

                                for (String suffixEdges : currentStepNodes2suffixEdges2.get(dest)) {
                                    edgePath = appendWithComma(nextLabel, suffixEdges);
                                    nextStepNodes2suffixEdges2.putIfAbsent(src, new ArrayList<>());
                                    nextStepNodes2suffixEdges2.get(src).add(edgePath);
                                }
                            }
                        }
                    }
                }

                currentStepNodes2suffixNodes2 = new HashMap<>(nextStepNodes2suffixNodes2);
                currentStepNodes2suffixEdges2 = new HashMap<>(nextStepNodes2suffixEdges2);
                nextStepNodes2suffixNodes2.clear();
                nextStepNodes2suffixEdges2.clear();
            }

            if (mtLen >= 3 && canBackward4) {
                v2label2v = pathStep % 2 == 0 ? dest2label2src : src2label2dest;
                for (Integer dest : currentStepNodes2suffixNodes4.keySet()) {
                    if (v2label2v.containsKey(dest)) {
                        for (Integer nextLabel : v2label2v.get(dest).keySet()) {
                            for (Integer src : v2label2v.get(dest).get(nextLabel)) {
                                for (String suffixNodes : currentStepNodes2suffixNodes4.get(dest)) {
                                    vertexPath = appendWithComma(dest, suffixNodes);
                                    nextStepNodes2suffixNodes4.putIfAbsent(src, new ArrayList<>());
                                    nextStepNodes2suffixNodes4.get(src).add(vertexPath);
                                }

                                for (String suffixEdges : currentStepNodes2suffixEdges4.get(dest)) {
                                    edgePath = appendWithComma(nextLabel, suffixEdges);
                                    nextStepNodes2suffixEdges4.putIfAbsent(src, new ArrayList<>());
                                    nextStepNodes2suffixEdges4.get(src).add(edgePath);
                                }
                            }
                        }
                    }
                }

                currentStepNodes2suffixNodes4 = new HashMap<>(nextStepNodes2suffixNodes4);
                currentStepNodes2suffixEdges4 = new HashMap<>(nextStepNodes2suffixEdges4);
                nextStepNodes2suffixNodes4.clear();
                nextStepNodes2suffixEdges4.clear();
            }

            pathStep--;
        }

        Path prefixPath;
        for (Integer src : currentStepNodes2suffixNodes13.keySet()) {
            for (int i = 0; i < currentStepNodes2suffixNodes13.get(src).size(); ++i) {
                String prefixNodes = currentStepNodes2suffixNodes13.get(src).get(i);
                if (prefixNodes.isEmpty()) continue;

                prefixNodes = appendWithComma(src, prefixNodes);
                String prefixEdges = currentStepNodes2suffixEdges13.get(src).get(i);
                prefixPath = new Path(prefixEdges);
                backwardPart13.putIfAbsent(centralVertex, new HashMap<>());
                backwardPart13.get(centralVertex).putIfAbsent(prefixPath, new ArrayList<>());
                backwardPart13.get(centralVertex).get(prefixPath).add(prefixNodes);
            }
        }

        for (Integer src : currentStepNodes2suffixNodes2.keySet()) {
            for (int i = 0; i < currentStepNodes2suffixNodes2.get(src).size(); ++i) {
                String prefixNodes = currentStepNodes2suffixNodes2.get(src).get(i);
                if (prefixNodes.isEmpty()) continue;

                prefixNodes = appendWithComma(src, prefixNodes);
                String prefixEdges = currentStepNodes2suffixEdges2.get(src).get(i);
                prefixPath = new Path(prefixEdges);
                backwardPart2.putIfAbsent(centralVertex, new HashMap<>());
                backwardPart2.get(centralVertex).putIfAbsent(prefixPath, new ArrayList<>());
                backwardPart2.get(centralVertex).get(prefixPath).add(prefixNodes);
//                backwardCount2.putIfAbsent(centralVertex, new HashMap<>());
//                backwardCount2.get(centralVertex).put(
//                    prefixPath, backwardCount2.get(centralVertex).getOrDefault(prefixPath, 0) + 1
//                );
            }
        }

        for (Integer src : currentStepNodes2suffixNodes4.keySet()) {
            for (int i = 0; i < currentStepNodes2suffixNodes4.get(src).size(); ++i) {
                String prefixNodes = currentStepNodes2suffixNodes4.get(src).get(i);
                if (prefixNodes.isEmpty()) continue;

                prefixNodes = appendWithComma(src, prefixNodes);
                String prefixEdges = currentStepNodes2suffixEdges4.get(src).get(i);
                prefixPath = new Path(prefixEdges);
                backwardPart4.putIfAbsent(centralVertex, new HashMap<>());
                backwardPart4.get(centralVertex).putIfAbsent(prefixPath, new ArrayList<>());
                backwardPart4.get(centralVertex).get(prefixPath).add(prefixNodes);
//                backwardCount4.putIfAbsent(centralVertex, new HashMap<>());
//                backwardCount4.get(centralVertex).put(
//                    prefixPath, backwardCount4.get(centralVertex).getOrDefault(prefixPath, 0) + 1
//                );
            }
        }
    }

    private void forward(int centralVertex) {
        currentStepNodes2prefixNodes124.clear();
        currentStepNodes2prefixEdges124.clear();
        currentStepNodes2prefixNodes3.clear();
        currentStepNodes2prefixEdges3.clear();

        boolean canForward124 = src2label2dest.containsKey(centralVertex);
        boolean canForward3 = dest2label2src.containsKey(centralVertex);

        int pathStep = mtLen / 2;

        currentStepNodes2prefixNodes124.putIfAbsent(centralVertex, emptyStringList);
        currentStepNodes2prefixEdges124.putIfAbsent(centralVertex, emptyStringList);
        currentStepNodes2prefixNodes3.putIfAbsent(centralVertex, emptyStringList);
        currentStepNodes2prefixEdges3.putIfAbsent(centralVertex, emptyStringList);

        nextStepNodes2prefixNodes124.clear();
        nextStepNodes2prefixEdges124.clear();
        nextStepNodes2prefixNodes3.clear();
        nextStepNodes2prefixEdges3.clear();
        String vertexPath, edgePath;

        // going forward
        while (pathStep > 0) {
            if (canForward124) {
                for (Integer src : currentStepNodes2prefixNodes124.keySet()) {
                    if (src2label2dest.containsKey(src)) {
                        for (Integer nextLabel : src2label2dest.get(src).keySet()) {
                            for (Integer dest : src2label2dest.get(src).get(nextLabel)) {
                                for (String prefixNodes : currentStepNodes2prefixNodes124.get(src)) {
                                    vertexPath = appendWithComma(prefixNodes, src);
                                    nextStepNodes2prefixNodes124.putIfAbsent(dest, new ArrayList<>());
                                    nextStepNodes2prefixNodes124.get(dest).add(vertexPath);
                                }

                                for (String prefixEdges : currentStepNodes2prefixEdges124.get(src)) {
                                    edgePath = appendWithComma(prefixEdges, nextLabel);
                                    nextStepNodes2prefixEdges124.putIfAbsent(dest, new ArrayList<>());
                                    nextStepNodes2prefixEdges124.get(dest).add(edgePath);
                                }
                            }
                        }
                    }
                }

                currentStepNodes2prefixNodes124 = new HashMap<>(nextStepNodes2prefixNodes124);
                currentStepNodes2prefixEdges124 = new HashMap<>(nextStepNodes2prefixEdges124);
                nextStepNodes2prefixNodes124.clear();
                nextStepNodes2prefixEdges124.clear();
            }

            if (canForward3) {
                for (Integer src : currentStepNodes2prefixNodes3.keySet()) {
                    if (dest2label2src.containsKey(src)) {
                        for (Integer nextLabel : dest2label2src.get(src).keySet()) {
                            for (Integer dest : dest2label2src.get(src).get(nextLabel)) {
                                for (String prefixNodes : currentStepNodes2prefixNodes3.get(src)) {
                                    vertexPath = appendWithComma(prefixNodes, src);
                                    nextStepNodes2prefixNodes3.putIfAbsent(dest, new ArrayList<>());
                                    nextStepNodes2prefixNodes3.get(dest).add(vertexPath);
                                }

                                for (String prefixEdges : currentStepNodes2prefixEdges3.get(src)) {
                                    edgePath = appendWithComma(prefixEdges, nextLabel);
                                    nextStepNodes2prefixEdges3.putIfAbsent(dest, new ArrayList<>());
                                    nextStepNodes2prefixEdges3.get(dest).add(edgePath);
                                }
                            }
                        }
                    }
                }

                currentStepNodes2prefixNodes3 = new HashMap<>(nextStepNodes2prefixNodes3);
                currentStepNodes2prefixEdges3 = new HashMap<>(nextStepNodes2prefixEdges3);
                nextStepNodes2prefixNodes3.clear();
                nextStepNodes2prefixEdges3.clear();
            }

            pathStep--;
        }

        Path suffixPath;
        for (Integer dest : currentStepNodes2prefixNodes124.keySet()) {
            for (int i = 0; i < currentStepNodes2prefixNodes124.get(dest).size(); ++i) {
                String suffixNodes = currentStepNodes2prefixNodes124.get(dest).get(i);
                if (suffixNodes.isEmpty()) continue;

                suffixNodes = appendWithComma(suffixNodes, dest);
                String suffixEdges = currentStepNodes2prefixEdges124.get(dest).get(i);
                suffixPath = new Path(suffixEdges);
                forwardPart124.putIfAbsent(centralVertex, new HashMap<>());
                forwardPart124.get(centralVertex).putIfAbsent(suffixPath, new ArrayList<>());
                forwardPart124.get(centralVertex).get(suffixPath).add(suffixNodes);
//                forwardCount24.putIfAbsent(centralVertex, new HashMap<>());
//                forwardCount24.get(centralVertex).put(
//                    suffixPath, forwardCount24.get(centralVertex).getOrDefault(suffixPath, 0) + 1
//                );
            }
        }

        for (Integer dest : currentStepNodes2prefixNodes3.keySet()) {
            for (int i = 0; i < currentStepNodes2prefixNodes3.get(dest).size(); ++i) {
                String suffixNodes = currentStepNodes2prefixNodes3.get(dest).get(i);
                if (suffixNodes.isEmpty()) continue;

                suffixNodes = appendWithComma(suffixNodes, dest);
                String suffixEdges = currentStepNodes2prefixEdges3.get(dest).get(i);
                suffixPath = new Path(suffixEdges);
                forwardPart3.putIfAbsent(centralVertex, new HashMap<>());
                forwardPart3.get(centralVertex).putIfAbsent(suffixPath, new ArrayList<>());
                forwardPart3.get(centralVertex).get(suffixPath).add(suffixNodes);
            }
        }
    }

    private void joinCount(int centralVertex, Path prefix, Path suffix, int entryType) {
        Long prevCumCount, cumCount;
        List<Pair<Integer, Long>> cumCounts;

        Path path = new Path(prefix);
        path.appendAll(suffix.getEdgeLabelList());

        Map<Integer, Map<Path, List<String>>> backwardPart;
        Map<Integer, Map<Path, List<String>>> forwardPart;
        Map<Path, List<Pair<Integer, Long>>> path2cumCounts;

        // TODO: to be removed
//        Map<Integer, Map<Path, Integer>> backwardCounts = new HashMap<>();
//        Map<Integer, Map<Path, Integer>> forwardCounts = new HashMap<>();

        switch (entryType) {
            case ENTRY_TYPE_1:
                forwardPart = forwardPart124;
                backwardPart = backwardPart13;
                path2cumCounts = path2cumCounts1;
                break;
            case ENTRY_TYPE_2:
                forwardPart = forwardPart124;
                backwardPart = backwardPart2;
                path2cumCounts = path2cumCounts2;

//                backwardCounts = backwardCount2;
//                forwardCounts = forwardCount24;
                break;
            case ENTRY_TYPE_3:
                forwardPart = forwardPart3;
                backwardPart = backwardPart13;
                path2cumCounts = path2cumCounts3;
                break;
            case ENTRY_TYPE_4:
                forwardPart = forwardPart124;
                backwardPart = backwardPart4;
                path2cumCounts = path2cumCounts4;

//                backwardCounts = backwardCount4;
//                forwardCounts = forwardCount24;
                break;
            default:
                forwardPart = new HashMap<>();
                backwardPart = new HashMap<>();
                path2cumCounts = new HashMap<>();
        }

        long backwardCount = backwardPart.get(centralVertex).get(prefix).size();
        long forwardCount = forwardPart.get(centralVertex).get(suffix).size();
//        int backwardCount = backwardCounts.get(centralVertex).get(prefix);
//        int forwardCount = forwardCounts.get(centralVertex).get(suffix);

        if (path2cumCounts.containsKey(path)) {
            cumCounts = path2cumCounts.get(path);
            prevCumCount = cumCounts.get(cumCounts.size() - 1).value;
            cumCount = prevCumCount + (backwardCount * forwardCount);
            path2cumCounts.get(path).add(new Pair<>(centralVertex, cumCount));
        } else {
            path2cumCounts.put(path, new ArrayList<>());
            cumCount = backwardCount * forwardCount;
            path2cumCounts.get(path).add(new Pair<>(centralVertex, cumCount));
        }
    }

    private void joinTogether() {
        long backwardCount;
        List<Pair<Integer, Long>> cumCounts;
        Long prevCumCount, cumCount;

        if (forwardPart124.isEmpty() && forwardPart3.isEmpty()) {
            for (Integer centralVertex : backwardPart13.keySet()) {
                for (Path prefix : backwardPart13.get(centralVertex).keySet()) {
                    backwardCount = backwardPart13.get(centralVertex).get(prefix).size();
                    if (path2cumCounts1.containsKey(prefix)) {
                        cumCounts = path2cumCounts1.get(prefix);
                        prevCumCount = cumCounts.get(cumCounts.size() - 1).value;
                        cumCount = prevCumCount + backwardCount;
                        path2cumCounts1.get(prefix).add(new Pair<>(centralVertex, cumCount));
                    } else {
                        path2cumCounts1.put(prefix, new ArrayList<>());
                        path2cumCounts1.get(prefix).add(new Pair<>(centralVertex, backwardCount));
                    }
                }
            }

            return;
        }

        for (int centralVertex : backwardPart13.keySet()) {
            for (Path prefix : backwardPart13.get(centralVertex).keySet()) {
                if (forwardPart124.containsKey(centralVertex)) {
                    for (Path suffix : forwardPart124.get(centralVertex).keySet()) {
                        joinCount(centralVertex, prefix, suffix, ENTRY_TYPE_1);
                    }
                }

                if (forwardPart3.containsKey(centralVertex)) {
                    for (Path suffix : forwardPart3.get(centralVertex).keySet()) {
                        joinCount(centralVertex, prefix, suffix, ENTRY_TYPE_3);
                    }
                }
            }
        }

        for (int centralVertex : backwardPart2.keySet()) {
            for (Path prefix : backwardPart2.get(centralVertex).keySet()) {
                if (forwardPart124.containsKey(centralVertex)) {
                    for (Path suffix : forwardPart124.get(centralVertex).keySet()) {
                        joinCount(centralVertex, prefix, suffix, ENTRY_TYPE_2);
                    }
                }
            }
        }

        for (int centralVertex : backwardPart4.keySet()) {
            for (Path prefix : backwardPart4.get(centralVertex).keySet()) {
                if (forwardPart124.containsKey(centralVertex)) {
                    for (Path suffix : forwardPart124.get(centralVertex).keySet()) {
                        joinCount(centralVertex, prefix, suffix, ENTRY_TYPE_4);
                    }
                }
            }
        }


        // TODO: to be removed
//        for (int centralVertex : backwardCount2.keySet()) {
//            for (Path prefix : backwardCount2.get(centralVertex).keySet()) {
//                if (forwardCount24.containsKey(centralVertex)) {
//                    for (Path suffix : forwardCount24.get(centralVertex).keySet()) {
//                        joinCount(centralVertex, prefix, suffix, ENTRY_TYPE_2);
//                    }
//                }
//            }
//        }
//        for (int centralVertex : backwardCount4.keySet()) {
//            for (Path prefix : backwardCount4.get(centralVertex).keySet()) {
//                if (forwardCount24.containsKey(centralVertex)) {
//                    for (Path suffix : forwardCount24.get(centralVertex).keySet()) {
//                        joinCount(centralVertex, prefix, suffix, ENTRY_TYPE_4);
//                    }
//                }
//            }
//        }
    }

    private String getPathInstance(Path path, int centralVertex, int offset, int entryType) {
        Map<Integer, Map<Path, List<String>>> backwardPart;
        Map<Integer, Map<Path, List<String>>> forwardPart;
        switch (entryType) {
            case ENTRY_TYPE_1:
                backwardPart = backwardPart13;
                forwardPart = forwardPart124;
                break;
            case ENTRY_TYPE_2:
                backwardPart = backwardPart2;
                forwardPart = forwardPart124;
                break;
            case ENTRY_TYPE_3:
                backwardPart = backwardPart13;
                forwardPart = forwardPart3;
                break;
            case ENTRY_TYPE_4:
                backwardPart = backwardPart4;
                forwardPart = forwardPart124;
                break;
            default:
                backwardPart = new HashMap<>();
                forwardPart = new HashMap<>();
                break;
        }


        if (1 == path.length()) {
            return backwardPart.get(centralVertex).get(path).get(offset - 1);
        }

        List<Integer> edgeLabelList = path.getEdgeLabelList();
        int boundary = path.length() / 2 + path.length() % 2;
        Path backwardPath = new Path(edgeLabelList.subList(0, boundary));
        Path forwardPath = new Path(edgeLabelList.subList(boundary, path.length()));

        int forwardSize = forwardPart.get(centralVertex).get(forwardPath).size();

        int backwardIndex = (offset - 1) / forwardSize;
        int forwardIndex = (offset - 1) % forwardSize;

        String backwardVertexList =
            backwardPart.get(centralVertex).get(backwardPath).get(backwardIndex);
        String forwardVertexList =
            forwardPart.get(centralVertex).get(forwardPath).get(forwardIndex);

        String sampledInstance = backwardVertexList;
        String[] forwardVListArray = forwardVertexList.split(",");
        sampledInstance += "," +
            String.join(",", Arrays.copyOfRange(forwardVListArray, 1, forwardVListArray.length));

        return sampledInstance;
    }

    private String materializePathInstance(
        Path path, List<Pair<Integer, Long>> cumCounts, long sampledIndex, int entryType) {

        Pair<Integer, Long> centralAndPathCounts, prevCentralAndPathCounts, nextCentralAndPathCounts;

        int low = 0;
        int high = cumCounts.size() - 1;
        int middlePairIndex = (low + high) / 2;
        while (middlePairIndex > 0) {
            prevCentralAndPathCounts = cumCounts.get(middlePairIndex - 1);
            centralAndPathCounts = cumCounts.get(middlePairIndex);

            if (sampledIndex > prevCentralAndPathCounts.value &&
                sampledIndex <= centralAndPathCounts.value) {
                return getPathInstance(
                    path, centralAndPathCounts.key,
                    toIntExact(sampledIndex - prevCentralAndPathCounts.value), entryType
                );
            } else if (sampledIndex > centralAndPathCounts.value) {
                nextCentralAndPathCounts = cumCounts.get(middlePairIndex + 1);
                if (sampledIndex <= nextCentralAndPathCounts.value) {
                    return getPathInstance(
                        path, nextCentralAndPathCounts.key,
                        toIntExact(sampledIndex - centralAndPathCounts.value), entryType
                    );
                }

                low = middlePairIndex;
                middlePairIndex = (middlePairIndex + high) / 2;
            } else if (sampledIndex <= prevCentralAndPathCounts.value) {
                high = middlePairIndex;
                middlePairIndex = (low + middlePairIndex) / 2;
            }
        }

        centralAndPathCounts = cumCounts.get(middlePairIndex);
        if (sampledIndex > centralAndPathCounts.value) {
            prevCentralAndPathCounts = centralAndPathCounts;
            centralAndPathCounts = cumCounts.get(middlePairIndex + 1);
            return getPathInstance(
                path, centralAndPathCounts.key,
                toIntExact(sampledIndex - prevCentralAndPathCounts.value), entryType
            );
        }
        return getPathInstance(path, centralAndPathCounts.key, toIntExact(sampledIndex), entryType);
    }

    private String samplesToString(List<String> samples) {
        StringJoiner sj = new StringJoiner(",");
        for (String sample : samples) {
            sj.add(String.join("->", sample.split(",")));
        }
        return sj.toString();
    }

    private void saveMTAndSampling(int entryType) throws Exception {
        Long total, sampledIndex;
        List<Pair<Integer, Long>> cumCounts;
//        Random random = new Random(0);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int multiplier = (int) (1 / RATIO);

        List<String> samples = new ArrayList<>();
        Set<Long> sampledIndices = new HashSet<>();

        Map<Path, List<Pair<Integer, Long>>> path2cumCounts;
        String mtFileOfType, sampleFileOfType;
        switch (entryType) {
            case ENTRY_TYPE_1:
                path2cumCounts = path2cumCounts1;
                mtFileOfType = mtFile + "_type1";
                sampleFileOfType = sampleFile + "_type1";
                break;
            case ENTRY_TYPE_2:
                path2cumCounts = path2cumCounts2;
                mtFileOfType = mtFile + "_type2";
                sampleFileOfType = sampleFile + "_type2";
                break;
            case ENTRY_TYPE_3:
                path2cumCounts = path2cumCounts3;
                mtFileOfType = mtFile + "_type3";
                sampleFileOfType = sampleFile + "_type3";
                break;
            case ENTRY_TYPE_4:
                path2cumCounts = path2cumCounts4;
                mtFileOfType = mtFile + "_type4";
                sampleFileOfType = sampleFile + "_type4";
                break;
            default:
                path2cumCounts = new HashMap<>();
                mtFileOfType = "";
                sampleFileOfType = "";
        }

        BufferedWriter mtWriter = new BufferedWriter(new FileWriter(mtFileOfType));
        BufferedWriter sampleWriter = new BufferedWriter(new FileWriter(sampleFileOfType));

        int numPaths = path2cumCounts.size();
        double progress = 0;

        for (Path path : path2cumCounts.keySet()) {
            samples.clear();
            sampledIndices.clear();

            cumCounts = path2cumCounts.get(path);
            total = cumCounts.get(cumCounts.size() - 1).value;

            // write to MT file
            mtWriter.write(path.toSimpleString() + "," + (total * multiplier) + "\n");

            while (samples.size() < total && samples.size() < numSamples) {
                sampledIndex = random.nextLong(total) + 1;
                while (sampledIndices.contains(sampledIndex)) {
                    sampledIndex = random.nextLong(total) + 1;
                }
                sampledIndices.add(sampledIndex);

                samples.add(materializePathInstance(path, cumCounts, sampledIndex, entryType));
            }

            sampleWriter.write(path.toSimpleString() + "," + samplesToString(samples) + "\n");

            progress += 100.0 / numPaths;
            System.out.print("\rMT & Sampling: " + (int) progress + "%");
        }
        mtWriter.close();
        sampleWriter.close();
    }

    private Set<Integer> getSamples(List<Integer> candidates, int sampleSize) {
        Set<Integer> sampledIndices = new HashSet<>();
        Random random = new Random(0);

        Set<Integer> samples = new HashSet<>();

        if (candidates.size() > sampleSize) {
            int sampledIndex;
            while (samples.size() < sampleSize) {
                sampledIndex = random.nextInt(candidates.size());
                while (sampledIndices.contains(sampledIndex)) {
                    sampledIndex = random.nextInt(candidates.size());
                }
                sampledIndices.add(sampledIndex);

                samples.add(candidates.get(sampledIndex));
            }
        } else {
            samples.addAll(candidates);
        }

        return samples;
    }

    public void build() throws Exception {
        Set<Integer> allVertices = new HashSet<>(src2label2dest.keySet());
        allVertices.addAll(dest2label2src.keySet());

        Set<Integer> sampledVertices = getSamples(
            new ArrayList<>(allVertices), (int) (allVertices.size() * RATIO)
        );

        int numVertices = sampledVertices.size();
        double progress = 0;
        double prevProgress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (int vertex : sampledVertices) {
            progress += 100.0 / numVertices;
            if (progress - prevProgress > 1) {
                System.out.print("\rForward: " + (int) progress + "%");
                prevProgress = progress;
            }

            forward(vertex);
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nForward: " + ((endTime - startTime) / 1000.0) + " sec");

        progress = 0;
        prevProgress = 0;
        startTime = System.currentTimeMillis();
        for (int vertex : sampledVertices) {
            progress += 100.0 / numVertices;
            if (progress - prevProgress > 1) {
                System.out.print("\rBackward: " + (int) progress + "%");
                prevProgress = progress;
            }

            backward(vertex);
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nBackward: " + ((endTime - startTime) / 1000.0) + " sec");

//        saveBackwardForwardParts();

        src2label2dest = null;
        dest2label2src = null;

        System.out.print("Joining forward and backward paths");
        startTime = System.currentTimeMillis();
        joinTogether();
        endTime = System.currentTimeMillis();
        System.out.println("\rJoining forward and backward paths: " + ((endTime - startTime) / 1000.0) + " sec");

        startTime = System.currentTimeMillis();
        saveMTAndSampling(ENTRY_TYPE_1);
        saveMTAndSampling(ENTRY_TYPE_2);
        saveMTAndSampling(ENTRY_TYPE_3);
        saveMTAndSampling(ENTRY_TYPE_4);
        endTime = System.currentTimeMillis();
        System.out.println("\nMT & Sampling: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public AcyclicMarkovTable(String graphFile, String mtFile, int mtLen, String sampleFile, int numSamples)
            throws Exception {
        src2label2dest = new HashMap<>();
        dest2label2src = new HashMap<>();
        this.mtFile = mtFile;
        this.sampleFile = sampleFile;
        this.numSamples = numSamples;
        this.mtLen = mtLen;

        emptyStringList = new ArrayList<>();
        emptyStringList.add("");

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

    public AcyclicMarkovTable(String mtFile, int mtLen, String sampleFile, int numSamples) {
        this.mtFile = mtFile;
        this.sampleFile = sampleFile;
        this.numSamples = numSamples;
        this.mtLen = mtLen;
    }

    public void proceed() throws Exception {
        readBackwardForwardParts();

        System.out.print("Joining forward and backward paths");
        long startTime = System.currentTimeMillis();
        joinTogether();
        long endTime = System.currentTimeMillis();
        System.out.println("\rJoining forward and backward paths: "
            + ((endTime - startTime) / 1000.0) + " sec");

        startTime = System.currentTimeMillis();
        saveMTAndSampling(ENTRY_TYPE_1);
        saveMTAndSampling(ENTRY_TYPE_2);
        saveMTAndSampling(ENTRY_TYPE_3);
        saveMTAndSampling(ENTRY_TYPE_4);
        endTime = System.currentTimeMillis();
        System.out.println("MT & Sampling: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void saveBackwardForwardParts() throws Exception {
        Map<Integer, Map<Path, List<String>>> backwardPart = backwardPart2;
        Map<Integer, Map<Path, List<String>>> forwardPart = forwardPart124;

        BufferedWriter backwardWriter = new BufferedWriter(
            new FileWriter("backward" + mtLen + ".csv")
        );
        BufferedWriter forwardWriter = new BufferedWriter(
            new FileWriter("forward" + mtLen + ".csv")
        );

        int numCentrals = forwardPart.size() + backwardPart.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        StringJoiner sj;
        for (Integer centralVertex : backwardPart.keySet()) {
            sj = new StringJoiner(",");
            sj.add(centralVertex.toString());
            for (Path path : backwardPart.get(centralVertex).keySet()) {
                sj.add(path.toSimpleString());
//                sj.add(Integer.toString(backwardPart.get(centralVertex).get(path).size()));
//                for (String vertexList : backwardPart.get(centralVertex).get(path)) {
//                    sj.add(String.join("->", vertexList.split(",")));
//                }
            }

            backwardWriter.write(sj.toString() + "\n");

            progress += 100.0 / numCentrals;
            System.out.print("\rSaving Backward/Forward: " + (int) progress + "%");
        }
        backwardWriter.close();

        for (Integer centralVertex : forwardPart.keySet()) {
            sj = new StringJoiner(",");
            sj.add(centralVertex.toString());
            for (Path path : forwardPart.get(centralVertex).keySet()) {
                sj.add(path.toSimpleString());
                sj.add(Integer.toString(forwardPart.get(centralVertex).get(path).size()));
//                for (String vertexList : forwardPart.get(centralVertex).get(path)) {
//                    sj.add(String.join("->", vertexList.split(",")));
//                }
            }

            forwardWriter.write(sj.toString() + "\n");

            progress += 100.0 / numCentrals;
            System.out.print("\rSaving Backward/Forward: " + (int) progress + "%");
        }
        forwardWriter.close();

        endTime = System.currentTimeMillis();
        System.out.println("\nSaving Backward/Forward: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void readBackwardForwardParts() throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        String[] info, labelList, vertexList;
        int centralVertex, numVertexLists;
        Path path;
        StringJoiner vertexPath;

        BufferedReader backwardReader = new BufferedReader(new FileReader("backward.csv"));
        String line = backwardReader.readLine();
        while (null != line) {
            info = line.split(",");
            centralVertex = Integer.parseInt(info[0]);
            backwardPart13.putIfAbsent(centralVertex, new HashMap<>());

            int k = 1;
            while (k < info.length) {
                labelList = info[k].split("->");
                path = new Path(new ArrayList<>());
                for (int i = 0; i < labelList.length; ++i) {
                    path.append(Integer.parseInt(labelList[i]));
                }
                backwardPart13.get(centralVertex).putIfAbsent(path, new ArrayList<>());

                numVertexLists = Integer.parseInt(info[k + 1]);
                k++;
                for (int i = 1; i <= numVertexLists; ++i) {
                    vertexList = info[k + i].split("->");
                    vertexPath = new StringJoiner(",");
                    for (int j = 0; j < vertexList.length; ++j) {
                        vertexPath.add(vertexList[j]);
                    }
                    backwardPart13.get(centralVertex).get(path).add(vertexPath.toString());
                }

                k += numVertexLists + 1;
            }

            line = backwardReader.readLine();
        }
        backwardReader.close();

        BufferedReader forwardReader = new BufferedReader(new FileReader("forward.csv"));
        line = forwardReader.readLine();
        while (null != line) {
            info = line.split(",");
            centralVertex = Integer.parseInt(info[0]);
            forwardPart124.putIfAbsent(centralVertex, new HashMap<>());

            int k = 1;
            while (k < info.length) {
                labelList = info[k].split("->");
                path = new Path(new ArrayList<>());
                for (int i = 0; i < labelList.length; ++i) {
                    path.append(Integer.parseInt(labelList[i]));
                }
                forwardPart124.get(centralVertex).putIfAbsent(path, new ArrayList<>());

                numVertexLists = Integer.parseInt(info[k + 1]);
                k++;
                for (int i = 1; i <= numVertexLists; ++i) {
                    vertexList = info[k + i].split("->");
                    vertexPath = new StringJoiner(",");
                    for (int j = 0; j < vertexList.length; ++j) {
                        vertexPath.add(vertexList[j]);
                    }
                    forwardPart124.get(centralVertex).get(path).add(vertexPath.toString());
                }

                k += numVertexLists + 1;
            }

            line = forwardReader.readLine();
        }
        forwardReader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Backward/Forward Loading: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println("mtFile: " + args[1]);
        System.out.println("mtLen: " + args[2]);
        System.out.println("sampleFile: " + args[3]);
        System.out.println("numSamples: " + args[4]);
        System.out.println("hasForwardBackwardFiles: " + args[5]);
        System.out.println();

        String graphFile = args[0];
        String mtFile = args[1];
        final int mtLen = Integer.parseInt(args[2]);
        String sampleFile = args[3];
        final int numSamples = Integer.parseInt(args[4]);
        boolean hasBackwardForward = Boolean.parseBoolean(args[5]);

        if (hasBackwardForward) {
            AcyclicMarkovTable mt = new AcyclicMarkovTable(mtFile, mtLen, sampleFile, numSamples);
            mt.proceed();
        } else {
            AcyclicMarkovTable mt = new AcyclicMarkovTable(graphFile, mtFile, mtLen, sampleFile, numSamples);
            mt.build();
        }
    }
}
