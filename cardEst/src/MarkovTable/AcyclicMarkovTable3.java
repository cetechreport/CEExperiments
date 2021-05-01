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

public class AcyclicMarkovTable3 {
    private Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    private Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();
    private Map<Integer, Map<Integer, List<Integer>>> src2dest2label = new HashMap<>();
    private Map<Integer, List<String>> label2srcdest = new HashMap<>();

    // central (eID or vID) -> label -> list of vertex list
    private Map<Integer, Map<Integer, List<String>>> backwardOut = new HashMap<>();
    private Map<Integer, Map<Integer, List<String>>> backwardIn = new HashMap<>();
    private Map<Integer, Map<Integer, List<String>>> forwardOut = new HashMap<>();
    private Map<Integer, Map<Integer, List<String>>> forwardIn = new HashMap<>();

    private Map<Path, List<Pair<Integer, Long>>> path2cumCounts = new HashMap<>();

    private Random random = new Random(0);
    private final double RATIO = 0.01;
    private final int MULTIPLIER = (int) (1 / RATIO);

    private List<String> eID2edgeString = new ArrayList<>();
    private Map<String, Integer> edgeString2eID = new HashMap<>();

    private String mtFile;
    private String sampleFile;
    private int numSamples;
    private int numTotalEdges = 0;

    private String edgeToString(Integer src, Integer label, Integer dest) {
        return src + "," + label + "," + dest;
    }

    private void traverseOneStep(Integer startV, int traverseDir, int centralType, int centralID) {
        Map<Integer, Map<Integer, List<String>>> outs;
        Map<Integer, Map<Integer, List<String>>> ins;
        if (traverseDir == Constants.FORWARD) {
            outs = forwardOut;
            ins = forwardIn;
        } else {
            outs = backwardOut;
            ins = backwardIn;
        }

        Integer midLabel = -1;
        Integer theOtherMidV = -1;
        String[] edge;
        if (centralType == Constants.EDGE) {
            edge = eID2edgeString.get(centralID).split(",");
            midLabel = Integer.parseInt(edge[1]);
            theOtherMidV = Integer.parseInt(edge[2]);
        }

        if (src2label2dest.containsKey(startV)) {
            for (Integer label : src2label2dest.get(startV).keySet()) {
                for (Integer endV : src2label2dest.get(startV).get(label)) {
                    if (traverseDir == Constants.BACKWARD &&
                        label.equals(midLabel) && endV.equals(theOtherMidV)) continue;

                    outs.putIfAbsent(centralID, new HashMap<>());
                    outs.get(centralID).putIfAbsent(label, new ArrayList<>());
                    if (traverseDir == Constants.FORWARD) {
                        outs.get(centralID).get(label).add(startV + "," + endV);
                    } else {
                        outs.get(centralID).get(label).add(endV + "," + startV);
                    }
                }
            }
        }

        if (dest2label2src.containsKey(startV)) {
            for (Integer label : dest2label2src.get(startV).keySet()) {
                for (Integer endV : dest2label2src.get(startV).get(label)) {
                    if (traverseDir == Constants.FORWARD &&
                        label.equals(midLabel) && endV.equals(theOtherMidV)) continue;

                    ins.putIfAbsent(centralID, new HashMap<>());
                    ins.get(centralID).putIfAbsent(label, new ArrayList<>());
                    if (traverseDir == Constants.FORWARD) {
                        ins.get(centralID).get(label).add(startV + "," + endV);
                    } else {
                        ins.get(centralID).get(label).add(endV + "," + startV);
                    }
                }
            }
        }
    }

    private void join(int length, int backwardDir, int forwardDir) {
        path2cumCounts.clear();

        Map<Integer, Map<Integer, List<String>>> backward;
        Map<Integer, Map<Integer, List<String>>> forward;
        if (backwardDir == Constants.OUTGOING) {
            backward = backwardOut;
        } else {
            backward = backwardIn;
        }

        if (forwardDir == Constants.OUTGOING) {
            forward = forwardOut;
        } else {
            forward = forwardIn;
        }

        long backwardCount, forwardCount;
        Path path;
        Long prevCumCount, cumCount;
        List<Pair<Integer, Long>> cumCounts;
        Integer midLabel = -1;
        String[] edge;
        for (Integer midEdgeID : backward.keySet()) {
            if (!forward.containsKey(midEdgeID)) continue;

            if (length == 3) {
                edge = eID2edgeString.get(midEdgeID).split(",");
                midLabel = Integer.parseInt(edge[1]);
            }

            for (Integer prefix : backward.get(midEdgeID).keySet()) {
                for (Integer suffix : forward.get(midEdgeID).keySet()) {
                    path = new Path(prefix.toString());
                    if (length == 3) {
                        path.append(midLabel);
                    }
                    path.append(suffix);

                    backwardCount = backward.get(midEdgeID).get(prefix).size();
                    forwardCount = forward.get(midEdgeID).get(suffix).size();

                    if (length == 2 && prefix.equals(suffix) && forwardDir == backwardDir) {
                        forwardCount--;
                    }

                    if (backwardCount == 0 || forwardCount == 0) continue;

                    if (path2cumCounts.containsKey(path)) {
                        cumCounts = path2cumCounts.get(path);
                        prevCumCount = cumCounts.get(cumCounts.size() - 1).value;
                        cumCount = prevCumCount + (backwardCount * forwardCount);
                        path2cumCounts.get(path).add(new Pair<>(midEdgeID, cumCount));
                    } else {
                        path2cumCounts.put(path, new ArrayList<>());
                        cumCount = backwardCount * forwardCount;
                        path2cumCounts.get(path).add(new Pair<>(midEdgeID, cumCount));
                    }
                }
            }
        }
    }

    private String getPathInstance(Path path, int central, int offset, int entryType) {
        Map<Integer, Map<Integer, List<String>>> backwardPart;
        Map<Integer, Map<Integer, List<String>>> forwardPart;
        switch (entryType) {
            case Constants.ENTRY_TYPE_1:
                backwardPart = backwardIn;
                forwardPart = forwardOut;
                break;
            case Constants.ENTRY_TYPE_2:
                backwardPart = backwardIn;
                forwardPart = forwardIn;
                break;
            case Constants.ENTRY_TYPE_3:
                backwardPart = backwardOut;
                forwardPart = forwardOut;
                break;
            case Constants.ENTRY_TYPE_4:
                backwardPart = backwardOut;
                forwardPart = forwardIn;
                break;
            default:
                backwardPart = new HashMap<>();
                forwardPart = new HashMap<>();
                break;
        }

        if (1 == path.length()) {
            return backwardPart.get(central).get(path.getEdgeLabelList().get(0)).get(offset - 1);
        }

        Integer backwardLabel = path.getEdgeLabelList().get(0);
        Integer forwardLabel = path.getEdgeLabelList().get(path.length() - 1);

        int forwardSize = forwardPart.get(central).get(forwardLabel).size();

        int backwardIndex = (offset - 1) / forwardSize;
        int forwardIndex = (offset - 1) % forwardSize;

        String backwardVertexList =
            backwardPart.get(central).get(backwardLabel).get(backwardIndex);
        String forwardVertexList =
            forwardPart.get(central).get(forwardLabel).get(forwardIndex);

        String sampledInstance = backwardVertexList;
        if (path.length() == 2) {
            String[] forwardVListArray = forwardVertexList.split(",");
            sampledInstance += "," +
                String.join(",", Arrays.copyOfRange(forwardVListArray, 1, forwardVListArray.length));
        } else if (path.length() == 3) {
            sampledInstance += "," + forwardVertexList;
        }

        return sampledInstance;
    }

    private String materializePathInstance(
        Path path, List<Pair<Integer, Long>> cumCounts, long sampledIndex, int entryType) {

        try {
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
        } catch (ArithmeticException e) {
            return materializePathInstance(
                path, cumCounts, random.nextInt(Integer.MAX_VALUE) + 1, entryType
            );
        }
    }

    private String samplesToString(List<String> samples) {
        StringJoiner sj = new StringJoiner(",");
        for (String sample : samples) {
            sj.add(String.join("->", sample.split(",")));
        }
        return sj.toString();
    }

    private void saveMTAndSampling(int length, int type) throws Exception {
        String mtFileOfType = mtFile + "_" + length + "_type" + type;
        String sampleFileOfType = sampleFile + "_" + length + "_type" + type;

        BufferedWriter mtWriter = new BufferedWriter(new FileWriter(mtFileOfType));
        BufferedWriter sampleWriter = new BufferedWriter(new FileWriter(sampleFileOfType));

        int numPaths = path2cumCounts.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        Long total, sampledIndex;
        List<Pair<Integer, Long>> cumCounts;
        ThreadLocalRandom random = ThreadLocalRandom.current();

        List<String> samples = new ArrayList<>();
        Set<Long> sampledIndices = new HashSet<>();

        for (Path path : path2cumCounts.keySet()) {
            samples.clear();
            sampledIndices.clear();

            cumCounts = path2cumCounts.get(path);
            total = cumCounts.get(cumCounts.size() - 1).value;

            // write to MT file
            if (length == 3) {
                mtWriter.write(path.toSimpleString() + "," + (total * MULTIPLIER) + "\n");
            } else if (length == 2) {
                mtWriter.write(path.toSimpleString() + "," + total + "\n");
            }

            while (samples.size() < total && samples.size() < numSamples) {
                sampledIndex = random.nextLong(total) + 1;
                while (sampledIndices.contains(sampledIndex)) {
                    sampledIndex = random.nextLong(total) + 1;
                }
                sampledIndices.add(sampledIndex);

                samples.add(materializePathInstance(path, cumCounts, sampledIndex, type));
            }

            sampleWriter.write(path.toSimpleString() + "," + samplesToString(samples) + "\n");

            progress += 100.0 / numPaths;
            System.out.print("\rMT & Sampling " + type + ": " + (int) progress + "%");
        }
        mtWriter.close();
        sampleWriter.close();

        endTime = System.currentTimeMillis();
        System.out.println("\nMT & Sampling " + type + ": " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void buildLen3() throws Exception {
        double progress = 0;
        double prevProgress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        int lottery;
        String edgeString;
        for (Integer src : src2dest2label.keySet()) {
            for (Integer dest : src2dest2label.get(src).keySet()) {
                for (Integer label : src2dest2label.get(src).get(dest)) {
                    progress += 100.0 / numTotalEdges;
                    if (progress - prevProgress > 1) {
                        System.out.print("\rForward & Backward 3: " + (int) progress + "%");
                        prevProgress = progress;
                    }
                    lottery = random.nextInt(eID2edgeString.size() - 1);
                    if (lottery > eID2edgeString.size() * RATIO) continue;

                    edgeString = edgeToString(src, label, dest);

                    traverseOneStep(src, Constants.BACKWARD, Constants.EDGE, edgeString2eID.get(edgeString));
                    traverseOneStep(dest, Constants.FORWARD, Constants.EDGE, edgeString2eID.get(edgeString));
                }
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nForward & Backward 3: " + ((endTime - startTime) / 1000.0) + " sec");

        startTime = System.currentTimeMillis();
        join(3, Constants.INCOMING, Constants.OUTGOING);
        endTime = System.currentTimeMillis();
        System.out.println("Join 1: " + ((endTime - startTime) / 1000.0) + " sec");
        saveMTAndSampling(3, Constants.ENTRY_TYPE_1);

        startTime = System.currentTimeMillis();
        join(3, Constants.INCOMING, Constants.INCOMING);
        endTime = System.currentTimeMillis();
        System.out.println("Join 2: " + ((endTime - startTime) / 1000.0) + " sec");
        saveMTAndSampling(3, Constants.ENTRY_TYPE_2);

        startTime = System.currentTimeMillis();
        join(3, Constants.OUTGOING, Constants.OUTGOING);
        endTime = System.currentTimeMillis();
        System.out.println("Join 3: " + ((endTime - startTime) / 1000.0) + " sec");
        saveMTAndSampling(3, Constants.ENTRY_TYPE_3);

        startTime = System.currentTimeMillis();
        join(3, Constants.OUTGOING, Constants.INCOMING);
        endTime = System.currentTimeMillis();
        System.out.println("Join 4: " + ((endTime - startTime) / 1000.0) + " sec");
        saveMTAndSampling(3, Constants.ENTRY_TYPE_4);
    }

    private void buildLen2() throws Exception {
        Set<Integer> allVertices = new HashSet<>(src2label2dest.keySet());
        allVertices.addAll(dest2label2src.keySet());

        double progress = 0;
        double prevProgress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer midV : allVertices) {
            progress += 100.0 / allVertices.size();
            if (progress - prevProgress > 1) {
                System.out.print("\rForward & Backward 2: " + (int) progress + "%");
                prevProgress = progress;
            }
            traverseOneStep(midV, Constants.BACKWARD, Constants.VERTEX, midV);
            traverseOneStep(midV, Constants.FORWARD, Constants.VERTEX, midV);
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nForward & Backward 2: " + ((endTime - startTime) / 1000.0) + " sec");

        startTime = System.currentTimeMillis();
        join(2, Constants.INCOMING, Constants.OUTGOING);
        endTime = System.currentTimeMillis();
        System.out.println("Join 1: " + ((endTime - startTime) / 1000.0) + " sec");
        saveMTAndSampling(2, Constants.ENTRY_TYPE_1);

        startTime = System.currentTimeMillis();
        join(2, Constants.INCOMING, Constants.INCOMING);
        endTime = System.currentTimeMillis();
        System.out.println("Join 2: " + ((endTime - startTime) / 1000.0) + " sec");
        saveMTAndSampling(2, Constants.ENTRY_TYPE_2);

        startTime = System.currentTimeMillis();
        join(2, Constants.OUTGOING, Constants.OUTGOING);
        endTime = System.currentTimeMillis();
        System.out.println("Join 3: " + ((endTime - startTime) / 1000.0) + " sec");
        saveMTAndSampling(2, Constants.ENTRY_TYPE_3);
    }

    private void buildLen1() throws Exception {
        List<String> samples = new ArrayList<>();
        int total, sampledIndex;
        Set<Integer> sampledIndices = new HashSet<>();

        double progress = 0;
        double prevProgress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedWriter mtWriter = new BufferedWriter(new FileWriter(mtFile + "_1"));
        BufferedWriter sampleWriter = new BufferedWriter(new FileWriter(sampleFile + "_1"));
        for (Integer label : label2srcdest.keySet()) {
            progress += 100.0 / label2srcdest.size();
            if (progress - prevProgress > 1) {
                System.out.print("\rLength 1: " + (int) progress + "%");
                prevProgress = progress;
            }

            samples.clear();
            sampledIndices.clear();

            // writes to mt file
            total = label2srcdest.get(label).size();
            mtWriter.write(label + "," + total + "\n");

            while (samples.size() < total && samples.size() < numSamples) {
                sampledIndex = random.nextInt(total);
                while (sampledIndices.contains(sampledIndex)) {
                    sampledIndex = random.nextInt(total);
                }
                sampledIndices.add(sampledIndex);

                samples.add(label2srcdest.get(label).get(sampledIndex));
            }

            sampleWriter.write(label + "," + samplesToString(samples) + "\n");
        }
        mtWriter.close();
        sampleWriter.close();

        label2srcdest.clear();

        endTime = System.currentTimeMillis();
        System.out.println("\nLength 1: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public void build() throws Exception {
        buildLen1();
        buildLen3();

        forwardOut.clear();
        forwardIn.clear();
        backwardOut.clear();
        backwardIn.clear();
        path2cumCounts.clear();
        buildLen2();
    }

    public AcyclicMarkovTable3(String graphFile, String mtFile, String sampleFile, int numSamples)
        throws Exception {
        this.mtFile = mtFile;
        this.sampleFile = sampleFile;
        this.numSamples = numSamples;

        long startTime = System.currentTimeMillis();
        long endTime;

        eID2edgeString.add(""); // let actual edgeID start from 1

        BufferedReader csvReader = new BufferedReader(new FileReader(graphFile));
        int[] line;
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            numTotalEdges++;
            line = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt).toArray();

            eID2edgeString.add(tripleString);
            edgeString2eID.put(tripleString, eID2edgeString.size() - 1);

            label2srcdest.putIfAbsent(line[1], new ArrayList<>());
            label2srcdest.get(line[1]).add(line[0] + "," + line[2]);

            src2label2dest.putIfAbsent(line[0], new HashMap<>());
            src2label2dest.get(line[0]).putIfAbsent(line[1], new ArrayList<>());
            src2label2dest.get(line[0]).get(line[1]).add(line[2]);

            dest2label2src.putIfAbsent(line[2], new HashMap<>());
            dest2label2src.get(line[2]).putIfAbsent(line[1], new ArrayList<>());
            dest2label2src.get(line[2]).get(line[1]).add(line[0]);

            src2dest2label.putIfAbsent(line[0], new HashMap<>());
            src2dest2label.get(line[0]).putIfAbsent(line[2], new ArrayList<>());
            src2dest2label.get(line[0]).get(line[2]).add(line[1]);

            tripleString = csvReader.readLine();
        }

        csvReader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Graph Loading: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println("mtFile: " + args[1]);
        System.out.println("sampleFile: " + args[2]);
        System.out.println("numSamples: " + args[3]);
        System.out.println();

        String graphFile = args[0];
        String mtFile = args[1];
        String sampleFile = args[2];
        final int numSamples = Integer.parseInt(args[3]);

        AcyclicMarkovTable3 mt = new AcyclicMarkovTable3(graphFile, mtFile, sampleFile, numSamples);
        mt.build();
    }
}
