package Graphflow;

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

public class Uniformity {
    private Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    private Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();
//    private Map<Integer, Map<Integer, List<Integer>>> label2src2dest = new HashMap<>();
//    private Map<Integer, Map<Integer, List<Integer>>> label2dest2src = new HashMap<>();
    private List<Map<Integer, Map<Integer, List<Integer>>>> extend = new ArrayList<>();

    // (orientation string) + base label seq -> extension label -> histogram
    private Map<String, Map<String, Map<Long, Long>>> distribution = new HashMap<>();

    // Due to sampling, some entries may have only one occurrence through exp, making
    // the SD 0, but it may not be as uniform as it looks like
    // (orientation string) + base label seq -> extension label -> # of times probing "distribution"
//    private Map<String, Map<String, Integer>> numOcc = new HashMap<>();
    private Map<String, Integer> numOcc = new HashMap<>();

    private int NUM_SAMPLES;

    private final String BASE = "";
    private final Long TOTAL = 0L;

    private final int F_HEAD = 0;
    private final int F_MID = 1;
    private final int F_FORK = 2;

    private Set<Integer> getSamples(List<Integer> candidates, int numSamples) {
        if (-1 == numSamples) {
            numSamples = NUM_SAMPLES;
        }
        Set<Integer> sampledIndices = new HashSet<>();
        Random random = new Random(0);

        Set<Integer> samples = new HashSet<>();

        if (candidates.size() > numSamples) {
            int sampledIndex;
            while (samples.size() < numSamples) {
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

    private Integer getSample(List<Integer> candidates, Random random) {
        return candidates.get(random.nextInt(candidates.size()));
    }

    private void occurred(String baseLabelSeq) {
        numOcc.put(baseLabelSeq, numOcc.getOrDefault(baseLabelSeq, 0) + 1);
    }

    // returns ((orientation (0: outgoing, 1: incoming), edge_seq), [2path middle vertices])
    // dir: forward or backward of the 4 path
    private void get2Paths(Integer center, Map<String, Map<String, Set<Integer>>> store, int dir, Random random) {
        int dir1 = random.nextInt(extend.size());
        if (!extend.get(dir1).containsKey(center)) {
            dir1 = dir1 == Constants.FORWARD ? Constants.BACKWARD : Constants.FORWARD;
        }
        Set<Integer> labels1 = new HashSet<>(extend.get(dir1).get(center).keySet());

        Map<Integer, Set<Integer>> label2mids = new HashMap<>();
        for (Integer label : labels1) {
            label2mids.put(label, new HashSet<>(extend.get(dir1).get(center).get(label)));
        }

        int dir2 = random.nextInt(extend.size());
        Set<Integer> intersection = new HashSet<>(extend.get(dir2).keySet());
        Set<Integer> allMids = new HashSet<>();
        for (Set<Integer> mids : label2mids.values()) {
            allMids.addAll(mids);
        }
        intersection.retainAll(allMids);
        if (intersection.isEmpty()) {
            dir2 = dir2 == Constants.FORWARD ? Constants.BACKWARD : Constants.FORWARD;
        }

        String orientation, labelSeq;
        if (Constants.FORWARD == dir) {
            orientation = dir1 + "->" + dir2;
            store.putIfAbsent(orientation, new HashMap<>());
            for (Integer label1 : labels1) {
                for (Integer mid : label2mids.get(label1)) {
                    if (extend.get(dir2).containsKey(mid)) {
                        for (Integer label2 : extend.get(dir2).get(mid).keySet()) {
                            if (label1.equals(label2)) continue;
                            labelSeq = label1 + "->" + label2;
                            if (store.get(orientation).containsKey(labelSeq)) continue;
                            store.get(orientation).put(labelSeq, label2mids.get(label1));
                        }
                    }
                }
            }
        } else {
            orientation = dir2 + "->" + dir1;
            store.putIfAbsent(orientation, new HashMap<>());
            for (Integer label1 : labels1) {
                for (Integer mid : label2mids.get(label1)) {
                    if (extend.get(dir2).containsKey(mid)) {
                        for (Integer label2 : extend.get(dir2).get(mid).keySet()) {
                            if (label1.equals(label2)) continue;
                            labelSeq = label2 + "->" + label1;
                            if (store.get(orientation).containsKey(labelSeq)) continue;
                            store.get(orientation).put(labelSeq, label2mids.get(label1));
                        }
                    }
                }
            }
        }
    }

    private void computePathDist() {
        Random random = new Random(0);

        Set<Integer> allVertices = new HashSet<>(src2label2dest.keySet());
        allVertices.addAll(dest2label2src.keySet());
        Set<Integer> centers = getSamples(
            new ArrayList<>(allVertices), -1
        );

        String orientAndLabelSeq, extendOrientLabelSeq;

        Integer firstLabel, lastLabel, firstDir, lastDir;
        long leftCount, rightCount;

        Map<String, Map<String, Set<Integer>>> backward = new HashMap<>();
        Map<String, Map<String, Set<Integer>>> forward = new HashMap<>();

        int numCenters = centers.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer center : centers) {
            progress += 100.0 / numCenters;
            System.out.print("\rPATH dist: " + (int) progress + "%");

            backward.clear();
            forward.clear();
            get2Paths(center, backward, Constants.BACKWARD, random);
            get2Paths(center, forward, Constants.FORWARD, random);

            for (String backOrientation : backward.keySet()) {
                for (String forOrientation : forward.keySet()) {
                    String[] backOrientSplit = backOrientation.split("->");
                    String[] forOrientSplit = forOrientation.split("->");
                    if (backOrientSplit[0].equals(forOrientSplit[1]) &&
                        backOrientSplit[1].equals(forOrientSplit[0])) continue;

                    firstDir = Integer.parseInt(backOrientSplit[0]);
                    lastDir = Integer.parseInt(forOrientSplit[1]);

                    for (String backLabelSeq : backward.get(backOrientation).keySet()) {
                        for (String forLabelSeq : forward.get(forOrientation).keySet()) {
                            String[] backLabelSeqSplit = backLabelSeq.split("->");
                            String[] forLabelSeqSplit = forLabelSeq.split("->");
                            firstLabel = Integer.parseInt(backLabelSeqSplit[0]);
                            lastLabel = Integer.parseInt(forLabelSeqSplit[1]);

                            for (Integer backwardMid : backward.get(backOrientation).get(backLabelSeq)) {
                                for (Integer forwardMid : forward.get(forOrientation).get(forLabelSeq)) {
                                    if (extend.get(firstDir).get(backwardMid).containsKey(firstLabel)) {
                                        leftCount = extend.get(firstDir).get(backwardMid).get(firstLabel).size();
                                    } else {
                                        leftCount = 0;
                                    }

                                    if (extend.get(lastDir).get(forwardMid).containsKey(lastLabel)) {
                                        rightCount = extend.get(lastDir).get(forwardMid).get(lastLabel).size();
                                    } else {
                                        rightCount = 0;
                                    }

                                    orientAndLabelSeq = backOrientation + "->" + forOrientSplit[0];
                                    orientAndLabelSeq += "," + backLabelSeq + "->" + forLabelSeqSplit[0];
                                    extendOrientLabelSeq = forOrientSplit[1] + "," + forLabelSeqSplit[1];

                                    distribution.putIfAbsent(orientAndLabelSeq, new HashMap<>());
                                    distribution.get(orientAndLabelSeq).putIfAbsent(BASE, new HashMap<>());
                                    distribution.get(orientAndLabelSeq).get(BASE).put(
                                        TOTAL,
                                        distribution.get(orientAndLabelSeq).get(BASE)
                                            .getOrDefault(TOTAL, 0L) + leftCount
                                    );
                                    distribution.get(orientAndLabelSeq).putIfAbsent(extendOrientLabelSeq, new HashMap<>());
                                    distribution.get(orientAndLabelSeq).get(extendOrientLabelSeq).put(
                                        rightCount,
                                        distribution.get(orientAndLabelSeq).get(extendOrientLabelSeq)
                                            .getOrDefault(rightCount, 0L) + leftCount
                                    );
                                    occurred(orientAndLabelSeq);

                                    orientAndLabelSeq = backOrientSplit[1] + "->" + forOrientation;
                                    orientAndLabelSeq += "," + backLabelSeqSplit[1] + "->" + forLabelSeq;
                                    extendOrientLabelSeq = backOrientSplit[0] + "," + backLabelSeqSplit[0];

                                    distribution.putIfAbsent(orientAndLabelSeq, new HashMap<>());
                                    distribution.get(orientAndLabelSeq).putIfAbsent(BASE, new HashMap<>());
                                    distribution.get(orientAndLabelSeq).get(BASE).put(
                                        TOTAL,
                                        distribution.get(orientAndLabelSeq).get(BASE)
                                            .getOrDefault(TOTAL, 0L) + rightCount
                                    );
                                    distribution.get(orientAndLabelSeq).putIfAbsent(extendOrientLabelSeq, new HashMap<>());
                                    distribution.get(orientAndLabelSeq).get(extendOrientLabelSeq).put(
                                        leftCount,
                                        distribution.get(orientAndLabelSeq).get(extendOrientLabelSeq)
                                            .getOrDefault(leftCount, 0L) + rightCount
                                    );
                                    occurred(orientAndLabelSeq);
                                }
                            }
                        }
                    }
                }
            }
        }

        endTime = System.currentTimeMillis();
        System.out.println("\nPATH dist: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void computeStarDist() {
        Random random = new Random(0);

        Set<Integer> allVertices = new HashSet<>(src2label2dest.keySet());
        allVertices.addAll(dest2label2src.keySet());
        Set<Integer> centers = getSamples(new ArrayList<>(allVertices), -1);

        List<Integer> dirs = new ArrayList<>();
        List<Set<Integer>> labels = new ArrayList<>();

        int numCenters = centers.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer center : centers) {
            progress += 100.0 / numCenters;
            System.out.print("\rSTAR dist: " + (int) progress + "%");

            dirs.clear();
            labels.clear();

            Set<String> processed4 = new HashSet<>();
            Set<String> processed3 = new HashSet<>();

            for (int i = 0; i < 4; ++i) {
                int dir = random.nextInt(extend.size());
                if (!extend.get(dir).containsKey(center)) {
                    dir = dir == Constants.FORWARD ? Constants.BACKWARD : Constants.FORWARD;
                }

                dirs.add(dir);
                labels.add(new HashSet<>(extend.get(dir).get(center).keySet()));
            }

            String[] labelSeqArray = new String[3];
            String[] sorted = new String[4];
            long count1, count2, count3, count4;
            String labelSeq;
            for (Integer label1 : labels.get(0)) {
                for (Integer label2 : labels.get(1)) {
                    if (label1.equals(label2)) continue;
                    for (Integer label3 : labels.get(2)) {
                        if (label1.equals(label3) || label2.equals(label3)) continue;
                        for (Integer label4 : labels.get(3)) {
                            if (label1.equals(label4) || label2.equals(label4) || label3.equals(label4)) continue;
                            sorted[0] = label1.toString();
                            sorted[1] = label2.toString();
                            sorted[2] = label3.toString();
                            sorted[3] = label4.toString();
                            Arrays.sort(sorted);
                            labelSeq = String.join("->", sorted);
                            if (processed4.contains(labelSeq)) continue;
                            processed4.add(labelSeq);

                            count1 = extend.get(dirs.get(0)).get(center).get(label1).size();
                            count2 = extend.get(dirs.get(1)).get(center).get(label2).size();
                            count3 = extend.get(dirs.get(2)).get(center).get(label3).size();
                            count4 = extend.get(dirs.get(3)).get(center).get(label4).size();

                            labelSeqArray[0] = label1.toString();
                            labelSeqArray[1] = label2.toString();
                            labelSeqArray[2] = label3.toString();
                            Arrays.sort(labelSeqArray);
                            labelSeq = String.join("->", labelSeqArray);
                            distribution.putIfAbsent(labelSeq, new HashMap<>());
                            if (!processed3.contains(labelSeq)) {
                                distribution.get(labelSeq).putIfAbsent(BASE, new HashMap<>());
                                distribution.get(labelSeq).get(BASE).put(
                                    TOTAL,
                                    distribution.get(labelSeq).get(BASE)
                                        .getOrDefault(TOTAL, 0L) + count1 * count2 * count3
                                );
                                processed3.add(labelSeq);
                            }
                            distribution.get(labelSeq).putIfAbsent(label4.toString(), new HashMap<>());
                            distribution.get(labelSeq).get(label4.toString()).put(
                                count4,
                                distribution.get(labelSeq).get(label4.toString())
                                    .getOrDefault(count4, 0L) + count1 * count2 * count3
                            );
                            occurred(labelSeq);

                            labelSeqArray[0] = label1.toString();
                            labelSeqArray[1] = label2.toString();
                            labelSeqArray[2] = label4.toString();
                            Arrays.sort(labelSeqArray);
                            labelSeq = String.join("->", labelSeqArray);
                            distribution.putIfAbsent(labelSeq, new HashMap<>());
                            if (!processed3.contains(labelSeq)) {
                                distribution.get(labelSeq).putIfAbsent(BASE, new HashMap<>());
                                distribution.get(labelSeq).get(BASE).put(
                                    TOTAL,
                                    distribution.get(labelSeq).get(BASE)
                                        .getOrDefault(TOTAL, 0L) + count1 * count2 * count4
                                );
                                processed3.add(labelSeq);
                            }
                            distribution.get(labelSeq).putIfAbsent(label3.toString(), new HashMap<>());
                            distribution.get(labelSeq).get(label3.toString()).put(
                                count3,
                                distribution.get(labelSeq).get(label3.toString())
                                    .getOrDefault(count3,0L) + count1 * count2 * count4
                            );
                            occurred(labelSeq);

                            labelSeqArray[0] = label1.toString();
                            labelSeqArray[1] = label3.toString();
                            labelSeqArray[2] = label4.toString();
                            Arrays.sort(labelSeqArray);
                            labelSeq = String.join("->", labelSeqArray);
                            distribution.putIfAbsent(labelSeq, new HashMap<>());
                            if (!processed3.contains(labelSeq)) {
                                distribution.get(labelSeq).putIfAbsent(BASE, new HashMap<>());
                                distribution.get(labelSeq).get(BASE).put(
                                    TOTAL,
                                    distribution.get(labelSeq).get(BASE)
                                        .getOrDefault(TOTAL, 0L) + count1 * count3 * count4
                                );
                                processed3.add(labelSeq);
                            }
                            distribution.get(labelSeq).putIfAbsent(label2.toString(), new HashMap<>());
                            distribution.get(labelSeq).get(label2.toString()).put(
                                count2,
                                distribution.get(labelSeq).get(label2.toString())
                                    .getOrDefault(count2,0L) + count1 * count3 * count4
                            );
                            occurred(labelSeq);

                            labelSeqArray[0] = label2.toString();
                            labelSeqArray[1] = label3.toString();
                            labelSeqArray[2] = label4.toString();
                            Arrays.sort(labelSeqArray);
                            labelSeq = String.join("->", labelSeqArray);
                            distribution.putIfAbsent(labelSeq, new HashMap<>());
                            if (!processed3.contains(labelSeq)) {
                                distribution.get(labelSeq).putIfAbsent(BASE, new HashMap<>());
                                distribution.get(labelSeq).get(BASE).put(
                                    TOTAL,
                                    distribution.get(labelSeq).get(BASE)
                                        .getOrDefault(TOTAL, 0L) + count2 * count3 * count4
                                );
                                processed3.add(labelSeq);
                            }
                            distribution.get(labelSeq).putIfAbsent(label1.toString(), new HashMap<>());
                            distribution.get(labelSeq).get(label1.toString()).put(
                                count1,
                                distribution.get(labelSeq).get(label1.toString())
                                    .getOrDefault(count1, 0L) + count2 * count3 * count4
                            );
                            occurred(labelSeq);
                        }
                    }
                }
            }
        }

        endTime = System.currentTimeMillis();
        System.out.println("\nSTAR dist: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void computeForkDist() {
        Random random = new Random(0);

        Set<Integer> allVertices = new HashSet<>(src2label2dest.keySet());
        allVertices.addAll(dest2label2src.keySet());
        Set<Integer> centers = getSamples(new ArrayList<>(allVertices), -1);

        List<Integer> dirs = new ArrayList<>();
        List<Set<Integer>> labels = new ArrayList<>();

        int numCenters = centers.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer center : centers) {
            progress += 100.0 / numCenters;
            System.out.print("\rFORK dist: " + (int) progress + "%");

            dirs.clear();
            labels.clear();

            Set<String> processed = new HashSet<>();

            for (int i = 0; i < 3; ++i) {
                int dir = random.nextInt(extend.size());
                if (!extend.get(dir).containsKey(center)) {
                    dir = dir == Constants.FORWARD ? Constants.BACKWARD : Constants.FORWARD;
                }

                dirs.add(dir);
                labels.add(new HashSet<>(extend.get(dir).get(center).keySet()));
            }

            String[] forkLabelSeqArray = new String[2];
            String labelSeq, mLabel;
            int fIndex1, fIndex2;
            long fCount1, fCount2, eCount;
            for (Integer label1 : labels.get(0)) {
                for (Integer label2 : labels.get(1)) {
                    if (label1.equals(label2)) continue;
                    for (Integer label3: labels.get(2)) {
                        if (label1.equals(label3) || label2.equals(label3)) continue;

                        Integer[] centerEdges = new Integer[] {label1, label2, label3};
                        List<Integer> mids;
                        for (int i = 0; i < centerEdges.length; ++i) {
                            mids = extend.get(dirs.get(i)).get(center).get(centerEdges[i]);

                            fIndex1 = (i + 1) % 3;
                            fIndex2 = (i + 2) % 3;
                            mLabel = centerEdges[i].toString();
                            forkLabelSeqArray[0] = centerEdges[fIndex1].toString();
                            forkLabelSeqArray[1] = centerEdges[fIndex2].toString();
                            Arrays.sort(forkLabelSeqArray);

                            String id = centerEdges[i] + "->" + String.join("->", forkLabelSeqArray);
                            if (processed.contains(id)) continue;
                            processed.add(id);

                            int dir4 = random.nextInt(extend.size());

                            for (Integer mid : mids) {
                                if (!extend.get(dir4).containsKey(mid)) continue;
                                for (Integer label4 : extend.get(dir4).get(mid).keySet()) {
                                    if (label1.equals(label4) || label2.equals(label4) ||
                                        label3.equals(label4)) continue;

                                    fCount1 = extend.get(dirs.get(fIndex1)).get(center).
                                        get(centerEdges[fIndex1]).size();
                                    fCount2 = extend.get(dirs.get(fIndex2)).get(center).
                                        get(centerEdges[fIndex2]).size();
                                    eCount = extend.get(dir4).get(mid).get(label4).size();

                                    labelSeq = F_MID + "->" + F_FORK + "->" + F_FORK + ",";
                                    labelSeq += mLabel + "->" + String.join("->", forkLabelSeqArray);

                                    distribution.putIfAbsent(labelSeq, new HashMap<>());
                                    distribution.get(labelSeq).putIfAbsent(BASE, new HashMap<>());
                                    distribution.get(labelSeq).get(BASE).put(
                                        TOTAL,
                                        distribution.get(labelSeq).get(BASE)
                                            .getOrDefault(TOTAL, 0L) + fCount1 * fCount2
                                    );
                                    distribution.get(labelSeq).putIfAbsent(label4.toString(), new HashMap<>());
                                    distribution.get(labelSeq).get(label4.toString()).put(
                                        eCount,
                                        distribution.get(labelSeq).get(label4.toString())
                                            .getOrDefault(eCount, 0L) + fCount1 * fCount2
                                    );
                                    occurred(labelSeq);

                                    labelSeq = F_HEAD + "->" + F_MID + "->" + F_FORK + ",";
                                    labelSeq += label4 + "->" + mLabel + "->" + centerEdges[fIndex2];

                                    distribution.putIfAbsent(labelSeq, new HashMap<>());
                                    distribution.get(labelSeq).putIfAbsent(BASE, new HashMap<>());
                                    distribution.get(labelSeq).get(BASE).put(
                                        TOTAL,
                                        distribution.get(labelSeq).get(BASE)
                                            .getOrDefault(TOTAL, 0L) + eCount * fCount2
                                    );
                                    distribution.get(labelSeq).putIfAbsent(centerEdges[fIndex1].toString(), new HashMap<>());
                                    distribution.get(labelSeq).get(centerEdges[fIndex1].toString()).put(
                                        fCount1,
                                        distribution.get(labelSeq).get(centerEdges[fIndex1].toString()).
                                            getOrDefault(fCount1, 0L) + eCount * fCount2
                                    );
                                    occurred(labelSeq);

                                    labelSeq = F_HEAD + "->" + F_MID + "->" + F_FORK + ",";
                                    labelSeq += label4 + "->" + mLabel + "->" + centerEdges[fIndex1];

                                    distribution.putIfAbsent(labelSeq, new HashMap<>());
                                    distribution.get(labelSeq).putIfAbsent(BASE, new HashMap<>());
                                    distribution.get(labelSeq).get(BASE).put(
                                        TOTAL,
                                        distribution.get(labelSeq).get(BASE)
                                            .getOrDefault(TOTAL, 0L) + eCount * fCount1
                                    );
                                    distribution.get(labelSeq).putIfAbsent(centerEdges[fIndex2].toString(), new HashMap<>());
                                    distribution.get(labelSeq).get(centerEdges[fIndex2].toString()).put(
                                        fCount2,
                                        distribution.get(labelSeq).get(centerEdges[fIndex2].toString()).
                                            getOrDefault(fCount2, 0L) + eCount * fCount1
                                    );
                                    occurred(labelSeq);
                                }
                            }
                        }
                    }
                }
            }
        }

        endTime = System.currentTimeMillis();
        System.out.println("\nFORK dist: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void saveDistribution(String destFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedWriter writer = new BufferedWriter(new FileWriter(destFile));
        for (String labelSeq : distribution.keySet()) {
            long inc0Total = distribution.get(labelSeq).get(BASE).get(TOTAL);
            for (String extendedEdge : distribution.get(labelSeq).keySet()) {
                if (extendedEdge.equals(BASE)) continue;
                if (numOcc.get(labelSeq) <= 10) continue;

                StringJoiner sj = new StringJoiner(",");
                sj.add(labelSeq);
                sj.add(extendedEdge);
                sj.add(numOcc.get(labelSeq).toString());

//                long exc0Total = 0;
                long total = 0;
                for (Long entry : distribution.get(labelSeq).get(extendedEdge).keySet()) {
                    total += entry * distribution.get(labelSeq).get(extendedEdge).get(entry);
//                    exc0Total += distribution.get(labelSeq).get(extendedEdge).get(entry);
                }

                double mean = ((double) total) / inc0Total;
                double sd = computeSD(distribution.get(labelSeq).get(extendedEdge), mean, inc0Total);
                double cv = sd / mean;

                sj.add(Double.toString(cv));
                writer.write(sj.toString() + "\n");
            }
        }
        writer.close();
        distribution.clear();
        numOcc.clear();

        endTime = System.currentTimeMillis();
        System.out.println("Saving CV: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private Uniformity(String graphFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader csvReader = new BufferedReader(new FileReader(graphFile));
        int[] edge;
        String line = csvReader.readLine();
        while (null != line) {
            edge = Arrays.stream(line.split(",")).mapToInt(Integer::parseInt).toArray();

            src2label2dest.putIfAbsent(edge[0], new HashMap<>());
            src2label2dest.get(edge[0]).putIfAbsent(edge[1], new ArrayList<>());
            src2label2dest.get(edge[0]).get(edge[1]).add(edge[2]);

            dest2label2src.putIfAbsent(edge[2], new HashMap<>());
            dest2label2src.get(edge[2]).putIfAbsent(edge[1], new ArrayList<>());
            dest2label2src.get(edge[2]).get(edge[1]).add(edge[0]);

//            label2src2dest.putIfAbsent(edge[1], new HashMap<>());
//            label2src2dest.get(edge[1]).putIfAbsent(edge[0], new ArrayList<>());
//            label2src2dest.get(edge[1]).get(edge[0]).add(edge[2]);
//
//            label2dest2src.putIfAbsent(edge[1], new HashMap<>());
//            label2dest2src.get(edge[1]).putIfAbsent(edge[2], new ArrayList<>());
//            label2dest2src.get(edge[1]).get(edge[2]).add(edge[0]);

            line = csvReader.readLine();
        }

        csvReader.close();

        Set<Integer> allVertices = new HashSet<>(src2label2dest.keySet());
        allVertices.addAll(dest2label2src.keySet());
        NUM_SAMPLES = (int) (allVertices.size() * 0.0001);

        extend.add(src2label2dest);
        extend.add(dest2label2src);

        endTime = System.currentTimeMillis();
        System.out.println("Graph Loading: " + ((endTime - startTime) / 1000.0) + " sec");
        System.out.println("NUM_SAMPLES: " + NUM_SAMPLES);
    }

    private double computeSD(Map<Long, Long> histogram, double mean, long numTotalBases) {
        double sd = 0;

        for (long entry: histogram.keySet()) {
            sd += (Math.pow(entry - mean, 2) / numTotalBases) * histogram.get(entry);
        }

        return Math.sqrt(sd);
    }

    private void computeCV() throws Exception {
//        computePathDist();
//        saveDistribution("path.csv");

        computeStarDist();
        saveDistribution("star.csv");

        computeForkDist();
        saveDistribution("fork.csv");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println();

        Uniformity uniformity = new Uniformity(args[0]);
        uniformity.computeCV();
    }
}
