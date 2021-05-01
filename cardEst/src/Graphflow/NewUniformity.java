package Graphflow;

import Graphflow.Parallel.ComputeForkDist;
import Graphflow.Parallel.ComputePathDist;
import Graphflow.Parallel.ComputeStarDist;
import com.sun.tools.internal.jxc.ap.Const;

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

public class NewUniformity {
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
        Set<Integer> rightMids = getSamples(new ArrayList<>(allVertices), -1);

        String orientAndLabelSeq, extendOrientLabelSeq;

        long branchCount, extCount;

        List<Integer> dirs = new ArrayList<>();
        StringJoiner prefix = new StringJoiner("->");

        int numSamples = rightMids.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (int i = 0; i < 4; ++i) {
            int dir = random.nextInt(extend.size());
            dirs.add(dir);
            if (i < 3) {
                prefix.add(Integer.toString(dir));
            }
        }

        for (Integer rightMid : rightMids) {
            progress += 100.0 / numSamples;
            System.out.print("\rPATH dist: " + (int) progress + "%");

            if (!extend.get(dirs.get(0)).containsKey(rightMid)) continue;

            for (Integer label1 : extend.get(dirs.get(0)).get(rightMid).keySet()) {
                for (Integer left1 : extend.get(dirs.get(0)).get(rightMid).get(label1)) {
                    if (!extend.get(dirs.get(1)).containsKey(left1)) continue;
                    for (Integer label2 : extend.get(dirs.get(1)).get(left1).keySet()) {
                        if (label1.equals(label2)) continue;
                        for (Integer left2 : extend.get(dirs.get(1)).get(left1).get(label2)) {
                            if (!extend.get(dirs.get(2)).containsKey(left2)) continue;
                            for (Integer label3 : extend.get(dirs.get(2)).get(left2).keySet()) {
                                if (label1.equals(label3) || label2.equals(label3)) continue;
                                if (!extend.get(dirs.get(3)).containsKey(rightMid)) continue;

                                orientAndLabelSeq = prefix.toString() + ",";
                                orientAndLabelSeq += label1 + "->" + label2 + "->" + label3;

                                branchCount = extend.get(dirs.get(2)).get(left2).get(label3).size();

                                distribution.putIfAbsent(orientAndLabelSeq, new HashMap<>());
                                distribution.get(orientAndLabelSeq).putIfAbsent(BASE, new HashMap<>());
                                distribution.get(orientAndLabelSeq).get(BASE).put(
                                    TOTAL,
                                    distribution.get(orientAndLabelSeq).get(BASE)
                                        .getOrDefault(TOTAL, 0L) + branchCount
                                );

                                for (Integer extLabel : extend.get(dirs.get(3)).get(rightMid).keySet()) {
                                    if (label1.equals(extLabel) || label2.equals(extLabel) ||
                                        label3.equals(extLabel)) continue;

                                    extCount = extend.get(dirs.get(3)).get(rightMid).get(extLabel).size();

                                    extendOrientLabelSeq = dirs.get(3) + "," + extLabel;

                                    distribution.get(orientAndLabelSeq).putIfAbsent(extendOrientLabelSeq, new HashMap<>());
                                    distribution.get(orientAndLabelSeq).get(extendOrientLabelSeq).put(
                                        extCount,
                                        distribution.get(orientAndLabelSeq).get(extendOrientLabelSeq)
                                            .getOrDefault(extCount, 0L) + branchCount
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

    private void computeStarRandomDist() {
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

        for (int i = 0; i < 4; ++i) {
            int dir = random.nextInt(extend.size());
            dirs.add(dir);
        }

        for (Integer center : centers) {
            progress += 100.0 / numCenters;
            System.out.print("\rSTAR dist: " + (int) progress + "%");

            labels.clear();
            boolean no3Star = false;
            for (int i = 0; i < 4; ++i) {
                if (i < 3 && !extend.get(dirs.get(i)).containsKey(center)) {
                    no3Star = true;
                    break;
                }

                if (extend.get(dirs.get(i)).containsKey(center)) {
                    labels.add(new HashSet<>(extend.get(dirs.get(i)).get(center).keySet()));
                }
            }
            if (no3Star) continue;

            Set<String> processed3 = new HashSet<>();

            String[] labelSeqArray = new String[3];
            long count1, count2, count3, count4;
            String labelSeq;
            for (Integer label1 : labels.get(0)) {
                for (Integer label2 : labels.get(1)) {
                    if (label1.equals(label2)) continue;
                    for (Integer label3 : labels.get(2)) {
                        if (label1.equals(label3) || label2.equals(label3)) continue;

                        count1 = extend.get(dirs.get(0)).get(center).get(label1).size();
                        count2 = extend.get(dirs.get(1)).get(center).get(label2).size();
                        count3 = extend.get(dirs.get(2)).get(center).get(label3).size();

                        labelSeqArray[0] = label1.toString();
                        labelSeqArray[1] = label2.toString();
                        labelSeqArray[2] = label3.toString();
                        Arrays.sort(labelSeqArray);
                        labelSeq = String.join("->", labelSeqArray);
                        if (processed3.contains(labelSeq)) continue;
                        processed3.add(labelSeq);

                        distribution.putIfAbsent(labelSeq, new HashMap<>());
                        distribution.get(labelSeq).putIfAbsent(BASE, new HashMap<>());
                        distribution.get(labelSeq).get(BASE).put(
                            TOTAL,
                            distribution.get(labelSeq).get(BASE)
                                .getOrDefault(TOTAL, 0L) + count1 * count2 * count3
                        );

                        if (labels.size() < 4) continue;
                        for (Integer label4 : labels.get(3)) {
                            if (label1.equals(label4) || label2.equals(label4) || label3.equals(label4)) continue;
                            count4 = extend.get(dirs.get(3)).get(center).get(label4).size();

                            distribution.get(labelSeq).putIfAbsent(label4.toString(), new HashMap<>());
                            distribution.get(labelSeq).get(label4.toString()).put(
                                count4,
                                distribution.get(labelSeq).get(label4.toString())
                                    .getOrDefault(count4, 0L) + count1 * count2 * count3
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

    private void computeStarDistSingleThread() {
        Set<Integer> allVertices = new HashSet<>(src2label2dest.keySet());
        allVertices.addAll(dest2label2src.keySet());
        Set<Integer> centers = getSamples(new ArrayList<>(allVertices), -1);

        List<List<Integer>> dirGroups = new ArrayList<>();
        List<Set<Integer>> labels = new ArrayList<>();

        int numCenters = centers.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        dirGroups.add(new ArrayList<>());
        dirGroups.get(0).add(Constants.BACKWARD);
        dirGroups.get(0).add(Constants.BACKWARD);
        dirGroups.get(0).add(Constants.FORWARD);
        dirGroups.get(0).add(Constants.FORWARD);
        dirGroups.add(new ArrayList<>());
        dirGroups.get(1).add(Constants.BACKWARD);
        dirGroups.get(1).add(Constants.FORWARD);
        dirGroups.get(1).add(Constants.FORWARD);
        dirGroups.get(1).add(Constants.BACKWARD);

        for (List<Integer> dirs : dirGroups) {
            for (Integer center : centers) {
                progress += 100.0 / numCenters;
                System.out.print("\rSTAR dist: " + (int) progress + "%");

                labels.clear();
                boolean no3Star = false;
                for (int i = 0; i < 4; ++i) {
                    if (i < 3 && !extend.get(dirs.get(i)).containsKey(center)) {
                        no3Star = true;
                        break;
                    }

                    if (extend.get(dirs.get(i)).containsKey(center)) {
                        labels.add(new HashSet<>(extend.get(dirs.get(i)).get(center).keySet()));
                    }
                }
                if (no3Star) continue;

                Set<String> processed3 = new HashSet<>();

                String[] labelSeqArray = new String[3];
                long count1, count2, count3, count4;
                String labelSeq;
                for (Integer label1 : labels.get(0)) {
                    for (Integer label2 : labels.get(1)) {
                        if (label1.equals(label2)) continue;
                        for (Integer label3 : labels.get(2)) {
                            if (label1.equals(label3) || label2.equals(label3)) continue;

                            count1 = extend.get(dirs.get(0)).get(center).get(label1).size();
                            count2 = extend.get(dirs.get(1)).get(center).get(label2).size();
                            count3 = extend.get(dirs.get(2)).get(center).get(label3).size();

                            labelSeqArray[0] = label1.toString();
                            labelSeqArray[1] = label2.toString();
                            labelSeqArray[2] = label3.toString();
                            Arrays.sort(labelSeqArray);
                            labelSeq = String.join("->", labelSeqArray);
                            if (processed3.contains(labelSeq)) continue;
                            processed3.add(labelSeq);

                            distribution.putIfAbsent(labelSeq, new HashMap<>());
                            distribution.get(labelSeq).putIfAbsent(BASE, new HashMap<>());
                            distribution.get(labelSeq).get(BASE).put(
                                TOTAL,
                                distribution.get(labelSeq).get(BASE)
                                    .getOrDefault(TOTAL, 0L) + count1 * count2 * count3
                            );

                            if (labels.size() < 4) continue;
                            for (Integer label4 : labels.get(3)) {
                                if (label1.equals(label4) || label2.equals(label4) || label3.equals(label4)) continue;
                                count4 = extend.get(dirs.get(3)).get(center).get(label4).size();

                                distribution.get(labelSeq).putIfAbsent(label4.toString(), new HashMap<>());
                                distribution.get(labelSeq).get(label4.toString()).put(
                                    count4,
                                    distribution.get(labelSeq).get(label4.toString())
                                        .getOrDefault(count4, 0L) + count1 * count2 * count3
                                );
                                occurred(labelSeq);
                            }
                        }
                    }
                }
            }
        }

        endTime = System.currentTimeMillis();
        System.out.println("\nSTAR dist: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void computeForkExtDist() {
        Random random = new Random(0);

        Set<Integer> allVertices = new HashSet<>(src2label2dest.keySet());
        allVertices.addAll(dest2label2src.keySet());
        Set<Integer> extMids = getSamples(new ArrayList<>(allVertices), -1);

        List<Integer> dirs = new ArrayList<>();
        String labelSeq, forkLabels;
        StringJoiner prefix = new StringJoiner("->");
        long branchCount, extCount;

        int numExtMids = extMids.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (int i = 0; i < 4; ++i) {
            int dir = random.nextInt(extend.size());
            dirs.add(dir);
            if (i < 3) {
                prefix.add(Integer.toString(dir));
            }
        }

        String[] sorted = new String[2];

        for (Integer extMid : extMids) {
            progress += 100.0 / numExtMids;
            System.out.print("\rFORK dist: " + (int) progress + "%");

            if (!extend.get(dirs.get(1)).containsKey(extMid)) continue;

            for (Integer label1 : extend.get(dirs.get(1)).get(extMid).keySet()) {
                for (Integer center : extend.get(dirs.get(1)).get(extMid).get(label1)) {
                    if (!extend.get(dirs.get(2)).containsKey(center)) continue;
                    if (!extend.get(dirs.get(3)).containsKey(center)) continue;

                    Set<String> forks = new HashSet<>();

                    for (Integer label2 : extend.get(dirs.get(2)).get(center).keySet()) {
                        if (label1.equals(label2)) continue;
                        for (Integer label3 : extend.get(dirs.get(3)).get(center).keySet()) {
                            if (label1.equals(label3) || label2.equals(label3)) continue;

                            sorted[0] = label2.toString();
                            sorted[1] = label3.toString();
                            Arrays.sort(sorted);
                            forkLabels = String.join("->", sorted);
                            if (forks.contains(forkLabels)) continue;
                            forks.add(forkLabels);

                            labelSeq = label1 + "->" + forkLabels;
                            branchCount = extend.get(dirs.get(2)).get(center).get(label2).size();
                            branchCount *= extend.get(dirs.get(3)).get(center).get(label3).size();

                            distribution.putIfAbsent(labelSeq, new HashMap<>());
                            distribution.get(labelSeq).putIfAbsent(BASE, new HashMap<>());
                            distribution.get(labelSeq).get(BASE).put(
                                TOTAL,
                                distribution.get(labelSeq).get(BASE)
                                    .getOrDefault(TOTAL, 0L) + branchCount
                            );

                            for (Integer extLabel : extend.get(dirs.get(0)).get(extMid).keySet()) {
                                if (label1.equals(extLabel) || label2.equals(extLabel) ||
                                    label3.equals(extLabel)) continue;

                                    extCount = extend.get(dirs.get(0)).get(extMid).get(extLabel).size();

                                    distribution.get(labelSeq).putIfAbsent(extLabel.toString(), new HashMap<>());
                                    distribution.get(labelSeq).get(extLabel.toString()).put(
                                        extCount,
                                        distribution.get(labelSeq).get(extLabel.toString())
                                            .getOrDefault(extCount, 0L) + branchCount
                                    );
                                    occurred(labelSeq);
                                }
                            }
                        }
                    }
                }
        }

        endTime = System.currentTimeMillis();
        System.out.println("\nFORK dist: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void computeForkDistSingleThread() {
        Set<Integer> allVertices = new HashSet<>(src2label2dest.keySet());
        allVertices.addAll(dest2label2src.keySet());
        Set<Integer> centers = getSamples(new ArrayList<>(allVertices), -1);

        List<List<Integer>> dirGroups = new ArrayList<>();
        String labelSeq, extString;
        long baseCount, extCount;

        int numCenters = centers.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        // TODO: create 6 threads, each thread (worker) still take dirGroups and compute
        // TODO: so that we can have all CVs
        dirGroups.add(new ArrayList<>());
        dirGroups.get(0).add(Constants.FORWARD);
        dirGroups.get(0).add(Constants.BACKWARD);
        dirGroups.get(0).add(Constants.FORWARD);
        dirGroups.get(0).add(Constants.FORWARD);
        dirGroups.add(new ArrayList<>());
        dirGroups.get(1).add(Constants.FORWARD);
        dirGroups.get(1).add(Constants.BACKWARD);
        dirGroups.get(1).add(Constants.FORWARD);
        dirGroups.get(1).add(Constants.BACKWARD);
        String[] bases = new String[] { "0-1;0-4;4-5", "0-1;0-4;4-6" };
        String[] exts = new String[] { "4-6", "2-4" };

        for (int i = 0; i < dirGroups.size(); ++i) {
            List<Integer> dirs = dirGroups.get(i);
            for (Integer center : centers) {
                progress += 100.0 / numCenters;
                System.out.print("\rFORK dist: " + (int) progress + "%");

                if (!extend.get(dirs.get(0)).containsKey(center)) continue;

                for (Integer fork1 : extend.get(dirs.get(0)).get(center).keySet()) {
                    int fork1Count = extend.get(dirs.get(0)).get(center).get(fork1).size();

                    if (!extend.get(dirs.get(1)).containsKey(center)) continue;
                    for (Integer label1 : extend.get(dirs.get(1)).get(center).keySet()) {
                        if (label1.equals(fork1)) continue;
                        for (Integer extMid : extend.get(dirs.get(1)).get(center).get(label1)) {
                            if (!extend.get(dirs.get(2)).containsKey(extMid)) continue;

                            for (Integer label2 : extend.get(dirs.get(2)).get(extMid).keySet()) {
                                if (label1.equals(label2) || fork1.equals(label2)) continue;

                                labelSeq = bases[i] + "," + label2 + "->" + label1 + "->" + fork1;
                                baseCount = fork1Count;
                                baseCount *= extend.get(dirs.get(2)).get(extMid).get(label2).size();

                                distribution.putIfAbsent(labelSeq, new HashMap<>());
                                distribution.get(labelSeq).putIfAbsent(BASE, new HashMap<>());
                                distribution.get(labelSeq).get(BASE).put(
                                    TOTAL,
                                    distribution.get(labelSeq).get(BASE)
                                        .getOrDefault(TOTAL, 0L) + baseCount
                                );

                                if (!extend.get(dirs.get(3)).containsKey(center)) continue;

                                for (Integer extLabel : extend.get(dirs.get(3)).get(center).keySet()) {
                                    if (label1.equals(extLabel) || label2.equals(extLabel) ||
                                        fork1.equals(extLabel)) continue;

                                    extString = exts[i] + "," + extLabel.toString();
                                    extCount = extend.get(dirs.get(3)).get(center).get(extLabel).size();

                                    distribution.get(labelSeq).putIfAbsent(extString, new HashMap<>());
                                    distribution.get(labelSeq).get(extString).put(
                                        extCount,
                                        distribution.get(labelSeq).get(extString)
                                            .getOrDefault(extCount, 0L) + baseCount
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
            long baseTotal = distribution.get(labelSeq).get(BASE).get(TOTAL);
            for (String extendedEdge : distribution.get(labelSeq).keySet()) {
                if (extendedEdge.equals(BASE)) continue;
//                if (numOcc.get(labelSeq) <= 10) continue;

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

                double mean = ((double) total) / baseTotal;
                double sd = computeSD(distribution.get(labelSeq).get(extendedEdge), mean, baseTotal);
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

    private NewUniformity(String graphFile) throws Exception {
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
        NUM_SAMPLES = allVertices.size();

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

    private List<Thread> computeDistOfPattern(final int type, final int[][][] DIR_GROUP) throws
        Exception {
        List<Thread> threads = new ArrayList<>();
        Runnable computeDist;
        Thread thread;

        for (int i = 0; i < DIR_GROUP.length; ++i) {
            switch (type) {
                case Constants.C_FORK:
                    computeDist = new ComputeForkDist(
                        i + 1,
                        "fork_pi_q_" + (i + 1) + ".csv",
                        src2label2dest,
                        dest2label2src,
                        DIR_GROUP[i]);
                    break;
                case Constants.C_STAR:
                    computeDist = new ComputeStarDist(
                        i + 1,
                        "star_fork_q_" + (i + 1) + ".csv",
                        src2label2dest,
                        dest2label2src,
                        DIR_GROUP[i]);
                    break;
                case Constants.C_PATH:
                    computeDist = new ComputePathDist(
                        i + 1,
                        "path_pi_q_" + (i + 1) + ".csv",
                        src2label2dest,
                        dest2label2src,
                        DIR_GROUP[i]);
                    break;
                default:
                    return new ArrayList<>();
            }

            thread = new Thread(computeDist);
            threads.add(thread);
            thread.start();
        }

        return threads;
    }

    private void computeCV() throws Exception {
//        computePathDist();
//        saveDistribution("path.csv");

//        computeStarDist();
//        saveDistribution("star_fork_q.csv");
//
//        computeForkDist();
//        saveDistribution("fork_fork_q.csv");

        List<Thread> threads = new ArrayList<>();

//        threads.addAll(computeDistOfPattern(Constants.C_STAR, Constants.STAR_OF_FORK_Q_DIR_GROUPS));
        threads.addAll(computeDistOfPattern(Constants.C_FORK, Constants.FORK_OF_PI_Q_DIR_GROUPS));
        threads.addAll(computeDistOfPattern(Constants.C_PATH, Constants.PATH_OF_PI_Q_DIR_GROUPS));

        for (Thread t : threads) {
            t.join();
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println();

        NewUniformity uniformity = new NewUniformity(args[0]);
        uniformity.computeCV();
    }
}
