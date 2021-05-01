package Graphflow.Parallel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class ComputeStarDist implements Runnable {
    private int threadId;
    private String destFile;

    private Map<Integer, Map<Integer, List<Integer>>> src2label2dest;
    private Map<Integer, Map<Integer, List<Integer>>> dest2label2src;
    private List<Map<Integer, Map<Integer, List<Integer>>>> extend = new ArrayList<>();

    // (orientation string) + base label seq -> extension label -> histogram
    private Map<String, Map<String, Map<Long, Long>>> distribution = new HashMap<>();

    private final String BASE = "";
    private final Long TOTAL = 0L;

    private Map<String, Integer> numOcc = new HashMap<>();

    private int[][] dirGroups;

    public void run() {
        Set<Integer> allVertices = new HashSet<>(src2label2dest.keySet());
        allVertices.addAll(dest2label2src.keySet());
        Set<Integer> centers = Util.getSamples(new ArrayList<>(allVertices), allVertices.size());

        List<Set<Integer>> labels = new ArrayList<>();
        TreeMap<Integer, Integer> sortedMap = new TreeMap<>();

        int numCenters = centers.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (int d = 0; d < dirGroups.length; ++d) {
            int[] dirs = dirGroups[d];

            for (Integer center : centers) {
                progress += 100.0 / numCenters;
                System.out.print("\r" + threadId + ": STAR dist: " + (int) progress + "%");

                labels.clear();
                boolean no3Star = false;
                for (int i = 0; i < 4; ++i) {
                    if (i < 3 && !extend.get(dirs[i]).containsKey(center)) {
                        no3Star = true;
                        break;
                    }

                    if (extend.get(dirs[i]).containsKey(center)) {
                        labels.add(new HashSet<>(extend.get(dirs[i]).get(center).keySet()));
                    }
                }
                if (no3Star) continue;

                Set<String> processed3 = new HashSet<>();

                long count1, count2, count3, count4;
                String labelSeq, extLabel;
                for (Integer label1 : labels.get(0)) {
                    for (Integer label2 : labels.get(1)) {
                        if (label1.equals(label2)) continue;
                        for (Integer label3 : labels.get(2)) {
                            if (label1.equals(label3) || label2.equals(label3)) continue;

                            sortedMap.clear();
                            sortedMap.put(label1, dirs[0]);
                            sortedMap.put(label2, dirs[1]);
                            sortedMap.put(label3, dirs[2]);
                            labelSeq = Util.sortEdgeDirByEdge(sortedMap);
                            if (processed3.contains(labelSeq)) continue;
                            processed3.add(labelSeq);

                            count1 = extend.get(dirs[0]).get(center).get(label1).size();
                            count2 = extend.get(dirs[1]).get(center).get(label2).size();
                            count3 = extend.get(dirs[2]).get(center).get(label3).size();

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

                                extLabel = dirs[3] + ";" + label4.toString();
                                count4 = extend.get(dirs[3]).get(center).get(label4).size();

                                distribution.get(labelSeq).putIfAbsent(extLabel, new HashMap<>());
                                distribution.get(labelSeq).get(extLabel).put(
                                    count4,
                                    distribution.get(labelSeq).get(extLabel)
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
        System.out.println("\n" + threadId + ": STAR dist: " +
            ((endTime - startTime) / 1000.0) + " sec");

        try {
            Util.saveDistribution(distribution, numOcc, destFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ComputeStarDist(
        int threadId,
        String destFile,
        Map<Integer, Map<Integer, List<Integer>>> src2label2dest,
        Map<Integer, Map<Integer, List<Integer>>> dest2label2src,
        int[][] dirGroups) {

        this.threadId = threadId;
        this.destFile = destFile;
        this.src2label2dest = src2label2dest;
        this.dest2label2src = dest2label2src;
        this.extend.add(src2label2dest);
        this.extend.add(dest2label2src);
        this.dirGroups = dirGroups;
    }

    private void occurred(String baseLabelSeq) {
        numOcc.put(baseLabelSeq, numOcc.getOrDefault(baseLabelSeq, 0) + 1);
    }
}
