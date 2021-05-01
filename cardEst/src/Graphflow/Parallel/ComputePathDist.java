package Graphflow.Parallel;

import Common.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ComputePathDist implements Runnable {
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
        Set<Integer> starters = Util.getSamples(new ArrayList<>(allVertices), allVertices.size());

        String labelSeq, extString;
        long baseCount, extCount;
        Pair<String, String> baseAndExtDir;

        int numCenters = starters.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (int i = 0; i < dirGroups.length; ++i) {
            int[] dirs = dirGroups[i];
            baseAndExtDir = Util.dirsToBaseAndExt(dirs);

            for (Integer leftV : starters) {
                progress += 100.0 / numCenters;
                System.out.print("\r" + threadId + ": PATH dist: " + (int) progress + "%");

                if (!extend.get(dirs[0]).containsKey(leftV)) continue;

                for (Integer head : extend.get(dirs[0]).get(leftV).keySet()) {
                    int headCount = extend.get(dirs[0]).get(leftV).get(head).size();

                    if (!extend.get(dirs[1]).containsKey(leftV)) continue;
                    for (Integer mid1 : extend.get(dirs[1]).get(leftV).keySet()) {
                        if (mid1.equals(head)) continue;
                        for (Integer centerV : extend.get(dirs[1]).get(leftV).get(mid1)) {
                            if (!extend.get(dirs[2]).containsKey(centerV)) continue;

                            for (Integer mid2 : extend.get(dirs[2]).get(centerV).keySet()) {
                                if (mid1.equals(mid2) || head.equals(mid2)) continue;

                                labelSeq = baseAndExtDir.getKey() + ";" +
                                    head + "-" + mid1 + "-" + mid2;

                                for (Integer rightV : extend.get(dirs[2]).get(centerV).get(mid2)) {
                                    distribution.putIfAbsent(labelSeq, new HashMap<>());
                                    distribution.get(labelSeq).putIfAbsent(BASE, new HashMap<>());
                                    distribution.get(labelSeq).get(BASE).put(
                                        TOTAL,
                                        distribution.get(labelSeq).get(BASE)
                                            .getOrDefault(TOTAL, 0L) + headCount
                                    );

                                    if (!extend.get(dirs[3]).containsKey(rightV)) continue;

                                    for (Integer extLabel : extend.get(dirs[3]).get(rightV).keySet()) {
                                        if (mid1.equals(extLabel) || mid2.equals(extLabel) ||
                                            head.equals(extLabel)) continue;

                                        extString = baseAndExtDir.getValue() + ";" + extLabel.toString();
                                        extCount = extend.get(dirs[3]).get(rightV).get(extLabel).size();

                                        distribution.get(labelSeq).putIfAbsent(extString, new HashMap<>());
                                        distribution.get(labelSeq).get(extString).put(
                                            extCount,
                                            distribution.get(labelSeq).get(extString)
                                                .getOrDefault(extCount, 0L) + headCount
                                        );
                                        occurred(labelSeq);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        endTime = System.currentTimeMillis();
        System.out.println("\n" + threadId + ": PATH dist: " +
            ((endTime - startTime) / 1000.0) + " sec");

        try {
            Util.saveDistribution(distribution, numOcc, destFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ComputePathDist(
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
