package Graphflow.Parallel;

import Common.Pair;
import Graphflow.Constants;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

public class CatalogueEntropyConstruction implements Runnable {
    int threadId;
    Map<Integer, List<Pair<Integer, Integer>>> label2srcdest;
    Map<Integer, Map<Integer, List<Integer>>> src2label2dest;
    Map<Integer, Map<Integer, List<Integer>>> dest2label2src;
    List<Pair<String, Long>> catalogueEntries;

    // patternType -> baseVList -> baseLabelSeq -> extVList -> extLabel -> extSize -> #instances
    private volatile Map<Integer, Map<String, Map<String, Map<String, Map<String, Map<Integer, Long>>>>>>
            distribution = new HashMap<>();

    public Map<Integer, Map<String, Map<String, Map<String, Map<String, Map<Integer, Long>>>>>>
    getDistribution() {
        return distribution;
    }

    public void run() {
        String[] info;
        for (Pair<String, Long> entry : catalogueEntries) {
            info = entry.key.split(",");
            Integer patternType = Integer.parseInt(info[0]);
            String vList = info[1];
            String labelSeq = info[2];
            computeDist(patternType, vList, labelSeq);
        }
    }

    private void computeStarDist(Integer patternType, String vListString, String labelSeqString) {
        Map<Integer, Map<Integer, List<Integer>>> current2label2next;
        Integer midPhysical, src, dest;

        Integer[] vList = toVList(vListString);
        final Set<Integer> leaves = getLeaves(vList);
        List<Pair<Pair<String, String>, Pair<String, String>>> baseAndExtList =
                splitToBaseAndExt(vListString, labelSeqString, leaves);

        for (Pair<Pair<String, String>, Pair<String, String>> baseAndExt : baseAndExtList) {
            Integer[] baseVList = toVList(baseAndExt.key.key);
            Integer[] extVList = toVList(baseAndExt.key.value);
            Integer[] baseLabelSeq = toLabelSeq(baseAndExt.value.key);
            Integer[] extLabel = toLabelSeq(baseAndExt.value.value);

            if (!label2srcdest.containsKey(baseLabelSeq[0])) {
                continue;
            }

            Map<Integer, Integer> middleVirtual2Physical = new HashMap<>();
            for (Pair<Integer, Integer> srcDest : label2srcdest.get(baseLabelSeq[0])) {
                middleVirtual2Physical.put(baseVList[0], srcDest.key);
                middleVirtual2Physical.put(baseVList[1], srcDest.value);

                long baseCount = 1L;

                for (int i = 2; i < baseVList.length; i += 2) {
                    src = baseVList[i];
                    dest = baseVList[i + 1];

                    Integer currentLabel = baseLabelSeq[i / 2];
                    if (middleVirtual2Physical.containsKey(src)) {
                        current2label2next = src2label2dest;
                        midPhysical = middleVirtual2Physical.get(src);
                    } else {
                        current2label2next = dest2label2src;
                        midPhysical = middleVirtual2Physical.get(dest);
                    }

                    if (!current2label2next.containsKey(midPhysical) ||
                            !current2label2next.get(midPhysical).containsKey(currentLabel)) {
                        baseCount = 0;
                        break;
                    } else {
                        baseCount *= current2label2next.get(midPhysical).get(currentLabel).size();
                    }
                }

                ArrayList<Integer> extCounts = new ArrayList<>();
                for (int i = 0; i < extVList.length; i += 2) {
                    src = extVList[i];
                    dest = extVList[i + 1];
                    if (middleVirtual2Physical.containsKey(src)) {
                        current2label2next = src2label2dest;
                        midPhysical = middleVirtual2Physical.get(src);
                    } else {
                        current2label2next = dest2label2src;
                        midPhysical = middleVirtual2Physical.get(dest);
                    }

                    if (current2label2next.containsKey(midPhysical)) {
                        if (current2label2next.get(midPhysical).containsKey(extLabel[i / 2])) {
                            extCounts.add(current2label2next.get(midPhysical).get(extLabel[i / 2]).size());
                        }
                    }
                }
                int extCount = extCounts.size() == extLabel.length? extCounts.stream().reduce(1, (a, b) -> a * b) : 0;
                addToDistribution(
                        patternType, baseAndExt.key.key, baseAndExt.value.key,
                        baseAndExt.key.value, baseAndExt.value.value, extCount, baseCount);
            }
        }
    }

    private void computePathDist(Integer patternType, String vListString, String labelSeqString) {
        Map<Integer, Map<Integer, List<Integer>>> current2label2next;
        Map<Integer, Map<Integer, List<Integer>>> current2label2next2;
        Integer midPhysical, nextVirtual, src, dest;

        Integer[] vList = toVList(vListString);
        Integer[] labelSeq = toLabelSeq(labelSeqString);
        final Set<Integer> leaves = getLeaves(vList);
        List<Pair<Pair<String, String>, Pair<String, String>>> baseAndExtList =
                splitToBaseAndExt(vListString, labelSeqString, leaves);

        for (Pair<Pair<String, String>, Pair<String, String>> baseAndExt : baseAndExtList) {
            Integer[] baseVList = toVList(baseAndExt.key.key);
            Integer[] extVList = toVList(baseAndExt.key.value);
            Integer[] baseLabelSeq = toLabelSeq(baseAndExt.value.key);
            Integer[] extLabel = toLabelSeq(baseAndExt.value.value);

            Map<Integer, Integer> middleVirtual2Physical = new HashMap<>();
            Integer[] middleE;
            if (labelSeq.length == 4) {
                middleE = getMiddleEdge(baseVList, baseLabelSeq, getLeaves(baseVList));
            } else if (labelSeq.length == 3) {
                middleE = getMiddleEdge(vList, labelSeq, leaves);
            } else {
                System.out.println("ERROR: path of length " + labelSeq.length + " not supported");
                return;
            }
            if (extLabel.length == 2) {
//                System.out.println(s + " base: " + String.valueOf(baseVList[0]) + " " + String.valueOf(baseVList[1]));
                if (leaves.contains(baseVList[0]) || leaves.contains(baseVList[1])) {
                    boolean srcLeaf = leaves.contains(baseVList[0]);
                    int shared = srcLeaf? baseVList[1] : baseVList[0];
                    List<Integer> extBasePhysicals = new ArrayList<>();
                    for (Pair<Integer, Integer> srcDest : label2srcdest.get(baseLabelSeq[0])) {
                        if (!srcLeaf) extBasePhysicals.add(srcDest.key);
                        else extBasePhysicals.add(srcDest.value);
                    }

                    if (extVList[2] == shared || extVList[3] == shared) {
                        Integer temp = extLabel[1];
                        extLabel[1] = extLabel[0];
                        extLabel[0] = temp;
                        extVList = new Integer[] {extVList[2], extVList[3], extVList[0], extVList[1]};
                    }

                    if (extVList[0] == shared) current2label2next = src2label2dest;
                    else current2label2next = dest2label2src;

                    if (leaves.contains(extVList[2])) current2label2next2 = dest2label2src;
                    else current2label2next2 = src2label2dest;

                    for (Integer extBasePhysical : extBasePhysicals) {
                        int extCount = 0;
                        if (current2label2next.containsKey(extBasePhysical)) {
                            if (current2label2next.get(extBasePhysical).containsKey(extLabel[0])) {
                                for (Integer v : current2label2next.get(extBasePhysical).get(extLabel[0])) {
                                    if (current2label2next2.containsKey(v)) {
                                        if (current2label2next2.get(v).containsKey(extLabel[1])) {
                                            extCount += current2label2next2.get(v).get(extLabel[1]).size();
                                        }
                                    }
                                }
                            }
                        }

                        addToDistribution(
                                patternType, baseAndExt.key.key, baseAndExt.value.key,
                                baseAndExt.key.value, baseAndExt.value.value, extCount, 1L);
                    }
                } else {
                    Integer label1;
                    Integer label2;
                    if (extVList[0].equals(baseVList[0])) {
                        current2label2next = src2label2dest;
                        label1 = extLabel[0];
                    } else if (extVList[1].equals(baseVList[0])) {
                        current2label2next = dest2label2src;
                        label1 = extLabel[0];
                    } else if (extVList[2].equals(baseVList[0])) {
                        current2label2next = src2label2dest;
                        label1 = extLabel[1];
                    } else {
                        current2label2next = dest2label2src;
                        label1 = extLabel[1];
                    }

                    if (extVList[0].equals(baseVList[1])) {
                        current2label2next2 = src2label2dest;
                        label2 = extLabel[0];
                    } else if (extVList[1].equals(baseVList[1])) {
                        current2label2next2 = dest2label2src;
                        label2 = extLabel[0];
                    } else if (extVList[2].equals(baseVList[1])) {
                        current2label2next2 = src2label2dest;
                        label2 = extLabel[1];
                    } else {
                        current2label2next2 = dest2label2src;
                        label2 = extLabel[1];
                    }

                    for (Pair<Integer, Integer> srcDest : label2srcdest.get(baseLabelSeq[0])) {
                        int extCount1 = 0;
                        if (current2label2next.containsKey(srcDest.key)) {
                            if (current2label2next.get(srcDest.key).containsKey(label1)) {
                                extCount1 += current2label2next.get(srcDest.key).get(label1).size();
                            }
                        }

                        int extCount2 = 0;
                        if (current2label2next2.containsKey(srcDest.value)) {
                            if (current2label2next2.get(srcDest.value).containsKey(label2)) {
                                extCount2 += current2label2next2.get(srcDest.value).get(label2).size();
                            }
                        }

                        addToDistribution(
                                patternType, baseAndExt.key.key, baseAndExt.value.key,
                                baseAndExt.key.value, baseAndExt.value.value, extCount1 * extCount2, 1L);
                    }

                }
                continue;
            }

            for (Pair<Integer, Integer> srcDest : label2srcdest.get(middleE[2])) {
                middleVirtual2Physical.put(middleE[0], srcDest.key);
                middleVirtual2Physical.put(middleE[1], srcDest.value);
                long baseCount = 0;
                List<Integer> extBasePhysicals = new ArrayList<>();

                for (int i = 0; i < baseVList.length; i += 2) {
                    src = baseVList[i];
                    dest = baseVList[i + 1];
                    if (middleVirtual2Physical.containsKey(src) &&
                            middleVirtual2Physical.containsKey(dest)) continue;

                    Integer currentLabel = baseLabelSeq[i / 2];
                    if (middleVirtual2Physical.containsKey(src)) {
                        current2label2next = src2label2dest;
                        midPhysical = middleVirtual2Physical.get(src);
                        nextVirtual = dest;
                    } else {
                        current2label2next = dest2label2src;
                        midPhysical = middleVirtual2Physical.get(dest);
                        nextVirtual = src;
                    }

                    if (current2label2next.containsKey(midPhysical)) {
                        if (current2label2next.get(midPhysical).containsKey(currentLabel)) {
                            if (leaves.contains(nextVirtual)) {
                                baseCount =
                                        current2label2next.get(midPhysical).get(currentLabel).size();
                            } else {
                                extBasePhysicals =
                                        current2label2next.get(midPhysical).get(currentLabel);
                            }
                        }
                    }
                }

                src = extVList[0];
                dest = extVList[1];
                if (leaves.contains(dest)) {
                    current2label2next = src2label2dest;
                    if (labelSeq.length == 3 && extBasePhysicals.isEmpty()) {
                        extBasePhysicals.add(middleVirtual2Physical.get(src));
                    }
                } else {
                    current2label2next = dest2label2src;
                    if (labelSeq.length == 3 && extBasePhysicals.isEmpty()) {
                        extBasePhysicals.add(middleVirtual2Physical.get(dest));
                    }
                }

                for (Integer extBasePhysical : extBasePhysicals) {
                    int extCount = 0;
                    if (current2label2next.containsKey(extBasePhysical)) {
                        if (current2label2next.get(extBasePhysical).containsKey(extLabel[0])) {
                            extCount =
                                    current2label2next.get(extBasePhysical).get(extLabel[0]).size();
                        }
                    }

                    addToDistribution(
                            patternType, baseAndExt.key.key, baseAndExt.value.key,
                            baseAndExt.key.value, baseAndExt.value.value, extCount, baseCount);
                }
            }
        }
    }

    private void computeForkDist(Integer patternType, String vListString, String labelSeqString) {
        Map<Integer, Map<Integer, List<Integer>>> current2label2next;
        Integer midPhysical, src, dest;

        Integer[] vList = toVList(vListString);
        Integer[] labelSeq = toLabelSeq(labelSeqString);
        final Set<Integer> leaves = getLeaves(vList);
        List<Pair<Pair<String, String>, Pair<String, String>>> baseAndExtList =
                splitToBaseAndExt(vListString, labelSeqString, leaves);

        Integer[] middleE = getMiddleEdge(vList, labelSeq, leaves);
        Map<Integer, Integer> middleVirtual2Physical = new HashMap<>();

        for (Pair<Pair<String, String>, Pair<String, String>> baseAndExt : baseAndExtList) {
            Integer[] baseVList = toVList(baseAndExt.key.key);
            Integer[] extVList = toVList(baseAndExt.key.value);
            Integer[] baseLabelSeq = toLabelSeq(baseAndExt.value.key);
            Integer[] extLabel = toLabelSeq(baseAndExt.value.value);

            for (Pair<Integer, Integer> srcDest : label2srcdest.get(middleE[2])) {
                middleVirtual2Physical.put(middleE[0], srcDest.key);
                middleVirtual2Physical.put(middleE[1], srcDest.value);
                long baseCount = 1L;

                for (int i = 0; i < baseVList.length; i += 2) {
                    src = baseVList[i];
                    dest = baseVList[i + 1];
                    if (middleVirtual2Physical.containsKey(src) &&
                            middleVirtual2Physical.containsKey(dest)) continue;

                    Integer currentLabel = baseLabelSeq[i / 2];
                    if (middleVirtual2Physical.containsKey(src)) {
                        current2label2next = src2label2dest;
                        midPhysical = middleVirtual2Physical.get(src);
                    } else {
                        current2label2next = dest2label2src;
                        midPhysical = middleVirtual2Physical.get(dest);
                    }

                    if (!current2label2next.containsKey(midPhysical) ||
                            !current2label2next.get(midPhysical).containsKey(currentLabel)) {
                        baseCount = 0;
                        break;
                    } else {
                        baseCount *= current2label2next.get(midPhysical).get(currentLabel).size();
                    }
                }

                src = extVList[0];
                dest = extVList[1];
                if (middleVirtual2Physical.containsKey(src)) {
                    current2label2next = src2label2dest;
                    midPhysical = middleVirtual2Physical.get(src);
                } else {
                    current2label2next = dest2label2src;
                    midPhysical = middleVirtual2Physical.get(dest);
                }

                int extCount = 0;
                if (current2label2next.containsKey(midPhysical)) {
                    if (current2label2next.get(midPhysical).containsKey(extLabel[0])) {
                        extCount = current2label2next.get(midPhysical).get(extLabel[0]).size();
                    }
                }

                addToDistribution(
                        patternType, baseAndExt.key.key, baseAndExt.value.key,
                        baseAndExt.key.value, baseAndExt.value.value, extCount, baseCount);
            }
        }
    }

    private void computeDist(Integer patternType, String vListString, String labelSeqString) {
        Integer[] vList = toVList(vListString);
        int type = getEntryType(vList);
        if (type == Constants.C_PATH) {
            computePathDist(patternType, vListString, labelSeqString);
        } else if (type == Constants.C_STAR) {
            computeStarDist(patternType, vListString, labelSeqString);
        } else if (type == Constants.C_FORK) {
            computeForkDist(patternType, vListString, labelSeqString);
        } else {
            System.err.println("ERROR: unrecognized pattern");
        }
    }

    private int getEntryType(Integer[] vList) {
        final int queryLength = vList.length / 2;
        Map<Integer, Integer> occurrences = new HashMap<>();
        for (Integer v : vList) {
            occurrences.put(v, occurrences.getOrDefault(v, 0) + 1);
        }

        int maxCount = Integer.MIN_VALUE;
        for (Integer count : occurrences.values()) {
            maxCount = Math.max(maxCount, count);
        }

        switch (queryLength) {
            case 4:
                if (maxCount == 4) return Constants.C_STAR;
                else if (maxCount == 3) return Constants.C_FORK;
                else if (maxCount == 2) return Constants.C_PATH;
            case 3:
                if (maxCount == 3) return Constants.C_STAR;
                else if (maxCount == 2) return Constants.C_PATH;
            case 2:
                return Constants.C_STAR;
        }

        return -1;
    }

    private Integer[] getMiddleEdge(Integer[] vList, Integer[] labelSeq, Set<Integer> leaves) {
        Integer src, dest;
        for (int i = 0; i < vList.length; i += 2) {
            src = vList[i];
            dest = vList[i + 1];
            if (!leaves.contains(src) && !leaves.contains(dest)) {
                return new Integer[]{src, dest, labelSeq[i / 2]};
            }
        }

        System.err.println("ERROR: getMiddleEdge - should not reach here");
        return null;
    }

    private Integer getExtBaseV(Integer[] extVList, Set<Integer> leaves) {
        for (Integer v : extVList) {
            if (!leaves.contains(v)) {
                return v;
            }
        }

        System.err.println("ERROR: getExtBaseV - should not reach here");
        return null;
    }

    private Integer[] toVList(String vListString) {
        String[] splitted = vListString.split(";");
        Integer[] vList = new Integer[splitted.length * 2];
        for (int i = 0; i < splitted.length; i++) {
            String[] srcDest = splitted[i].split("-");
            vList[i * 2] = Integer.parseInt(srcDest[0]);
            vList[i * 2 + 1] = Integer.parseInt(srcDest[1]);
        }
        return vList;
    }

    private List<Pair<Pair<String, String>, Pair<String, String>>> splitToBaseAndExt(
            String vListString, String labelSeqString, Set<Integer> leaves) {

        List<Pair<Pair<String, String>, Pair<String, String>>> splits = new ArrayList<>();

        String[] edges = vListString.split(";");
        String[] labelSeq = labelSeqString.split("->");
        for (int i = 0; i < edges.length; ++i) {
            String[] srcDest = edges[i].split("-");
//            if (leaves.contains(Integer.parseInt(srcDest[0])) ||
//                leaves.contains(Integer.parseInt(srcDest[1]))) {

            StringJoiner vListSj = new StringJoiner(";");
            StringJoiner labelSeqSj = new StringJoiner("->");
            for (int j = 0; j < edges.length; ++j) {
                if (i == j) continue;
                vListSj.add(edges[j]);
                labelSeqSj.add(labelSeq[j]);
            }
            if (leaves.contains(Integer.parseInt(srcDest[0])) ||
                    leaves.contains(Integer.parseInt(srcDest[1]))) {
                splits.add(new Pair<>(
                        new Pair<>(vListSj.toString(), edges[i]),
                        new Pair<>(labelSeqSj.toString(), labelSeq[i])
                ));
            }
//            System.out.println("1: " + edges[i] + " " + vListSj.toString());
            splits.add(new Pair<>(
                    new Pair<>(edges[i], vListSj.toString()),
                    new Pair<>(labelSeq[i], labelSeqSj.toString())
            ));
//            }
        }

        return splits;
    }

    private Integer[] toLabelSeq(String labelSeqString) {
        String[] splitted = labelSeqString.split("->");
        Integer[] labelSeq = new Integer[splitted.length];
        for (int i = 0; i < splitted.length; ++i) {
            labelSeq[i] = Integer.parseInt(splitted[i]);
        }
        return labelSeq;
    }

    private Set<Integer> getLeaves(Integer[] vList) {
        Map<Integer, Integer> occurrences = new HashMap<>();
        for (Integer v : vList) {
            occurrences.put(v, occurrences.getOrDefault(v, 0) + 1);
        }

        Set<Integer> leaves = new HashSet<>();
        for (Integer v : occurrences.keySet()) {
            if (occurrences.get(v).equals(1)) {
                leaves.add(v);
            }
        }
        return leaves;
    }

    private void addToDistribution(
            Integer patternType, String baseVList, String baseLabelSeq, String extVList, String extLabel,
            int extSize, long contribution) {

        distribution.putIfAbsent(patternType, new HashMap<>());
        distribution.get(patternType).putIfAbsent(baseVList, new HashMap<>());
        distribution.get(patternType).get(baseVList).putIfAbsent(baseLabelSeq, new HashMap<>());
        distribution.get(patternType).get(baseVList).get(baseLabelSeq)
                .putIfAbsent(extVList, new HashMap<>());
        distribution.get(patternType).get(baseVList).get(baseLabelSeq).get(extVList)
                .putIfAbsent(extLabel, new HashMap<>());
        long count = distribution.get(patternType).get(baseVList).get(baseLabelSeq)
                .get(extVList).get(extLabel).getOrDefault(extSize, 0L);

        distribution.get(patternType).get(baseVList).get(baseLabelSeq).get(extVList).get(extLabel)
                .put(extSize, count + contribution);
    }

    private void persistDist() throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter("dist_" + threadId + ".csv"));
        for (Integer patternType : distribution.keySet()) {
            for (String baseVList : distribution.get(patternType).keySet()) {
                for (String baseLabelSeq : distribution.get(patternType).get(baseVList).keySet()) {
                    Map<String, Map<String, Map<Integer, Long>>> extDist =
                            distribution.get(patternType).get(baseVList).get(baseLabelSeq);

                    for (String extVList : extDist.keySet()) {
                        for (String extLabel : extDist.get(extVList).keySet()) {
                            StringJoiner sj = new StringJoiner(",");
                            sj.add(patternType.toString());
                            sj.add(baseVList);
                            sj.add(baseLabelSeq);
                            sj.add(extVList);
                            sj.add(extLabel);

                            for (Integer extSize : extDist.get(extVList).get(extLabel).keySet()) {
                                sj.add(extSize.toString());
                                sj.add(extDist.get(extVList).get(extLabel).get(extSize).toString());
                            }

                            writer.write(sj.toString() + "\n");
                        }
                    }
                }
            }
        }
        writer.close();
    }

    public CatalogueEntropyConstruction(
            int threadId,
            Map<Integer, List<Pair<Integer, Integer>>> label2srcdest,
            Map<Integer, Map<Integer, List<Integer>>> src2label2dest,
            Map<Integer, Map<Integer, List<Integer>>> dest2label2src,
            List<Pair<String, Long>> catalogueEntries) {

        this.threadId = threadId;
        this.label2srcdest = label2srcdest;
        this.src2label2dest = src2label2dest;
        this.dest2label2src = dest2label2src;
        this.catalogueEntries = catalogueEntries;
    }
}
