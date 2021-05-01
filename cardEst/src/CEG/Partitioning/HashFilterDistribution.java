package CEG.Partitioning;

import Common.Pair;
import Common.Util;
import Graphflow.Constants;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashFilterDistribution {
    Map<Integer, List<Pair<Integer, Integer>>> label2srcdest = new HashMap<>();
    Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();

    // hashIdComb -> vList -> labelSeq -> count
    volatile Map<String, Map<String, Map<String, Long>>> catalogue;
    // hashIdComb -> baseVList -> baseLabelSeq -> extVList -> extLabel -> maxdeg
    volatile Map<String, Map<String, Map<String, Map<String, Map<String, Integer>>>>> maxDeg;

    // hashIdComb -> vList -> label -> cardinality
    volatile Map<String, Map<String, Map<String, Long>>> singleEdge;

    public void compute2(String vListString, String labelSeqString, String scheme) {
        Map<Integer, Integer> toBudget = mapVirtual2budgetOrHash(vListString, scheme);

        String[] vListEdges = vListString.split(";");
        String[] labelStrings = labelSeqString.split("->");

        Integer[] vList = Util.toVList(vListString);
        Integer[] labels = Util.toLabelSeq(labelSeqString);

        Integer mid = getMid(vList);
        Integer firstLeaf = getFirstLeaf(vList, mid);
        Integer secLeaf = getSecLeaf(vList, mid);
        int firstDir = mid < firstLeaf ? Constants.FORWARD : Constants.BACKWARD;
        int secDir = mid < secLeaf ? Constants.FORWARD : Constants.BACKWARD;
        Map<Integer, Map<Integer, List<Integer>>> secMid2Next = determineMid2Next(vList, mid, 2);

        Integer midBudget = toBudget.get(mid);
        Integer firstLeafBudget = toBudget.get(firstLeaf);
        Integer secLeafBudget = toBudget.get(secLeaf);

        // hashIdComb -> mid -> count
        Map<String, Map<Integer, Integer>> firstCounts = new HashMap<>();
        label2srcdest.get(labels[0]).stream()
            .filter(srcDest -> {
                if (firstDir == Constants.FORWARD) {
                    return secMid2Next.containsKey(srcDest.key) &&
                        secMid2Next.get(srcDest.key).containsKey(labels[1]);
                } else {
                    return secMid2Next.containsKey(srcDest.value) &&
                        secMid2Next.get(srcDest.value).containsKey(labels[1]);
                }
            })
            .forEach(srcDest -> {
                Integer midPhysical;
                String firstHash;
                if (firstDir == Constants.FORWARD) {
                    midPhysical = srcDest.key;
                    firstHash = Util.hash(midPhysical, midBudget, srcDest.value, firstLeafBudget);
                } else {
                    midPhysical = srcDest.value;
                    firstHash = Util.hash(srcDest.key, firstLeafBudget, midPhysical, midBudget);
                }

                // hashIdComb -> count
                Map<String, Integer> secCounts = new HashMap<>();
                secMid2Next.get(midPhysical).get(labels[1])
                    .forEach(secLeafPhysical -> {
                        String secHash;
                        if (secDir == Constants.FORWARD) {
                            secHash = Util.hash(midPhysical, midBudget, secLeafPhysical, secLeafBudget);
                        } else {
                            secHash = Util.hash(secLeafPhysical, secLeafBudget, midPhysical, midBudget);
                        }
                        String hashIdComb = firstHash + ";" + secHash;
                        secCounts.put(hashIdComb, secCounts.getOrDefault(hashIdComb, 0) + 1);
                    });

                for (String hashIdComb : secCounts.keySet()) {
                    addCat(hashIdComb, vListString, labelSeqString, secCounts.get(hashIdComb));
                    addMaxDeg(hashIdComb, vListEdges[0], labelStrings[0], vListEdges[1], labelStrings[1],
                        secCounts.get(hashIdComb));
                    firstCounts.putIfAbsent(hashIdComb, new HashMap<>());
                    firstCounts.get(hashIdComb).put(
                        midPhysical,
                        firstCounts.get(hashIdComb).getOrDefault(midPhysical, 0) + 1);
                }
            });

        for (String hashIdComb : firstCounts.keySet()) {
            Integer maxDeg = Integer.MIN_VALUE;
            for (Integer midV : firstCounts.get(hashIdComb).keySet()) {
                maxDeg = Math.max(maxDeg, firstCounts.get(hashIdComb).get(midV));
            }
            addMaxDeg(hashIdComb, vListEdges[1], labelStrings[1], vListEdges[0], labelStrings[0], maxDeg);
        }
    }

    private void addCat(String hashIdComb, String vList, String labelSeq, Integer extCount) {
        catalogue.putIfAbsent(hashIdComb, new HashMap<>());
        catalogue.get(hashIdComb).putIfAbsent(vList, new HashMap<>());
        catalogue.get(hashIdComb).get(vList).put(
            labelSeq,
            catalogue.get(hashIdComb).get(vList).getOrDefault(labelSeq, 0L) + extCount);
    }

    private void addMaxDeg(String hashIdComb, String baseVList, String baseLabelSeq,
        String extVList, String extLabel, Integer extCount) {
        maxDeg.putIfAbsent(hashIdComb, new HashMap<>());
        maxDeg.putIfAbsent(hashIdComb, new HashMap<>());
        maxDeg.get(hashIdComb).putIfAbsent(baseVList, new HashMap<>());
        maxDeg.get(hashIdComb).get(baseVList).putIfAbsent(baseLabelSeq, new HashMap<>());
        maxDeg.get(hashIdComb).get(baseVList).get(baseLabelSeq).putIfAbsent(extVList, new HashMap<>());
        Integer currentMax =
            maxDeg.get(hashIdComb).get(baseVList).get(baseLabelSeq).get(extVList).getOrDefault(extLabel, 0);

        maxDeg.get(hashIdComb).get(baseVList).get(baseLabelSeq).get(extVList)
            .put(extLabel, Math.max(currentMax, extCount));
    }

    // assumes entry length = 2
    private Integer getMid(Integer[] vList) {
        if (vList[0].equals(vList[2]) || vList[0].equals(vList[3])) {
            return vList[0];
        }
        return vList[1];
    }

    // assumes entry length = 2
    private Integer getFirstLeaf(Integer[] vList, Integer mid) {
        if (vList[0].equals(mid)) {
            return vList[1];
        }
        return vList[0];
    }

    // assumes entry length = 2
    private Integer getSecLeaf(Integer[] vList, Integer mid) {
        if (vList[2].equals(mid)) {
            return vList[3];
        }
        return vList[2];
    }

    // assumes entry length = 2
    private Map<Integer, Map<Integer, List<Integer>>> determineMid2Next(Integer[] vList, Integer mid, int side) {
        if (side == 1) {
            if (vList[0].equals(mid)) {
                return src2label2dest;
            }
            return dest2label2src;
        } else {
            if (vList[2].equals(mid)) {
                return src2label2dest;
            }
            return dest2label2src;
        }
    }

    public void compute1(String vListString, String labelString, String scheme) {
        Integer[] budgets = Util.toVList(scheme);
        label2srcdest.get(Integer.parseInt(labelString))
            .forEach(srcDest -> {
                String hashId = Util.hash(srcDest.key, budgets[0], srcDest.value, budgets[1]);
                singleEdge.putIfAbsent(hashId, new HashMap<>());
                singleEdge.get(hashId).putIfAbsent(vListString, new HashMap<>());
                singleEdge.get(hashId).get(vListString).put(
                    labelString,
                    singleEdge.get(hashId).get(vListString).getOrDefault(labelString, 0L) + 1);
            });
    }

    private Map<Integer, Integer> mapVirtual2budgetOrHash(String vListString, String schemeOrHashString) {
        Integer[] vList = Util.toVList(vListString);
        Integer[] budgetOrHash = Util.toVList(schemeOrHashString);

        Map<Integer, Integer> virtual2budgetOrHash = new HashMap<>();
        for (int i = 0; i < vList.length; ++i) {
            virtual2budgetOrHash.put(vList[i], budgetOrHash[i]);
        }
        return virtual2budgetOrHash;
    }

    private void readGraph(String graphFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader csvReader = new BufferedReader(new FileReader(graphFile));
        int[] line;
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            line = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt).toArray();

            label2srcdest.putIfAbsent(line[1], new ArrayList<>());
            label2srcdest.get(line[1]).add(new Pair<>(line[0], line[2]));

            src2label2dest.putIfAbsent(line[0], new HashMap<>());
            src2label2dest.get(line[0]).putIfAbsent(line[1], new ArrayList<>());
            src2label2dest.get(line[0]).get(line[1]).add(line[2]);

            dest2label2src.putIfAbsent(line[2], new HashMap<>());
            dest2label2src.get(line[2]).putIfAbsent(line[1], new ArrayList<>());
            dest2label2src.get(line[2]).get(line[1]).add(line[0]);

            tripleString = csvReader.readLine();
        }

        endTime = System.currentTimeMillis();
        System.out.println("Loading Graph: " + ((endTime - startTime) / 1000.0) + " sec");

        csvReader.close();
    }

    public HashFilterDistribution(
        Map<Integer, List<Pair<Integer, Integer>>> label2srcdest,
        Map<Integer, Map<Integer, List<Integer>>> src2label2dest,
        Map<Integer, Map<Integer, List<Integer>>> dest2label2src) {
        this.label2srcdest = label2srcdest;
        this.src2label2dest = src2label2dest;
        this.dest2label2src = dest2label2src;

        this.catalogue = new HashMap<>();
        this.maxDeg = new HashMap<>();
        this.singleEdge = new HashMap<>();
    }
}
