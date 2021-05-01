package TrueCardinality.Parallel;

import Common.Pair;
import Common.Query;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PartialCount implements Runnable {
    int threadId;

    Map<Integer, Map<Integer, List<Integer>>> src2label2dest;
    Map<Integer, Map<Integer, List<Integer>>> dest2label2src;
    Map<Integer, List<Pair<Integer, Integer>>> label2srcdest;

    private volatile Query query;
    Integer pivot;
    Integer[] leftVList;
    Integer[] rightVList;
    Integer[] leftLabelSeq;
    Integer[] rightLabelSeq;
    List<Integer> starters;

    private volatile long cardinality;

    public long getCardinality() {
        return cardinality;
    }

    public Query getQuery() {
        return query;
    }

    public void run() {
        cardinality = evaluate(pivot, leftVList, rightVList, leftLabelSeq, rightLabelSeq, starters);
    }

    private long evaluate(
        Integer pivot,
        Integer[] leftVList, Integer[] rightVList,
        Integer[] leftLabelSeq, Integer[] rightLabelSeq,
        List<Integer> starters) {

        Set<Integer> leaves = getLeaves(leftVList, rightVList);

        Set<Integer> covered;
        Map<Integer, List<Integer>> virtual2physicals;
        List<Long> leftLeafCounts, rightLeafCounts;

        long cardinality = 0;
        for (Integer starter : starters) {
            virtual2physicals = new HashMap<>();
            virtual2physicals.put(pivot, new ArrayList<>());
            virtual2physicals.get(pivot).add(starter);
            covered = new HashSet<>();
            covered.add(pivot);
            leftLeafCounts = new ArrayList<>();
            rightLeafCounts = new ArrayList<>();

            computeLeafCount(leaves, virtual2physicals, covered, leftVList, leftLabelSeq, leftLeafCounts);
            computeLeafCount(leaves, virtual2physicals, covered, rightVList, rightLabelSeq, rightLeafCounts);
            cardinality += computeCardinality(leftLeafCounts, rightLeafCounts);
        }

        return cardinality;
    }

    private void computeLeafCount(
        Set<Integer> leaves,
        Map<Integer, List<Integer>> virtual2physicals,
        Set<Integer> covered,
        Integer[] vList,
        Integer[] labelSeq,
        List<Long> leafCounts) {

        Map<Integer, Map<Integer, List<Integer>>> current2label2next;
        Integer nextVirtual;
        List<Integer> currentPhysicals, nextPhysicals;
        Integer currentLabel;
        Map<Integer, Pair<Set<Integer>, List<Long>>> physical2leafCount = new HashMap<>();

        for (int i = 0; i < vList.length; i += 2) {
            currentLabel = labelSeq[i / 2];
            if (covered.contains(vList[i])) {
                currentPhysicals = virtual2physicals.get(vList[i]);
                nextVirtual = vList[i + 1];
                current2label2next = src2label2dest;
            } else if (covered.contains(vList[i + 1])) {
                currentPhysicals = virtual2physicals.get(vList[i + 1]);
                nextVirtual = vList[i];
                current2label2next = dest2label2src;
            } else {
                System.err.println("ERROR: both virtual nodes are uncovered");
                System.err.println("   covered: " + covered);
                System.err.println("   vList: " + vList[i] + "-" + vList[i + 1]);
                return;
            }

            covered.add(nextVirtual);

            if (currentPhysicals.isEmpty()) break;

            for (Integer currentPhysical : currentPhysicals) {
                nextPhysicals =
                    current2label2next.getOrDefault(currentPhysical, new HashMap<>())
                        .getOrDefault(currentLabel, new ArrayList<>());
                if (leaves.contains(nextVirtual)) {
                    addToLeafCount(
                        physical2leafCount, currentPhysical, currentLabel, nextPhysicals.size());
                } else {
                    virtual2physicals.putIfAbsent(nextVirtual, new ArrayList<>());
                    virtual2physicals.get(nextVirtual).addAll(nextPhysicals);
                }
            }
        }

        for (Pair<Set<Integer>, List<Long>> labelsAndCounts : physical2leafCount.values()) {
            for (Long leafCount : labelsAndCounts.value) {
                if (leafCount.equals(0L)) continue;

                leafCounts.add(leafCount);
            }
        }
    }

    private void addToLeafCount(
        Map<Integer, Pair<Set<Integer>, List<Long>>> physical2leafCount,
        Integer currentPhysical,
        Integer currentLabel,
        int numNextPhysical) {

        if (!physical2leafCount.containsKey(currentPhysical)) {
            physical2leafCount.put(currentPhysical, new Pair<>(new HashSet<>(), new ArrayList<>()));
        }

        Pair<Set<Integer>, List<Long>> nextLabelsAndCounts = physical2leafCount.get(currentPhysical);
        if (nextLabelsAndCounts.key.isEmpty() || nextLabelsAndCounts.key.contains(currentLabel)) {
            nextLabelsAndCounts.value.add((long) numNextPhysical);
        } else {
            List<Long> counts = nextLabelsAndCounts.value;
            for (int i = 0; i < counts.size(); ++i) {
                Long count = counts.get(i);
                counts.set(i, count * numNextPhysical);
            }
        }
        nextLabelsAndCounts.key.add(currentLabel);
    }

    private long computeCardinality(List<Long> leftLeafCount, List<Long> rightLeafCount) {
        long cardinality = 0;
        for (Long left : leftLeafCount) {
            for (Long right : rightLeafCount) {
                cardinality += left * right;
            }
        }
        return cardinality;
    }

    private Set<Integer> getLeaves(Integer[] leftVList, Integer[] rightVList) {
        Map<Integer, Integer> occurrences = new HashMap<>();
        for (Integer v : leftVList) {
            occurrences.put(v, occurrences.getOrDefault(v, 0) + 1);
        }
        for (Integer v : rightVList) {
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

    public PartialCount(
        int threadId,
        Map<Integer, Map<Integer, List<Integer>>> src2label2dest,
        Map<Integer, Map<Integer, List<Integer>>> dest2label2src,
        Map<Integer, List<Pair<Integer, Integer>>> label2srcdest,
        Query query,
        Integer pivot,
        Integer[] leftVList, Integer[] rightVList,
        Integer[] leftLabelSeq, Integer[] rightLabelSeq,
        List<Integer> starters) {

        this.threadId = threadId;
        this.src2label2dest = src2label2dest;
        this.dest2label2src = dest2label2src;
        this.label2srcdest = label2srcdest;
        this.query = query;
        this.pivot = pivot;
        this.leftVList = leftVList;
        this.rightVList = rightVList;
        this.leftLabelSeq = leftLabelSeq;
        this.rightLabelSeq = rightLabelSeq;
        this.starters = starters;
    }
}
