package Graphflow.Parallel;

import Common.Pair;
import Common.Triple;
import Common.Util;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class TwoPathComputation implements Callable<Triple<String, String, String>> {
    List<Pair<Integer, Integer>> starters;
    Map<Integer, Map<Integer, List<Integer>>> src2label2dest;
    Map<Integer, Map<Integer, List<Integer>>> dest2label2src;

    String runType;
    String sortedVList;
    String sortedLabelSeq;
    String vListString;
    String labelSeqString;

    public Triple<String, String, String> call() {
        Integer[] vList = Common.Util.toVList(vListString);
        Integer[] labelSeq = Util.toLabelSeq(labelSeqString);
        String[] vListEdges = vListString.split(";");

        if (vList.length == 2) { // single label
            return new Triple<>(runType, vListString + "," + labelSeqString + "," + starters.size(), null);
        }

        Map<Integer, Map<Integer, List<Integer>>> mid2rightLeaf;
        Integer midPhysical;

        int maxDeg = Integer.MIN_VALUE;
        long totalCard = 0L;
        for (Pair<Integer, Integer> srcdest : starters) {
            if (vList[0].equals(vList[2])) {
                mid2rightLeaf = src2label2dest;
                midPhysical = srcdest.key;
            } else if (vList[0].equals(vList[3])) {
                mid2rightLeaf = dest2label2src;
                midPhysical = srcdest.key;
            } else if (vList[1].equals(vList[2])) {
                mid2rightLeaf = src2label2dest;
                midPhysical = srcdest.value;
            } else if (vList[1].equals(vList[3])) {
                mid2rightLeaf = dest2label2src;
                midPhysical = srcdest.value;
            } else {
                System.err.println("ERROR: unrecognized 2-path vList");
                System.err.println("   vList: " + vListString);
                return new Triple<>(null, null, null);
            }

            if (!mid2rightLeaf.containsKey(midPhysical)) continue;
            if (!mid2rightLeaf.get(midPhysical).containsKey(labelSeq[1])) continue;

            totalCard += mid2rightLeaf.get(midPhysical).get(labelSeq[1]).size();

            maxDeg = Math.max(maxDeg, mid2rightLeaf.get(midPhysical).get(labelSeq[1]).size());
        }

        return new Triple<>(
            runType,
            sortedVList + "," + sortedLabelSeq + "," + totalCard,
            vListEdges[0] + "," + labelSeq[0] + "," + vListEdges[1] + "," + labelSeq[1] + "," + maxDeg
        );
    }

    public TwoPathComputation(
        String runType,
        String sortedVList,
        String sortedLabelSeq,
        String vListString,
        String labelSeqString,
        Map<Integer, Map<Integer, List<Integer>>> src2label2dest,
        Map<Integer, Map<Integer, List<Integer>>> dest2label2src,
        List<Pair<Integer, Integer>> starters) {
        this.runType = runType;
        this.sortedVList = sortedVList;
        this.sortedLabelSeq = sortedLabelSeq;
        this.vListString = vListString;
        this.labelSeqString = labelSeqString;
        this.src2label2dest = src2label2dest;
        this.dest2label2src = dest2label2src;
        this.starters = starters;
    }
}
