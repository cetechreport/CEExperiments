package TrueCardinality.Parallel;

import Common.Pair;
import Common.Util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CyclicCardinalityCounter implements Runnable {
    int patternType;
    Map<Integer, List<Pair<Integer, Integer>>> label2srcdest;
    Map<Integer, Map<Integer, List<Integer>>> src2label2dest;
    Map<Integer, Map<Integer, List<Integer>>> dest2label2src;

    Pair<String, String> vListAndLabelSeq;

    long cardinality;

    public void run() {
        switch (patternType) {
            case 301:
                cardinality = squareDiag(vListAndLabelSeq.key, vListAndLabelSeq.value);
                break;
            case 302:
                cardinality = biTriangle(vListAndLabelSeq.key, vListAndLabelSeq.value);
                break;
            case 303:
                cardinality = lollipop(vListAndLabelSeq.key, vListAndLabelSeq.value);
                break;
        }
    }

    public long squareDiag(String vListString, String labelSeqString) {
        Integer[] vList = Util.toVList(vListString);
        Integer[] labelSeq = Util.toLabelSeq(labelSeqString);

        long totalCard = 0L;
        for (Pair<Integer, Integer> srcdest : label2srcdest.get(labelSeq[1])) {
            if (!src2label2dest.containsKey(srcdest.key)) continue;
            if (!src2label2dest.get(srcdest.key).containsKey(labelSeq[2])) continue;
            if (!src2label2dest.containsKey(srcdest.value)) continue;
            if (!src2label2dest.get(srcdest.value).containsKey(labelSeq[4])) continue;
            if (!src2label2dest.containsKey(srcdest.key)) continue;
            if (!src2label2dest.get(srcdest.key).containsKey(labelSeq[0])) continue;
            if (!dest2label2src.containsKey(srcdest.value)) continue;
            if (!dest2label2src.get(srcdest.value).containsKey(labelSeq[3])) continue;

            Set<Integer> fromSrcLeft = new HashSet<>(src2label2dest.get(srcdest.key).get(labelSeq[2]));
            Set<Integer> fromDestLeft = new HashSet<>(src2label2dest.get(srcdest.value).get(labelSeq[4]));
            fromSrcLeft.retainAll(fromDestLeft); // get the intersection

            Set<Integer> fromSrcRight = new HashSet<>(src2label2dest.get(srcdest.key).get(labelSeq[0]));
            Set<Integer> toDestRight = new HashSet<>(dest2label2src.get(srcdest.value).get(labelSeq[3]));
            fromSrcRight.retainAll(toDestRight); // get the intersection
            totalCard += fromSrcLeft.size() * fromSrcRight.size();
        }

        return totalCard;
    }

    public long biTriangle(String vListString, String labelSeqString) {
        Integer[] vList = Util.toVList(vListString);
        Integer[] labelSeq = Util.toLabelSeq(labelSeqString);

        Set<Integer> vHasOutgoing = new HashSet<>(src2label2dest.keySet());
        Set<Integer> vHasIncoming = new HashSet<>(dest2label2src.keySet());
        vHasOutgoing.retainAll(vHasIncoming);

        long totalCard = 0L;
        for (Integer center : vHasOutgoing) {
            if (!dest2label2src.get(center).containsKey(labelSeq[1])) continue;
            if (!dest2label2src.get(center).containsKey(labelSeq[2])) continue;
            if (!src2label2dest.get(center).containsKey(labelSeq[3])) continue;
            if (!src2label2dest.get(center).containsKey(labelSeq[4])) continue;

            List<Integer> leftSrcs = dest2label2src.get(center).get(labelSeq[1]);
            List<Integer> leftDests = dest2label2src.get(center).get(labelSeq[2]);
            long leftTri = 0L;
            for (Integer leftSrc : leftSrcs) {
                if (!src2label2dest.containsKey(leftSrc)) continue;
                if (!src2label2dest.get(leftSrc).containsKey(labelSeq[0])) continue;
                Set<Integer> fromLeftSrc = new HashSet<>(src2label2dest.get(leftSrc).get(labelSeq[0]));
                fromLeftSrc.retainAll(leftDests);
                leftTri += fromLeftSrc.size();
            }

            List<Integer> rightSrcs = src2label2dest.get(center).get(labelSeq[3]);
            List<Integer> rightDests = src2label2dest.get(center).get(labelSeq[4]);
            long rightTri = 0L;
            for (Integer rightSrc : rightSrcs) {
                if (!src2label2dest.containsKey(rightSrc)) continue;
                if (!src2label2dest.get(rightSrc).containsKey(labelSeq[5])) continue;
                Set<Integer> fromRightSrc = new HashSet<>(src2label2dest.get(rightSrc).get(labelSeq[5]));
                fromRightSrc.retainAll(rightDests);
                rightTri += fromRightSrc.size();
            }

            totalCard += leftTri * rightTri;
        }

        return totalCard;
    }

    public long lollipop(String vListString, String labelSeqString) {
        Integer[] vList = Util.toVList(vListString);
        Integer[] labelSeq = Util.toLabelSeq(labelSeqString);

        long totalCard = 0L;
        for (Pair<Integer, Integer> srcdest : label2srcdest.get(labelSeq[0])) {
            if (!src2label2dest.containsKey(srcdest.key)) continue;
            if (!src2label2dest.get(srcdest.key).containsKey(labelSeq[1])) continue;
            if (!src2label2dest.containsKey(srcdest.value)) continue;
            if (!src2label2dest.get(srcdest.value).containsKey(labelSeq[2])) continue;

            Set<Integer> fromSrc = new HashSet<>(src2label2dest.get(srcdest.key).get(labelSeq[1]));
            Set<Integer> fromDest = new HashSet<>(src2label2dest.get(srcdest.value).get(labelSeq[2]));
            fromSrc.retainAll(fromDest);

            for (Integer start : fromSrc) {
                if (!src2label2dest.containsKey(start)) continue;
                if (!src2label2dest.get(start).containsKey(labelSeq[3])) continue;
                for (Integer mid : src2label2dest.get(start).get(labelSeq[3])) {
                    if (!src2label2dest.containsKey(mid)) continue;
                    if (!src2label2dest.get(mid).containsKey(labelSeq[4])) continue;

                    totalCard += src2label2dest.get(mid).get(labelSeq[4]).size();
                }
            }
        }

        return totalCard;
    }

    public long getCardinality() {
        return cardinality;
    }

    public CyclicCardinalityCounter(
        int patternType,
        Map<Integer, List<Pair<Integer, Integer>>> label2srcdest,
        Map<Integer, Map<Integer, List<Integer>>> src2label2dest,
        Map<Integer, Map<Integer, List<Integer>>> dest2label2src,
        Pair<String, String> vListAndLabelSeq) {
        this.patternType = patternType;
        this.label2srcdest = label2srcdest;
        this.src2label2dest = src2label2dest;
        this.dest2label2src = dest2label2src;
        this.vListAndLabelSeq = vListAndLabelSeq;
    }
}
