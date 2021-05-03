package Graphflow.Parallel;

import Common.Pair;
import Common.Triple;
import Common.Util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class TriangleComputation implements Callable<Triple<String, String, String>> {
    List<Pair<Integer, Integer>> starters;
    Map<Integer, Map<Integer, List<Integer>>> src2label2dest;
    Map<Integer, Map<Integer, List<Integer>>> dest2label2src;

    String runType;
    String sortedTriangleVList;
    String sortedTriangleLabelSeq;
    String triangleVList;
    String triangleLabelSeq;
    String trig;

    public Triple<String, String, String> call() {
        Integer[] vList = Common.Util.toVList(triangleVList);
        Integer[] labelSeq = Common.Util.toLabelSeq(triangleLabelSeq);
        String[] vListEdges = triangleVList.split(";");

        if(!trig.equals("")) {
            vList = Common.Util.toVList(trig);
        }

        Map<Integer, Map<Integer, List<Integer>>> src2intersect;
        Map<Integer, Map<Integer, List<Integer>>> dest2intersect;
        Integer src2intersectLabel, dest2intersectLabel;

        if (vList[2].equals(vList[0]) || vList[3].equals(vList[0])) {
            src2intersectLabel = labelSeq[1];
            dest2intersectLabel = labelSeq[2];
            if (vList[2].equals(vList[0])) {
                src2intersect = src2label2dest;
            } else {
                src2intersect = dest2label2src;
            }

            if (vList[4].equals(vList[1])) {
                dest2intersect = src2label2dest;
            } else {
                dest2intersect = dest2label2src;
            }
        } else if (vList[2].equals(vList[1]) || vList[3].equals(vList[1])) {
            src2intersectLabel = labelSeq[2];
            dest2intersectLabel = labelSeq[1];
            if (vList[2].equals(vList[1])) {
                dest2intersect = src2label2dest;
            } else {
                dest2intersect = dest2label2src;
            }

            if (vList[4].equals(vList[0])) {
                src2intersect = src2label2dest;
            } else {
                src2intersect = dest2label2src;
            }
        } else {
            System.err.println("ERROR: not a triangle");
            System.err.println("   vList: " + triangleVList);
            return new Triple<>(null, null, null);
        }

        int label1MaxDeg = Integer.MIN_VALUE;
        long totalCount = 0L;
        for (Pair<Integer, Integer> srcdest : starters) {
            if (!src2intersect.containsKey(srcdest.key)) continue;
            if (!src2intersect.get(srcdest.key).containsKey(src2intersectLabel)) continue;
            if (!dest2intersect.containsKey(srcdest.value)) continue;
            if (!dest2intersect.get(srcdest.value).containsKey(dest2intersectLabel)) continue;

            int numIntersection = getNumIntersection(
                src2intersect.get(srcdest.key).get(src2intersectLabel),
                dest2intersect.get(srcdest.value).get(dest2intersectLabel)
            );
            totalCount += numIntersection;

            label1MaxDeg = Math.max(label1MaxDeg, numIntersection);
        }

        Pair<String, String> sorted =
            Util.sort(new Pair<>(vListEdges[1] + ";" + vListEdges[2], labelSeq[1] + "->" + labelSeq[2]));

        if(!trig.equals("")) sortedTriangleVList = "*" + sortedTriangleVList;

        return new Triple<>(
            runType,
            sortedTriangleVList + "," + sortedTriangleLabelSeq + "," + totalCount,
            vListEdges[0] + "," + labelSeq[0] + "," + sorted.key + "," + sorted.value + "," + label1MaxDeg);
    }

    public int getNumIntersection(List<Integer> vertices1, List<Integer> vertices2) {
        int count = 0;

        int i1 = 0;
        int i2 = 0;
        while (i1 < vertices1.size() && i2 < vertices2.size()) {
            if (vertices1.get(i1).equals(vertices2.get(i2))) {
                ++count;
                ++i1;
                ++i2;
            } else if (vertices1.get(i1) < vertices2.get(i2)) {
                ++i1;
            } else if (vertices1.get(i1) > vertices2.get(i2)) {
                ++i2;
            } else {
                System.err.println("ERROR: infinite loop");
                break;
            }
        }

        return count;
    }

    public TriangleComputation(
        String runType,
        String sortedTriangleVList,
        String sortedTriangleLabelSeq,
        String triangleVList,
        String triangleLabelSeq,
        Map<Integer, Map<Integer, List<Integer>>> src2label2dest,
        Map<Integer, Map<Integer, List<Integer>>> dest2label2src,
        List<Pair<Integer, Integer>> starters,
        String trig) {
        this.runType = runType;
        this.sortedTriangleVList = sortedTriangleVList;
        this.sortedTriangleLabelSeq = sortedTriangleLabelSeq;
        this.triangleVList = triangleVList;
        this.triangleLabelSeq = triangleLabelSeq;
        this.src2label2dest = src2label2dest;
        this.dest2label2src = dest2label2src;
        this.starters = starters;
        this.trig = trig;
    }
}
