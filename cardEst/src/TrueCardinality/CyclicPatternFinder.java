package TrueCardinality;

import Common.Pair;
import TrueCardinality.Parallel.PatternFinder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Stack;
import java.util.StringJoiner;

public class CyclicPatternFinder extends PatternFinder {
    Map<Integer, Map<Integer, List<Integer>>> src2dest2label;
    Map<Integer, Map<Integer, List<Integer>>> dest2src2label;

    @Override
    public void run() {
        Set<String> labelSeqs = new HashSet<>();
        Integer nextVirtual, currentPhysical;
        Map<Integer, Map<Integer, List<Integer>>> current2next2label;
        Integer srcPhysical;
        Integer destPhysical;
        Integer[] vList = toVList(vListString);

        for (Integer startV : startVertices) {
            if (!src2dest2label.containsKey(startV)) continue;

            // (vList index, virtual2physical)
            Stack<Pair<Integer, Map<Integer, Integer>>> stack = new Stack<>();
            Map<Integer, Integer> starter = new HashMap<>();
            starter.put(vList[0], startV);
            stack.push(new Pair<>(0, starter));
            while (!stack.isEmpty()) {
                Pair<Integer, Map<Integer, Integer>> current = stack.pop();
                Map<Integer, Integer> virtual2physical = current.value;

                // add to labelSeqs if it's already complete
                if (current.key.equals(vList.length)) {
                    List<Integer> labelSeq = new ArrayList<>();
                    for (int i = 0; i < vList.length; i += 2) {
                        srcPhysical = virtual2physical.get(vList[i]);
                        destPhysical = virtual2physical.get(vList[i + 1]);
//                        List<Integer> labels = new LinkedList<>(src2dest2label.get(srcPhysical).get(destPhysical));
//                        labels.removeAll(labelSeq);
//                        if (labels.isEmpty()) break;
//                        labelSeq.add(labels.get(0));
                        List<Integer> nextLabels = src2dest2label.get(srcPhysical).get(destPhysical);
                        labelSeq.add(nextLabels.get(random.nextInt(nextLabels.size())));
                    }

                    if (labelSeq.size() == vList.length / 2) {
                        StringJoiner sj = new StringJoiner("->");
                        for (Integer label : labelSeq) {
                            sj.add(label.toString());
                        }
                        labelSeqs.add(sj.toString());
                        if (labelSeqs.size() >= 1) {
                            try {
                                persist(labelSeqs);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            return;
                        }
                    }
                    continue;
                }

                // cyclic edge
                if (virtual2physical.containsKey(vList[current.key]) &&
                    virtual2physical.containsKey(vList[current.key + 1])) {
                    srcPhysical = virtual2physical.get(vList[current.key]);
                    destPhysical = virtual2physical.get(vList[current.key + 1]);
                    if (src2dest2label.containsKey(srcPhysical)) {
                        if (src2dest2label.get(srcPhysical).keySet().contains(destPhysical)) {
                            stack.push(new Pair<>(current.key + 2, virtual2physical));
                        }
                    }
                    continue;
                }

                // acyclic edge
                if (virtual2physical.containsKey(vList[current.key])) {
                    current2next2label = src2dest2label;
                    nextVirtual = vList[current.key + 1];
                    currentPhysical = virtual2physical.get(vList[current.key]);
                } else if (virtual2physical.containsKey(vList[current.key + 1])) {
                    current2next2label = dest2src2label;
                    nextVirtual = vList[current.key];
                    currentPhysical = virtual2physical.get(vList[current.key + 1]);
                } else {
                    System.err.println("ERROR: both uncovered vertices");
                    System.err.println("   src: " + vList[current.key]);
                    System.err.println("   dest: " + vList[current.key + 1]);
                    return;
                }

                if (!current2next2label.containsKey(currentPhysical)) continue;

                for (Integer next : current2next2label.get(currentPhysical).keySet()) {
                    Map<Integer, Integer> updatedVirtual2physical = new HashMap<>(virtual2physical);
                    updatedVirtual2physical.put(nextVirtual, next);
                    if (patternType.equals(301)) {
                        if (updatedVirtual2physical.containsKey(1) && updatedVirtual2physical.containsKey(2) &&
                            updatedVirtual2physical.get(1).equals(updatedVirtual2physical.get(2))) continue;
                        if (updatedVirtual2physical.containsKey(1) && updatedVirtual2physical.containsKey(3) &&
                            updatedVirtual2physical.get(1).equals(updatedVirtual2physical.get(3))) continue;
                        if (updatedVirtual2physical.containsKey(2) && updatedVirtual2physical.containsKey(3) &&
                            updatedVirtual2physical.get(2).equals(updatedVirtual2physical.get(3))) continue;
                    }
                    stack.push(new Pair<>(current.key + 2, updatedVirtual2physical));
                }
            }
        }

        try {
            persist(labelSeqs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public CyclicPatternFinder(
        int threadId,
        String filePrefix,
        Map<Integer, Map<Integer, List<Integer>>> src2label2dest,
        Map<Integer, Map<Integer, List<Integer>>> dest2label2src,
        Map<Integer, Map<Integer, List<Integer>>> src2dest2label,
        Map<Integer, Map<Integer, List<Integer>>> dest2src2label,
        Integer patternType,
        String pattern,
        List<Integer> startVertices,
        Random random) {

        super(threadId, filePrefix, src2label2dest, dest2label2src, patternType, pattern, startVertices);
        this.src2dest2label = src2dest2label;
        this.dest2src2label = dest2src2label;
        this.random = random;
    }
}
