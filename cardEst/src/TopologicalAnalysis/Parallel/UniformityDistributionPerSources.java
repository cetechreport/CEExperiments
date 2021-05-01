package TopologicalAnalysis.Parallel;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

public class UniformityDistributionPerSources implements Runnable {
    int threadId;
    Map<Integer, Map<Integer, List<Integer>>> src2label2dest;
    Map<Integer, Map<Integer, List<Integer>>> dest2label2src;
    String vListString;
    String labelSeqString;
    Integer targetBaseV;
    Integer targetExtV;
    List<Integer> starters;

    // e.g. for 0-1;1-2;2-5;5-6, each key is the vID of virtual 5 and its value is #6's attached
    Map<Integer, Integer> basePhysical2numExt;

    Map<Integer, Long> basePhysical2leafCountExcExt;

    public void run() {
        Integer[] vList = toVList(vListString);
        Integer[] labelSeq = toLabelSeq(labelSeqString);

        Map<Integer, List<Integer>> virtual2physicals;
        Set<Integer> covered;
        Set<Integer> leaves = getLeaves(vList);
        leaves.remove(targetExtV);

        for (Integer starter : starters) {
            virtual2physicals = new HashMap<>();
            virtual2physicals.put(vList[1], new ArrayList<>());
            virtual2physicals.get(vList[1]).add(starter);

            covered = new HashSet<>();
            covered.add(vList[1]);

            computeDistribution(vList, labelSeq, virtual2physicals, leaves, covered);
        }

        try {
            persist();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void computeDistribution(
        Integer[] vList,
        Integer[] labelSeq,
        Map<Integer, List<Integer>> virtual2physicals,
        Set<Integer> leaves,
        Set<Integer> covered) {

        Integer currentLabel, currentVirtual, nextVirtual;
        List<Integer> currentPhysicals, updatedCurrentPhysicals, nextPhysicals;
        Map<Integer, Map<Integer, List<Integer>>> current2label2next;

        for (int i = 0; i < vList.length; i += 2) {
            currentLabel = labelSeq[i / 2];
            if (covered.contains(vList[i])) {
                currentVirtual = vList[i];
                currentPhysicals = virtual2physicals.get(vList[i]);
                nextVirtual = vList[i + 1];
                current2label2next = src2label2dest;
            } else if (covered.contains(vList[i + 1])) {
                currentVirtual = vList[i + 1];
                currentPhysicals = virtual2physicals.get(vList[i + 1]);
                nextVirtual = vList[i];
                current2label2next = dest2label2src;
            } else {
                System.err.println("ERROR: both virtual nodes are uncovered");
                break;
            }

            if (currentPhysicals == null) break;

            covered.add(nextVirtual);

            updatedCurrentPhysicals = new ArrayList<>();
            for (Integer currentPhysical : currentPhysicals) {
                nextPhysicals =
                    current2label2next.getOrDefault(currentPhysical, new HashMap<>())
                        .getOrDefault(currentLabel, new ArrayList<>());

                if (currentVirtual.equals(targetBaseV) && nextVirtual.equals(targetExtV)) {
                    basePhysical2numExt.put(currentPhysical, nextPhysicals.size());

                    long leafCountExcTarget = 1;
                    for (Integer leaf : leaves) {
                        leafCountExcTarget *= virtual2physicals.getOrDefault(leaf, new ArrayList<>()).size();
                    }
                    basePhysical2leafCountExcExt.put(currentPhysical, leafCountExcTarget);
                }

                if (nextPhysicals.isEmpty()) continue;

                updatedCurrentPhysicals.add(currentPhysical);

                virtual2physicals.putIfAbsent(nextVirtual, new ArrayList<>());
                virtual2physicals.get(nextVirtual).addAll(nextPhysicals);
            }

            virtual2physicals.put(currentVirtual, updatedCurrentPhysicals);
        }
    }

    private void persist() throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter("dist_" + threadId + ".csv"));
        for (Integer v : basePhysical2numExt.keySet()) {
            StringJoiner sj = new StringJoiner(",");
            sj.add(vListString);
            sj.add(labelSeqString);
            sj.add(v.toString());
            sj.add(basePhysical2leafCountExcExt.get(v).toString());
            sj.add(basePhysical2numExt.get(v).toString());
            writer.write(sj.toString() + "\n");
        }
        writer.close();
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

    public UniformityDistributionPerSources(
        int threadId,
        Map<Integer, Map<Integer, List<Integer>>> src2label2dest,
        Map<Integer, Map<Integer, List<Integer>>> dest2label2src,
        String vListString,
        String labelSeqString,
        Integer targetBaseV,
        Integer targetExtV,
        List<Integer> starters) {

        this.threadId = threadId;
        this.src2label2dest = src2label2dest;
        this.dest2label2src = dest2label2src;
        this.vListString = vListString;
        this.labelSeqString = labelSeqString;
        this.targetBaseV = targetBaseV;
        this.targetExtV = targetExtV;
        this.starters = starters;

        this.basePhysical2numExt = new HashMap<>();
        this.basePhysical2leafCountExcExt = new HashMap<>();
    }
}
