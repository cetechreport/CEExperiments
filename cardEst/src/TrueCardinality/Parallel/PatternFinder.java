package TrueCardinality.Parallel;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringJoiner;

public class PatternFinder implements Runnable {
    protected int threadId;
    protected String filePrefix;

    protected Map<Integer, Map<Integer, List<Integer>>> src2label2dest;
    protected Map<Integer, Map<Integer, List<Integer>>> dest2label2src;
    protected Integer patternType;
    protected String vListString;
    protected List<Integer> startVertices;
    protected Random random = new Random(0);

    public void run() {
        Set<String> labelSeqs = new HashSet<>();
        List<Integer> prefix;
        List<Integer> nextLabels, nextVertices;
        Set<Integer> covered;
        Map<Integer, Integer> virtual2physical;
        Map<Integer, Map<Integer, List<Integer>>> current2label2next;

        Integer nextVirtual, currentPhysical, nextPhysical;

        Integer[] vList = toVList(vListString);

        for (Integer startV : startVertices) {
            virtual2physical = new HashMap<>();
            virtual2physical.put(vList[0], startV);
            prefix = new ArrayList<>();
            covered = new HashSet<>();
            covered.add(vList[0]);

            for (int i = 0; i < vList.length; i += 2) {
                if (covered.contains(vList[i])) {
                    current2label2next = src2label2dest;
                    currentPhysical = virtual2physical.get(vList[i]);
                    nextVirtual = vList[i + 1];
                } else if (covered.contains(vList[i + 1])) {
                    current2label2next = dest2label2src;
                    currentPhysical = virtual2physical.get(vList[i + 1]);
                    nextVirtual = vList[i];
                } else {
                    System.err.println("ERROR: both virtual nodes are uncovered");
                    break;
                }

                if (!current2label2next.containsKey(currentPhysical)) break;

                covered.add(nextVirtual);

                nextLabels = new ArrayList<>(current2label2next.get(currentPhysical).keySet());
                Integer nextLabel = nextLabels.get(random.nextInt(nextLabels.size()));
                prefix.add(nextLabel);
                nextVertices = current2label2next.get(currentPhysical).get(nextLabel);
                nextPhysical = nextVertices.get(random.nextInt(nextVertices.size()));
                virtual2physical.put(nextVirtual, nextPhysical);
            }

            Set<Integer> dedup = new HashSet<>(prefix);
            if (dedup.size() < vList.length / 2) continue;

            labelSeqs.add(toPrefixString(prefix));
        }

        try {
            persist(labelSeqs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected String toPrefixString(List<Integer> prefix) {
        StringJoiner sj = new StringJoiner("->");

        for (Integer label : prefix) {
            sj.add(label.toString());
        }

        return sj.toString();
    }

    protected void persist(Set<String> labelSeqs) throws Exception {
        if (labelSeqs.size() == 0) return;

        BufferedWriter writer =
            new BufferedWriter(new FileWriter(filePrefix + "_" + threadId + ".csv"));

        StringJoiner sj = new StringJoiner(",");
        sj.add(patternType.toString());
        sj.add(vListString);
        for (String labelSeq : labelSeqs) {
            sj.add(labelSeq);
        }
        writer.write(sj.toString() + "\n");

        writer.close();
    }

    protected Integer[] toVList(String vListString) {
        String[] splitted = vListString.split(";");
        Integer[] vList = new Integer[splitted.length * 2];
        for (int i = 0; i < splitted.length; i++) {
            String[] srcDest = splitted[i].split("-");
            vList[i * 2] = Integer.parseInt(srcDest[0]);
            vList[i * 2 + 1] = Integer.parseInt(srcDest[1]);
        }
        return vList;
    }

    public PatternFinder(
        int threadId,
        String filePrefix,
        Map<Integer, Map<Integer, List<Integer>>> src2label2dest,
        Map<Integer, Map<Integer, List<Integer>>> dest2label2src,
        Integer patternType,
        String pattern,
        List<Integer> startVertices) {

        this.threadId = threadId;
        this.filePrefix = filePrefix;
        this.src2label2dest = src2label2dest;
        this.dest2label2src = dest2label2src;
        this.patternType = patternType;
        this.vListString = pattern;
        this.startVertices = startVertices;
    }
}
