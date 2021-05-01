package TopologicalAnalysis;

import Common.Pair;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

public class IntermediateCountCombiner {
    // vList -> labelSeq -> vID -> (leafCountExcExt, extCount)
    Map<String, Map<String, Map<Integer, Pair<Long, Integer>>>> cache = new HashMap<>();

    private void aggregateIntermediate(String fileName) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        String vList, labelSeq;
        Integer vId, extCount;
        Long leafCountExcExt;
        String[] info;
        String line = reader.readLine();
        while (null != line) {
            info = line.split(",");
            vList = info[0];
            labelSeq = info[1];
            vId = Integer.parseInt(info[2]);
            leafCountExcExt = Long.parseLong(info[3]);
            extCount = Integer.parseInt(info[4]);

            cache.putIfAbsent(vList, new HashMap<>());
            cache.get(vList).putIfAbsent(labelSeq, new HashMap<>());
            if (cache.get(vList).get(labelSeq).containsKey(vId)) {
                Pair<Long, Integer> dist = cache.get(vList).get(labelSeq).get(vId);
                dist.key += leafCountExcExt;

                // sanity check
                if (!dist.value.equals(extCount)) {
                    System.err.println("SANITY CHECK:");
                    System.err.println(line + ": " + dist.toString());
                    return;
                }

                cache.get(vList).get(labelSeq).put(vId, dist);
            } else {
                cache.get(vList).get(labelSeq).put(vId, new Pair<>(leafCountExcExt, extCount));
            }

            line = reader.readLine();
        }

        endTime = System.currentTimeMillis();
        System.out.println("Aggregating: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void persist(String destFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedWriter writer = new BufferedWriter(new FileWriter(destFile));
        for (String vList : cache.keySet()) {
            for (String labelSeq : cache.get(vList).keySet()) {
                for (Integer vId : cache.get(vList).get(labelSeq).keySet()) {
                    Pair<Long, Integer> dist = cache.get(vList).get(labelSeq).get(vId);
                    StringJoiner sj = new StringJoiner(",");
                    sj.add(vList);
                    sj.add(labelSeq);
                    sj.add(vId.toString());
                    sj.add(dist.key.toString());
                    sj.add(dist.value.toString());

                    writer.write(sj.toString() + "\n");
                }
            }
        }

        writer.close();

        endTime = System.currentTimeMillis();
        System.out.println("Saving: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("intermediateFile: " + args[0]);
        System.out.println("destFile: " + args[1]);
        System.out.println();

        IntermediateCountCombiner combiner = new IntermediateCountCombiner();
        combiner.aggregateIntermediate(args[0]);
        combiner.persist(args[1]);
    }
}
