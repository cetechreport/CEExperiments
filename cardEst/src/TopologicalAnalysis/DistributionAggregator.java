package TopologicalAnalysis;

import Common.Pair;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/*
 * Aggregate to
 *   vList, labelSeq, extCount, #baseV giving the count, #contributions
 */
public class DistributionAggregator {
    // vList -> labelSeq -> extCount -> (#baseV, totalLeafCountExcExt)
    Map<String, Map<String, Map<Integer, Pair<Long, Long>>>> cache = new HashMap<>();

    public void persist(String destFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedWriter writer = new BufferedWriter(new FileWriter(destFile));
        for (String vList : cache.keySet()) {
            for (String labelSeq : cache.get(vList).keySet()) {
                List<Integer> distCounts = new ArrayList<>(cache.get(vList).get(labelSeq).keySet());
                Collections.sort(distCounts);

                for (Integer extCount: distCounts) {
                    Pair<Long, Long> dist = cache.get(vList).get(labelSeq).get(extCount);
                    StringJoiner sj = new StringJoiner(",");
                    sj.add(vList);
                    sj.add(labelSeq);
                    sj.add(extCount.toString());
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

    private void aggregate(String distFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader reader = new BufferedReader(new FileReader(distFile));
        String vList, labelSeq;
        Integer extCount;
        Long leafCountExcExt;
        String[] info;
        String line = reader.readLine();
        while (null != line) {
            info = line.split(",");
            vList = info[0];
            labelSeq = info[1];
            leafCountExcExt = Long.parseLong(info[3]);
            extCount = Integer.parseInt(info[4]);

            cache.putIfAbsent(vList, new HashMap<>());
            cache.get(vList).putIfAbsent(labelSeq, new HashMap<>());
            Pair<Long, Long> dist =
                cache.get(vList).get(labelSeq).getOrDefault(extCount, new Pair<>(0L, 0L));
            dist.key += 1;
            dist.value += leafCountExcExt;
            cache.get(vList).get(labelSeq).put(extCount, dist);

            line = reader.readLine();
        }

        endTime = System.currentTimeMillis();
        System.out.println("Aggregating: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("distFile: " + args[0]);
        System.out.println("destFile: " + args[1]);
        System.out.println();

        DistributionAggregator aggregator = new DistributionAggregator();
        aggregator.aggregate(args[0]);
        aggregator.persist(args[1]);
    }
}
