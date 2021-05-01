package Graphflow.Parallel;

import Common.Pair;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;

public class Util {
    private static final String BASE = "";
    private static final Long TOTAL = 0L;

    public static void saveDistribution(
        Map<String, Map<String, Map<Long, Long>>> distribution,
        Map<String, Integer> numOcc,
        String destFile) throws Exception {

        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedWriter writer = new BufferedWriter(new FileWriter(destFile));
        for (String labelSeq : distribution.keySet()) {
            long baseTotal = distribution.get(labelSeq).get(BASE).get(TOTAL);
            for (String extendedEdge : distribution.get(labelSeq).keySet()) {
                if (extendedEdge.equals(BASE)) continue;

                StringJoiner sj = new StringJoiner(",");
                sj.add(labelSeq);
                sj.add(extendedEdge);
                sj.add(numOcc.get(labelSeq).toString());

                long total = 0;
                for (Long entry : distribution.get(labelSeq).get(extendedEdge).keySet()) {
                    total += entry * distribution.get(labelSeq).get(extendedEdge).get(entry);
                }

                double mean = ((double) total) / baseTotal;
                double sd = computeSD(distribution.get(labelSeq).get(extendedEdge), mean, baseTotal);
                double cv = sd / mean;

                sj.add(Double.toString(cv));
                writer.write(sj.toString() + "\n");
            }
        }
        writer.close();
        distribution.clear();
        numOcc.clear();

        endTime = System.currentTimeMillis();
        System.out.println("Saving CV (" + destFile + "): "
            + ((endTime - startTime) / 1000.0) + " sec");
    }

    public static Set<Integer> getSamples(List<Integer> candidates, int numSamples) {
        Set<Integer> sampledIndices = new HashSet<>();
        Random random = new Random(0);

        Set<Integer> samples = new HashSet<>();

        if (candidates.size() > numSamples) {
            int sampledIndex;
            while (samples.size() < numSamples) {
                sampledIndex = random.nextInt(candidates.size());
                while (sampledIndices.contains(sampledIndex)) {
                    sampledIndex = random.nextInt(candidates.size());
                }
                sampledIndices.add(sampledIndex);

                samples.add(candidates.get(sampledIndex));
            }
        } else {
            samples.addAll(candidates);
        }

        return samples;
    }

    public static Pair<String, String> dirsToBaseAndExt(int[] dirs) {
        StringJoiner sj = new StringJoiner("-");
        for (int i = 0; i < dirs.length - 1; ++i) {
            sj.add(Integer.toString(dirs[i]));
        }
        return new Pair<>(sj.toString(), Integer.toString(dirs[dirs.length - 1]));
    }

    public static String sortEdgeDirByEdge(TreeMap<Integer, Integer> sortedMap) {
        StringJoiner labels = new StringJoiner("-");
        StringJoiner dirs = new StringJoiner("-");

        for (Map.Entry<Integer, Integer> entry : sortedMap.entrySet()) {
            labels.add(entry.getKey().toString());
            dirs.add(entry.getValue().toString());
        }

        return dirs.toString() + ";" + labels.toString();
    }

    private static double computeSD(Map<Long, Long> histogram, double mean, long numTotalBases) {
        double sd = 0;

        for (long entry: histogram.keySet()) {
            sd += (Math.pow(entry - mean, 2) / numTotalBases) * histogram.get(entry);
        }

        return Math.sqrt(sd);
    }
}
