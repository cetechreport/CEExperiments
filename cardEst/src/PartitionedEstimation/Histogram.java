package PartitionedEstimation;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

public class Histogram {
    // extSize -> #instances
    public Map<Integer, Long> histogram = new HashMap<>();
    public void add(Integer extSize, Long numInstances) {
        histogram.put(extSize, histogram.getOrDefault(extSize, 0L) + numInstances);
    }

    public String toString() {
        StringJoiner sj = new StringJoiner(",");
        for (Integer extSize : histogram.keySet()) {
            sj.add(extSize.toString()).add(histogram.get(extSize).toString());
        }
        return sj.toString();
    }

    public int getMaxDeg() {
        int maxDeg = Integer.MIN_VALUE;
        for (Integer deg : histogram.keySet()) {
            Long numInstances = histogram.get(deg);
            if (!numInstances.equals(0L)) {
                maxDeg = Math.max(maxDeg, deg);
            }
        }
        return maxDeg;
    }
}
