package PartitionedEstimation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class Distribution {
    // baseVList -> baseLabelSeq -> extVList -> extLabel -> histogram
    public Map<String, Map<String, Map<String, Map<String, Histogram>>>> entries = new HashMap<>();

    public void add(
        String baseVList, String baseLabelSeq, String extVList, String extLabel, Histogram hist) {

        entries.putIfAbsent(baseVList, new HashMap<>());
        entries.get(baseVList).putIfAbsent(baseLabelSeq, new HashMap<>());
        entries.get(baseVList).get(baseLabelSeq).putIfAbsent(extVList, new HashMap<>());
        entries.get(baseVList).get(baseLabelSeq).get(extVList).put(extLabel, hist);
    }

    public void add(Distribution dist) {
        Map<String, Map<String, Map<String, Map<String, Histogram>>>> newEntries = dist.entries;

        for (String baseVList : newEntries.keySet()) {
            for (String baseLabelSeq : newEntries.get(baseVList).keySet()) {
                for (String extVList : newEntries.get(baseVList).get(baseLabelSeq).keySet()) {
                    Map<String, Histogram> extLabel2hist =
                        newEntries.get(baseVList).get(baseLabelSeq).get(extVList);
                    for (String extLabel : extLabel2hist.keySet()) {
                        this.entries.putIfAbsent(baseVList, new HashMap<>());
                        this.entries.get(baseVList).putIfAbsent(baseLabelSeq, new HashMap<>());
                        this.entries.get(baseVList).get(baseLabelSeq)
                            .putIfAbsent(extVList, new HashMap<>());
                        if (this.entries.get(baseVList).get(baseLabelSeq).get(extVList)
                            .containsKey(extLabel)) {
                            System.out.println("ERROR: should not exist");
                        }
                        this.entries.get(baseVList).get(baseLabelSeq).get(extVList)
                            .put(extLabel, extLabel2hist.get(extLabel));
                    }
                }
            }
        }
    }

    public List<String> toStrings() {
        List<String> lines = new ArrayList<>();
        for (String baseVList : entries.keySet()) {
            for (String baseLabelSeq : entries.get(baseVList).keySet()) {
                for (String extVList : entries.get(baseVList).get(baseLabelSeq).keySet()) {
                    Map<String, Histogram> extLabel2hist =
                        entries.get(baseVList).get(baseLabelSeq).get(extVList);

                    for (String extLabel : extLabel2hist.keySet()) {
                        StringJoiner sj = new StringJoiner(",");
                        sj.add(baseVList).add(baseLabelSeq).add(extVList).add(extLabel);
                        sj.add(extLabel2hist.get(extLabel).toString());
                        lines.add(sj.toString());
                    }
                }
            }
        }
        return lines;
    }

    public List<String> getMaxDegs() {
        List<String> maxDegs = new ArrayList<>();
        for (String baseVList : entries.keySet()) {
            for (String baseLabelSeq : entries.get(baseVList).keySet()) {
                for (String extVList : entries.get(baseVList).get(baseLabelSeq).keySet()) {
                    Map<String, Histogram> extLabel2hist =
                        entries.get(baseVList).get(baseLabelSeq).get(extVList);
                    for (String extLabel : extLabel2hist.keySet()) {
                        Histogram hist = extLabel2hist.get(extLabel);

                        StringJoiner line = new StringJoiner(",");
                        line.add(baseVList).add(baseLabelSeq).add(extVList).add(extLabel);
                        line.add(Integer.toString(hist.getMaxDeg()));
                        maxDegs.add(line.toString());
                    }
                }
            }
        }

        return maxDegs;
    }

    public String toString() {
        return entries.toString();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }
}
