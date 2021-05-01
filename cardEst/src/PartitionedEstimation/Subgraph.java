package PartitionedEstimation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Subgraph {
    public Map<Integer, Map<Integer, Set<Integer>>> label2src2dest = new HashMap<>();

    void add(Integer src, Integer label, Integer dest) {
        label2src2dest.putIfAbsent(label, new HashMap<>());
        label2src2dest.get(label).putIfAbsent(src, new HashSet<>());
        label2src2dest.get(label).get(src).add(dest);
    }

    public void add(Subgraph subgraph) {
        for (Integer label : subgraph.label2src2dest.keySet()) {
            for (Integer src : subgraph.label2src2dest.get(label).keySet()) {
                Set<Integer> dests = subgraph.label2src2dest.get(label).get(src);
                this.label2src2dest.putIfAbsent(label, new HashMap<>());
                this.label2src2dest.get(label).putIfAbsent(src, new HashSet<>());
                this.label2src2dest.get(label).get(src).addAll(dests);
            }
        }
    }

    public Subgraph extract(Integer label) {
        Subgraph subgraph = new Subgraph();
        if (label2src2dest.containsKey(label)) {
            subgraph.label2src2dest.put(label, new HashMap<>());
            for (Integer src : label2src2dest.get(label).keySet()) {
                Set<Integer> dests = new HashSet<>(label2src2dest.get(label).get(src));
                subgraph.label2src2dest.get(label).put(src, dests);
            }
        }
        return subgraph;
    }
}
