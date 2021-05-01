package Common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class Topology {
    // representational src id -> label -> representational dest ids
    public List<Map<Integer, List<Integer>>> outgoing;
    // representational dest id -> label -> representational src ids
    public List<Map<Integer, List<Integer>>> incoming;
    // representational src id -> representational dest ids -> label
    public List<Map<Integer, Integer>> src2dest2label;

    private Pair<String, String> printables = null;

    public Topology(List<Map<Integer, List<Integer>>> outgoing) {
        this.outgoing = outgoing;
    }

    public Topology() {
        this.outgoing = new ArrayList<>();
        this.incoming = new ArrayList<>();
        this.src2dest2label = new ArrayList<>();
    }

    public Topology(Topology topology) {
        this.outgoing = new ArrayList<>();
        Map<Integer, List<Integer>> clone;
        for (Map<Integer, List<Integer>> label2dests : topology.outgoing) {
            clone = new HashMap<>();
            for (Integer v : label2dests.keySet()) {
                clone.put(v, new ArrayList<>(label2dests.get(v)));
            }
            this.outgoing.add(clone);
        }

        this.incoming = new ArrayList<>();
        for (Map<Integer, List<Integer>> label2dests : topology.incoming) {
            clone = new HashMap<>();
            for (Integer v : label2dests.keySet()) {
                clone.put(v, new ArrayList<>(label2dests.get(v)));
            }
            this.incoming.add(clone);
        }

        this.src2dest2label = new ArrayList<>();
        for (Map<Integer, Integer> label2dest : topology.src2dest2label) {
            this.src2dest2label.add(new HashMap<>(label2dest));
        }
    }

    public void addEdge(Integer src, Integer label, Integer dest) {
        while (Math.max(src, dest) > outgoing.size() - 1) {
            outgoing.add(new HashMap<>());
        }
        while (Math.max(src, dest) > incoming.size() - 1) {
            incoming.add(new HashMap<>());
        }
        while (Math.max(src, dest) > src2dest2label.size() - 1) {
            src2dest2label.add(new HashMap<>());
        }

        if (outgoing.get(src).containsKey(label)) {
            outgoing.get(src).get(label).add(dest);
        } else {
            outgoing.get(src).put(label, new ArrayList<>());
            outgoing.get(src).get(label).add(dest);
        }

        if (incoming.get(dest).containsKey(label)) {
            incoming.get(dest).get(label).add(src);
        } else {
            incoming.get(dest).put(label, new ArrayList<>());
            incoming.get(dest).get(label).add(src);
        }

        src2dest2label.get(src).put(dest, label);
    }

    public void removeEdge(Integer src, Integer label, Integer dest) {
        outgoing.get(src).get(label).remove(dest);
        if (outgoing.get(src).get(label).isEmpty()) {
            outgoing.get(src).remove(label);
        }

        incoming.get(dest).get(label).remove(src);
        if (incoming.get(dest).get(label).isEmpty()) {
            incoming.get(dest).remove(label);
        }

        src2dest2label.get(src).remove(dest);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Topology topology = (Topology) o;

        if (src2dest2label.size() != topology.src2dest2label.size()) return false;

        for (int i = 0; i < src2dest2label.size(); ++i) {
            if (!src2dest2label.get(i).equals(topology.src2dest2label.get(i))) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        for (Map<Integer, Integer> dest2label : src2dest2label) {
            for (Integer dest : dest2label.keySet()) {
                result += dest + dest2label.get(dest);
            }
        }
        return result;
    }

    @Override
    public String toString() {
        if (printables == null) {
            List<String> vListEdges = new ArrayList<>();
            StringJoiner labelSeqJoiner = new StringJoiner("->");
            for (int src = 0; src < src2dest2label.size(); ++src) {
                for (Integer dest : src2dest2label.get(src).keySet()) {
                    vListEdges.add(src + "-" + dest);
                    labelSeqJoiner.add(src2dest2label.get(src).get(dest).toString());
                }
            }

            String queryVList = String.join(";", vListEdges);
            Collections.sort(vListEdges);
            String sortedVList = String.join(";", vListEdges);
            String sortedLabelSeq =
                Util.extractLabelSeq(sortedVList, queryVList, labelSeqJoiner.toString());
            printables = new Pair<>(sortedVList, sortedLabelSeq);
        }

        return printables.key + "," + printables.value;
    }
}
