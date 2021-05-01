package PartitionedEstimation;

import Common.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SubgraphFormer implements Runnable {
    private int threadId;
    private volatile Map<Integer, List<Pair<Integer, Integer>>> label2srcdest;
    private volatile Map<Integer, Map<Integer, List<Integer>>> src2label2dest;
    private volatile Map<Integer, Map<Integer, List<Integer>>> dest2label2src;

    private Pair<String, Long> entry;
    private String[] vListEdges;
    private String[] edgeHashIds;
    private Map<String, Map<String, Subgraph>> partitions;

    public void run() {
        label2srcdest = new HashMap<>();
        src2label2dest = new HashMap<>();
        dest2label2src = new HashMap<>();
        Subgraph integratedSubgraph = new Subgraph();
        Subgraph subgraph;

        String[] labelSeqEdges = entry.key.split(",")[2].split("->");
        for (int i = 0; i < vListEdges.length; ++i) {
            String vListEdge = vListEdges[i];
            String edgeHashId = edgeHashIds[i];
            Integer label = Integer.parseInt(labelSeqEdges[i]);
            subgraph = partitions.get(vListEdge).get(edgeHashId).extract(label);
            integratedSubgraph.add(subgraph);
        }

        for (Integer label : integratedSubgraph.label2src2dest.keySet()) {
            for (Integer src : integratedSubgraph.label2src2dest.get(label).keySet()) {
                for (Integer dest : integratedSubgraph.label2src2dest.get(label).get(src)) {
                    label2srcdest.putIfAbsent(label, new ArrayList<>());
                    label2srcdest.get(label).add(new Pair<>(src, dest));

                    src2label2dest.putIfAbsent(src, new HashMap<>());
                    src2label2dest.get(src).putIfAbsent(label, new ArrayList<>());
                    src2label2dest.get(src).get(label).add(dest);

                    dest2label2src.putIfAbsent(dest, new HashMap<>());
                    dest2label2src.get(dest).putIfAbsent(label, new ArrayList<>());
                    dest2label2src.get(dest).get(label).add(src);
                }
            }
        }
    }

    public int getThreadId() {
        return threadId;
    }

    public Map<Integer, List<Pair<Integer, Integer>>> getLabel2srcdest() {
        return label2srcdest;
    }

    public Map<Integer, Map<Integer, List<Integer>>> getSrc2label2dest() {
        return src2label2dest;
    }

    public Map<Integer, Map<Integer, List<Integer>>> getDest2label2src() {
        return dest2label2src;
    }

    public SubgraphFormer(
        int threadId,
        Pair<String, Long> entry,
        String[] vListEdges,
        String[] edgeHashIds,
        Map<String, Map<String, Subgraph>> partitions) {

        this.threadId = threadId;
        this.entry = entry;
        this.vListEdges = vListEdges;
        this.edgeHashIds = edgeHashIds;
        this.partitions = partitions;
    }
}
