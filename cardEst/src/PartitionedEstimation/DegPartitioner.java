package PartitionedEstimation;

import Common.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DegPartitioner extends Partitioner {
    public Map<String, Map<String, Subgraph>> partition(
        String vListString, List<String> labelSeqs, Map<Integer, Integer> numPartitions) {
        // vListEdge -> hashID of src and dest -> subgraph
        Map<String, Map<String, Subgraph>> partitions = new HashMap<>();

        Map<String, Set<Integer>> vListEdge2label = new HashMap<>();

        Integer[] vList = Util.toVList(vListString);
        for (String labelSeqString : labelSeqs) {
            Integer[] labelSeq = Util.toLabelSeq(labelSeqString);

            for (int i = 0; i < vList.length; i += 2) {
                Integer src = vList[i];
                Integer dest = vList[i + 1];
                Integer label = labelSeq[i / 2];
                String vListEdge = src + "-" + dest;

                vListEdge2label.putIfAbsent(vListEdge, new HashSet<>());
                if (vListEdge2label.get(vListEdge).contains(label)) continue;
                vListEdge2label.get(vListEdge).add(label);

                for (Integer srcPhysical : label2src2dest.get(label).keySet()) {
                    int srcDeg = label2src2dest.get(label).get(srcPhysical).size();
                    for (Integer destPhysical : label2src2dest.get(label).get(srcPhysical)) {
                        int destDeg = label2dest2src.get(label).get(destPhysical).size();
                        String bucketId = Util.bucketize(
                            srcDeg, numPartitions.get(src),
                            destDeg, numPartitions.get(dest));

                        partitions.putIfAbsent(vListEdge, new HashMap<>());
                        partitions.get(vListEdge).putIfAbsent(bucketId, new Subgraph());
                        partitions.get(vListEdge).get(bucketId).add(srcPhysical, label, destPhysical);
                    }
                }
            }
        }

        return partitions;
    }

    public Map<String, Map<String, Subgraph>> partition(String vListString, List<String> labelSeqs) {
        return partition(vListString, labelSeqs, numPartitions);
    }

    public void determineNumPartitions(String vListString, int expBase) {
        Integer[] vList = Util.toVList(vListString);
        Set<Integer> leaves = Util.getLeaves(vList);
        Set<Integer> joinVertices = new HashSet<>(Arrays.asList(vList));
        joinVertices.removeAll(leaves);

        numPartitions = new HashMap<>();
        for (int i = 0; i < vList.length / 2 + 1; ++i) {
            numPartitions.put(i, 0);
        }

        // partition for join vertices
        for (Integer v : joinVertices) {
            numPartitions.put(v, expBase);
        }
    }

    public DegPartitioner(String graphFile, String vList, int budget) {
        super(graphFile, vList, budget);
    }
}
