package PartitionedEstimation;

import Common.Query;
import Common.Util;
import Graphflow.Constants;
import Graphflow.QueryToPatterns;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

public class PartitionedStats {
    // vListEdge -> hashID of src and dest -> subgraph
    private Map<String, Map<String, Subgraph>> partitions;

    // vListEdge -> hashIdComb -> label -> degree/count
    Map<String, Map<String, Map<String, Long>>> maxOutDegs = new HashMap<>();
    Map<String, Map<String, Map<String, Long>>> maxInDegs = new HashMap<>();
    Map<String, Map<String, Map<String, Long>>> labelCounts = new HashMap<>();

    public void compute(
        String graphFile, String queryFile, String queryVList, int partType, int budget)
        throws Exception {

        QueryToPatterns converter = new QueryToPatterns();
        List<Query> queries = converter.readQueries(queryFile);
        List<String> labelSeqs = new ArrayList<>();
        for (Query query : queries) {
            labelSeqs.add(converter.extractEdgeLabels(query.topology, Util.toVList(queryVList)));
        }

        Partitioner partitioner;
        if (partType == Constants.HASH) {
            partitioner = new HashPartitioner(graphFile, queryVList, budget);
        } else if (partType == Constants.DEG) {
            partitioner = new DegPartitioner(graphFile, queryVList, budget);
        } else {
            System.err.println("ERROR: unrecognized partitioning type");
            return;
        }
        this.partitions = partitioner.partition(queryVList, labelSeqs);

        long startTime = System.currentTimeMillis();
        long endTime;
        double progress = 0;
        int total = labelSeqs.size();

        for (String labelSeq : labelSeqs) {
            computeStats(queryVList, labelSeq);

            progress += 100.0 / total;
            System.out.print("\rComputing Stats: " + (int) progress + "%");
        }

        endTime = System.currentTimeMillis();
        System.out.println("\rComputing Stats: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void computeStats(String queryVList, String queryLabelSeq) {
        String[] vListEdges = queryVList.split(";");
        String[] labelSeq = queryLabelSeq.split("->");

        Map<Integer, Map<Integer, Set<Integer>>> label2src2dest;
        for (int i = 0; i < vListEdges.length; ++i) {
            for (String hashIdComb : partitions.get(vListEdges[i]).keySet()) {
                label2src2dest = partitions.get(vListEdges[i]).get(hashIdComb).label2src2dest;
                Integer label = Integer.parseInt(labelSeq[i]);
                long maxOutDeg = 0;
                long maxInDeg = 0;
                long labelCount = 0;
                if (label2src2dest.containsKey(label)) {
                    maxOutDeg = getMaxOutDeg(label2src2dest.get(label));
                    maxInDeg = getMaxInDeg(label2src2dest.get(label));
                    labelCount = getLabelCount(label2src2dest.get(label));
                }

                addStat(Constants.FORWARD, vListEdges[i], hashIdComb, labelSeq[i], maxOutDeg);
                addStat(Constants.BACKWARD, vListEdges[i], hashIdComb, labelSeq[i], maxInDeg);
                addStat(null, vListEdges[i], hashIdComb, labelSeq[i], labelCount);
            }
        }
    }

    private long getLabelCount(Map<Integer, Set<Integer>> src2dest) {
        long count = 0L;
        for (Set<Integer> dests : src2dest.values()) {
            count += dests.size();
        }
        return count;
    }

    private void addStat(Integer type, String vListEdge, String hashIdComb, String label, Long stat) {
        Map<String, Map<String, Map<String, Long>>> stats;
        if (type == null) {
            stats = labelCounts;
        } else if (type.equals(Constants.FORWARD)) {
            stats = maxOutDegs;
        } else {
            stats = maxInDegs;
        }

        stats.putIfAbsent(vListEdge, new HashMap<>());
        stats.get(vListEdge).putIfAbsent(hashIdComb, new HashMap<>());
        stats.get(vListEdge).get(hashIdComb).put(label, stat);
    }

    private long getMaxOutDeg(Map<Integer, Set<Integer>> src2dest) {
        long maxDeg = Long.MIN_VALUE;
        for (Set<Integer> dests : src2dest.values()) {
            maxDeg = Math.max(maxDeg, dests.size());
        }
        return maxDeg;
    }

    private long getMaxInDeg(Map<Integer, Set<Integer>> src2dest) {
        Map<Integer, Set<Integer>> dest2src = new HashMap<>();
        for (Integer src : src2dest.keySet()) {
            for (Integer dest : src2dest.get(src)) {
                dest2src.putIfAbsent(dest, new HashSet<>());
                dest2src.get(dest).add(src);
            }
        }

        long maxDeg = Long.MIN_VALUE;
        for (Set<Integer> srcs : dest2src.values()) {
            maxDeg = Math.max(maxDeg, srcs.size());
        }
        return maxDeg;
    }

    private void persist(Map<String, Map<String, Map<String, Long>>> stats,
        BufferedWriter writer, String prefix) throws Exception {
        for (String vListEdge : stats.keySet()) {
            for (String hashIdComb : stats.get(vListEdge).keySet()) {
                for (String label : stats.get(vListEdge).get(hashIdComb).keySet()) {
                    Long deg = stats.get(vListEdge).get(hashIdComb).get(label);
                    StringJoiner sj = new StringJoiner(",");
                    if (!prefix.isEmpty()) {
                        sj.add(prefix);
                    }
                    sj.add(vListEdge).add(hashIdComb).add(label).add(deg.toString());
                    writer.write( sj.toString() + "\n");
                }
            }
        }
    }

    private void persist(String maxDegFile, String labelCountFile) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(maxDegFile));
        persist(maxOutDegs, writer, Integer.toString(Constants.FORWARD));
        persist(maxInDegs, writer, Integer.toString(Constants.BACKWARD));
        writer.close();

        writer = new BufferedWriter(new FileWriter(labelCountFile));
        persist(labelCounts, writer, "");
        writer.close();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println("queryFile: " + args[1]);
        System.out.println("queryVList: " + args[2]);
        System.out.println("partType: " + args[3]);
        System.out.println("budget: " + args[4]);
        System.out.println("destMaxDegFile: " + args[5]);
        System.out.println("destLabelCountFile: " + args[6]);
        System.out.println();

        int partType = args[3].contains("hash") ? Constants.HASH : Constants.DEG;

        PartitionedStats partitionedStats = new PartitionedStats();
        partitionedStats.compute(args[0], args[1], args[2], partType, Integer.parseInt(args[4]));
        partitionedStats.persist(args[5], args[6]);
    }
}
