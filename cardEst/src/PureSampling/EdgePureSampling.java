package PureSampling;

import Common.Pair;
import Common.Query;
import Common.Topology;
import IMDB.Labels;
import org.omg.CORBA.INTERNAL;

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringJoiner;

public class EdgePureSampling {
    private Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    private Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();
    private Map<Integer, Map<Integer, List<Integer>>> label2src2dest = new HashMap<>();
    private List<String> eid2edgeString = new ArrayList<>();
    private Map<Integer, Integer> vid2prodYear = new HashMap<>();

    private Random random = new Random(0);
    private int sampleSize;
    private Set<Integer> sampledIndices = new HashSet<>();

    private boolean applyFilter(Integer vid, Pair<String, Integer> filter) throws Exception {
        int literal;
        String operator;

        operator = filter.key;
        if (operator.isEmpty()) return true;

        literal = filter.value;

        if (!vid2prodYear.containsKey(vid)) return true;

        switch (operator) {
            case "<":
                return vid2prodYear.get(vid) < literal;
            case ">":
                return vid2prodYear.get(vid) > literal;
            case "<=":
                return vid2prodYear.get(vid) <= literal;
            case ">=":
                return vid2prodYear.get(vid) >= literal;
            case "=":
                return vid2prodYear.get(vid) == literal;
            default:
                throw new Exception("ERROR: unrecognized operator: " + operator);
        }
    }

    private List<String> getSamples(List<String> candidates) {
        sampledIndices.clear();

        List<String> samples = new ArrayList<>();

        if (candidates.size() > sampleSize) {
            int sampledIndex;
            while (samples.size() < sampleSize) {
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

    private Integer[] toEdgeArray(String edge) {
        Integer[] srcDest = new Integer[2];
        String[] srcDestStrings = edge.split(",");
        for (int i = 0; i < srcDestStrings.length; ++i) {
            srcDest[i] = Integer.parseInt(srcDestStrings[i]);
        }
        return srcDest;
    }

    private boolean isLeaf(Integer virtual, Topology topology) {
        Set<Integer> neighbors = new HashSet<>();
        for (List<Integer> outNeighbors : topology.outgoing.get(virtual).values()) {
            neighbors.addAll(outNeighbors);
        }
        for (List<Integer> inNeighbors : topology.incoming.get(virtual).values()) {
            neighbors.addAll(inNeighbors);
        }

        return neighbors.size() == 1;
    }

    private int countQualified(List<Integer> physicals, Pair<String, Integer> filter) throws Exception {
        int numQualified = 0;
        for (Integer physical : physicals) {
            if (applyFilter(physical, filter)) numQualified++;
        }
        return numQualified;
    }

    public double estimate(Query query) throws Exception {
        Topology topology = query.topology;
        Integer[] virtualEdge, physicalEdge;

        int startLabel = -1;
        int minDest = Integer.MAX_VALUE;
        for (Integer label : topology.outgoing.get(0).keySet()) {
            for (Integer dest : topology.outgoing.get(0).get(label)) {
                if (dest < minDest) {
                    minDest = dest;
                    startLabel = label;
                }
            }
        }
        final String START_EDGE = "0," + startLabel + "," + minDest;
        virtualEdge = toEdgeArray(START_EDGE);

        Set<String> currentEdges = new HashSet<>();
        Set<String> nextEdges = new HashSet<>();
        Set<Integer> traversed = new HashSet<>();
        final int NUM_QUERY_VERTICES = topology.outgoing.size();

        Map<String, Set<String>> virtual2physicals = new HashMap<>();
        Map<String, List<String>> virtual2samples = new HashMap<>();
        List<Integer> physicalDests;
        String virtualEdgeStr, physicalEdgeStr;
        int covered, uncovered;

        List<String> startSamples = getSamples(eid2edgeString);

        double estAcrossPhysicals = 0;
        for (String startPhysical : startSamples) {
            currentEdges.clear();
            nextEdges.clear();
            traversed.clear();
            virtual2physicals.clear();
            virtual2samples.clear();

            traversed.add(virtualEdge[0]);
            traversed.add(virtualEdge[2]);

            physicalEdge = toEdgeArray(startPhysical);
            if (!applyFilter(physicalEdge[0], query.filters.get(virtualEdge[0]))) continue;
            if (!applyFilter(physicalEdge[2], query.filters.get(virtualEdge[2]))) continue;

            double startNumLeavesProduct = 1;

            for (int i = 0; i < physicalEdge.length; i += 2) {
                if (src2label2dest.containsKey(physicalEdge[i])) {
                    for (Integer outLabel : topology.outgoing.get(virtualEdge[i]).keySet()) {
                        if (!src2label2dest.get(physicalEdge[i]).containsKey(outLabel)) continue;

                        // our queries assume this to be 1
                        Integer nextVirtual = topology.outgoing.get(virtualEdge[i]).get(outLabel).get(0);

                        physicalDests = src2label2dest.get(physicalEdge[i]).get(outLabel);

                        if (isLeaf(nextVirtual, topology)) {
                            traversed.add(nextVirtual);
                            startNumLeavesProduct *=
                                countQualified(physicalDests, query.filters.get(nextVirtual));
                        } else {
                            virtualEdgeStr = virtualEdge[i] + "," + outLabel + "," + nextVirtual;
                            if (!traversed.contains(nextVirtual)) {
                                currentEdges.add(virtualEdgeStr);
                                virtual2physicals.putIfAbsent(virtualEdgeStr, new HashSet<>());
                                for (Integer dest : physicalDests) {
                                    physicalEdgeStr = physicalEdge[i] + "," + outLabel + "," + dest;
                                    virtual2physicals.get(virtualEdgeStr).add(physicalEdgeStr);
                                }
                            }
                        }
                    }
                }

                if (dest2label2src.containsKey(physicalEdge[i])) {
                    for (Integer inLabel : topology.incoming.get(virtualEdge[i]).keySet()) {
                        if (!dest2label2src.get(physicalEdge[i]).containsKey(inLabel)) continue;

                        // our queries assume this to be 1
                        Integer nextVirtual = topology.incoming.get(virtualEdge[i]).get(inLabel).get(0);

                        physicalDests = dest2label2src.get(physicalEdge[i]).get(inLabel);

                        if (isLeaf(nextVirtual, topology)) {
                            traversed.add(nextVirtual);
                            startNumLeavesProduct *=
                                countQualified(physicalDests, query.filters.get(nextVirtual));
                        } else {
                            virtualEdgeStr = nextVirtual + "," + inLabel + "," + virtualEdge[i];
                            if (!traversed.contains(nextVirtual)) {
                                currentEdges.add(virtualEdgeStr);
                                virtual2physicals.putIfAbsent(virtualEdgeStr, new HashSet<>());
                                for (Integer dest : physicalDests) {
                                    physicalEdgeStr = physicalEdge[i] + "," + inLabel + "," + dest;
                                    virtual2physicals.get(virtualEdgeStr).add(physicalEdgeStr);
                                }
                            }
                        }
                    }
                }
            }

            while (!currentEdges.isEmpty() && traversed.size() < NUM_QUERY_VERTICES) {
                for (String currentVirtualEdge : currentEdges) {
                    virtualEdge = toEdgeArray(currentVirtualEdge);
                    if (traversed.contains(virtualEdge[0]) && traversed.contains(virtualEdge[2])) continue;

                    if (traversed.contains(virtualEdge[0])) {
                        covered = 0;
                        uncovered = 2;
                        traversed.add(virtualEdge[2]);
                    } else {
                        covered = 2;
                        uncovered = 0;
                        traversed.add(virtualEdge[0]);
                    }

                    virtual2samples.put(
                        currentVirtualEdge,
                        getSamples(new ArrayList<>(virtual2physicals.get(currentVirtualEdge)))
                    );

                    // TODO: wait for Semih understanding how to do sampling properly
                    /*
                    int numLeavesAcrossPhysicals = 0;
                    for (String physical : virtual2samples.get(currentVirtualEdge)) {
                        physicalEdge = toEdgeArray(physical);
                        if (!applyFilter(physicalEdge[uncovered], query.filters.get(virtualEdge[uncovered]))) continue;

                        int numLeavesProduct = 1;
                        if (src2label2dest.containsKey(physicalEdge[uncovered])) {
                            for (Integer outLabel : topology.outgoing.get(currentVirtual).keySet()) {
                                if (!src2label2dest.get(physical).containsKey(outLabel)) continue;

                                // our queries assume this to be 1
                                Integer nextVirtual = topology.outgoing.get(currentVirtual).get
                                    (outLabel).get(0);

                                physicalDests = src2label2dest.get(physical).get(outLabel);

                                if (isLeaf(nextVirtual, topology)) {
                                    traversed.add(nextVirtual);
                                    numLeavesProduct *=
                                        countQualified(physicalDests, query.filters.get(nextVirtual));
                                } else {
                                    if (!traversed.contains(nextVirtual)) {
                                        nextStep.add(nextVirtual);
                                        virtual2physicals.putIfAbsent(nextVirtual, new HashSet<>());
                                        virtual2physicals.get(nextVirtual).addAll(physicalDests);
                                    }
                                }
                            }
                        }
                    }
                    */
                }
            }
        }

        // TODO: scale up by ratio
        return estAcrossPhysicals;
    }

    public EdgePureSampling(String graphFile, String propFile, int sampleSize) throws Exception {
        this.sampleSize = sampleSize;

        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader csvReader = new BufferedReader(new FileReader(graphFile));
        int[] values;
        String line = csvReader.readLine();
        while (null != line) {
            values = Arrays.stream(line.split(",")).mapToInt(Integer::parseInt).toArray();

            src2label2dest.putIfAbsent(values[0], new HashMap<>());
            src2label2dest.get(values[0]).putIfAbsent(values[1], new ArrayList<>());
            src2label2dest.get(values[0]).get(values[1]).add(values[2]);

            dest2label2src.putIfAbsent(values[2], new HashMap<>());
            dest2label2src.get(values[2]).putIfAbsent(values[1], new ArrayList<>());
            dest2label2src.get(values[2]).get(values[1]).add(values[0]);

            label2src2dest.putIfAbsent(values[1], new HashMap<>());
            label2src2dest.get(values[1]).putIfAbsent(values[0], new ArrayList<>());
            label2src2dest.get(values[1]).get(values[0]).add(values[2]);

            eid2edgeString.add(line);

            line = csvReader.readLine();
        }

        csvReader.close();

        // Read property file
        String[] properties;

        BufferedReader tsvReader = new BufferedReader(new FileReader(propFile));
        tsvReader.readLine(); // Header

        line = tsvReader.readLine();
        while (null != line) {
            properties = line.split("\t");
            if (!properties[Labels.PROD_YEAR_INDEX].isEmpty()) {
                vid2prodYear.put(
                    Integer.parseInt(properties[0]), Integer.parseInt(properties[Labels.PROD_YEAR_INDEX])
                );
            }
            line = tsvReader.readLine();
        }
        tsvReader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Graph & Properties Loading: " + ((endTime - startTime) / 1000.0) + " sec");
    }
}
