package PureSampling;

import Common.Pair;
import Common.Query;
import Common.Topology;
import IMDB.Labels;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class NodePureSampling {
    private Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    private Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();
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

    private List<Integer> getSamples(List<Integer> candidates) {
        sampledIndices.clear();

        List<Integer> samples = new ArrayList<>();

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

    private int countQualified(List<Integer> physicals, Pair<String, Integer> filter) throws Exception {
        int numQualified = 0;
        for (Integer physical : physicals) {
            if (applyFilter(physical, filter)) numQualified++;
        }
        return numQualified;
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

    public double estimate(Query query) throws Exception {
        Topology topology = query.topology;

        Set<Integer> vertexSet = new HashSet<>(src2label2dest.keySet());
        vertexSet.addAll(dest2label2src.keySet());

        int numQueryVertices = topology.outgoing.size();

        final int START_VIRTUAL = 0;

        Set<Integer> currentStep = new HashSet<>();
        Set<Integer> nextStep = new HashSet<>();
        Set<Integer> traversed = new HashSet<>();

        Map<Integer, List<Integer>> virtual2samples = new HashMap<>();
        Map<Integer, Set<Integer>> virtual2physicals = new HashMap<>();
        List<Integer> startSamples = new ArrayList<>();
        List<Integer> physicalDests;

        double estAcrossPhysicals = 0;

        if (sampleSize > 0) {
            startSamples.addAll(getSamples(new ArrayList<>(vertexSet)));
        } else {
            startSamples.addAll(vertexSet);
        }

        for (Integer startPhysical : startSamples) {
            currentStep.clear();
            nextStep.clear();
            traversed.clear();
            virtual2physicals.clear();
            virtual2samples.clear();

            traversed.add(START_VIRTUAL);

            if (!applyFilter(startPhysical, query.filters.get(START_VIRTUAL))) continue;

            double startNumLeavesProduct = 1;
            if (src2label2dest.containsKey(startPhysical)) {
                for (Integer outLabel : topology.outgoing.get(START_VIRTUAL).keySet()) {
                    if (!src2label2dest.get(startPhysical).containsKey(outLabel)) continue;

                    // our queries assume this to be 1
                    Integer nextVirtual = topology.outgoing.get(START_VIRTUAL).get(outLabel).get(0);

                    physicalDests = src2label2dest.get(startPhysical).get(outLabel);

                    if (isLeaf(nextVirtual, topology)) {
                        traversed.add(nextVirtual);
                        startNumLeavesProduct *=
                            countQualified(physicalDests, query.filters.get(nextVirtual));
                    } else {
                        if (!traversed.contains(nextVirtual)) {
                            currentStep.add(nextVirtual);
                            virtual2physicals.putIfAbsent(nextVirtual, new HashSet<>());
                            virtual2physicals.get(nextVirtual).addAll(physicalDests);
                        }
                    }
                }
            }

            if (dest2label2src.containsKey(startPhysical)) {
                for (Integer inLabel : topology.incoming.get(START_VIRTUAL).keySet()) {
                    if (!dest2label2src.get(startPhysical).containsKey(inLabel)) continue;

                    // our queries assume this to be 1
                    Integer nextVirtual = topology.incoming.get(START_VIRTUAL).get(inLabel).get(0);

                    physicalDests = dest2label2src.get(startPhysical).get(inLabel);

                    if (isLeaf(nextVirtual, topology)) {
                        traversed.add(nextVirtual);
                        startNumLeavesProduct *=
                            countQualified(physicalDests, query.filters.get(nextVirtual));
                    } else {
                        if (!traversed.contains(nextVirtual)) {
                            currentStep.add(nextVirtual);
                            virtual2physicals.putIfAbsent(nextVirtual, new HashSet<>());
                            virtual2physicals.get(nextVirtual).addAll(physicalDests);
                        }
                    }
                }
            }

            while (!currentStep.isEmpty() && traversed.size() < numQueryVertices) {
                traversed.addAll(currentStep);

                for (Integer currentVirtual : currentStep) {
                    if (sampleSize > 0) {
                        virtual2samples.put(
                            currentVirtual,
                            getSamples(new ArrayList<>(virtual2physicals.get(currentVirtual)))
                        );
                    } else {
                        virtual2samples.put(
                            currentVirtual,
                            new ArrayList<>(virtual2physicals.get(currentVirtual))
                        );
                    }

                    int numLeavesAcrossPhysicals = 0;
                    for (Integer physical : virtual2samples.get(currentVirtual)) {
                        if (!applyFilter(physical, query.filters.get(currentVirtual))) continue;

                        int numLeavesProduct = 1;
                        if (src2label2dest.containsKey(physical)) {
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

                        if (dest2label2src.containsKey(physical)) {
                            for (Integer inLabel : topology.incoming.get(currentVirtual).keySet()) {
                                if (!dest2label2src.get(physical).containsKey(inLabel)) continue;

                                // our queries assume this to be 1
                                Integer nextVirtual = topology.incoming.get(currentVirtual).get(inLabel).get(0);

                                physicalDests = dest2label2src.get(physical).get(inLabel);

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

                        if (numLeavesProduct != 1) {
                            numLeavesAcrossPhysicals += numLeavesProduct;
                        }
                    }

                    int currentSampleSize = virtual2samples.get(currentVirtual).size();
                    int currentNumAllPhysicals = virtual2physicals.get(currentVirtual).size();
                    if (numLeavesAcrossPhysicals != 0) {
                        startNumLeavesProduct *= numLeavesAcrossPhysicals;
                    }
                    startNumLeavesProduct *= ((double) currentNumAllPhysicals) / currentSampleSize;
                }

                currentStep = nextStep;
                nextStep = new HashSet<>();
            }

            if (traversed.size() == numQueryVertices && startNumLeavesProduct != 1) {
                estAcrossPhysicals += startNumLeavesProduct;
            }
        }

        return estAcrossPhysicals / startSamples.size() * vertexSet.size();
    }

    public NodePureSampling(String graphFile, String propFile, int sampleSize) throws Exception {
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
