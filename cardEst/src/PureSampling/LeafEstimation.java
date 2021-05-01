package PureSampling;

import Common.Pair;
import Common.Query;
import Common.Topology;
import IMDB.Labels;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class LeafEstimation {
    private Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    private Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();

    private Map<Integer, Integer> vid2prodYear = new HashMap<>();

    private final int SAMPLE_SIZE = 1;
    private Random random = new Random(0);
    private Set<Integer> sampledIndices = new HashSet<>();

    private boolean applyFilter(List<Integer> sample, List<Pair<String, Integer>> filters)
        throws Exception {
        int vid, literal;
        String operator;

        boolean qualified = true;

        for (int i = 0; i < sample.size(); ++i) {
            operator = filters.get(i).key;
            if (operator.isEmpty()) continue;

            vid = sample.get(i);
            literal = filters.get(i).value;

            if (!vid2prodYear.containsKey(vid)) continue;

            switch (operator) {
                case "<":
                    qualified = qualified && vid2prodYear.get(vid) < literal;
                    break;
                case ">":
                    qualified = qualified && vid2prodYear.get(vid) > literal;
                    break;
                case "<=":
                    qualified = qualified && vid2prodYear.get(vid) <= literal;
                    break;
                case ">=":
                    qualified = qualified && vid2prodYear.get(vid) >= literal;
                    break;
                case "=":
                    qualified = qualified && vid2prodYear.get(vid) == literal;
                    break;
                default:
                    throw new Exception("ERROR: unrecognized operator: " + operator);
            }
        }

        return qualified;
    }

    private List<Integer> getSamples(List<Integer> vertices) {
        sampledIndices.clear();

        List<Integer> samples = new ArrayList<>();

        if (vertices.size() > SAMPLE_SIZE) {
            int sampledIndex;
            while (samples.size() < SAMPLE_SIZE) {
                sampledIndex = random.nextInt(vertices.size());
                while (sampledIndices.contains(sampledIndex)) {
                    sampledIndex = random.nextInt(vertices.size());
                }
                sampledIndices.add(sampledIndex);

                samples.add(vertices.get(sampledIndex));
            }
        } else {
            samples = vertices;
        }

        return samples;
    }

    private Set<Set<Integer>> getBranchEnds(Topology topology) {
        Set<Integer> branchStarts = new HashSet<>();
        for (List<Integer> branches : topology.outgoing.get(0).values()) {
            branchStarts.addAll(branches);
        }
        for (List<Integer> branches : topology.incoming.get(0).values()) {
            branchStarts.addAll(branches);
        }

        Set<Set<Integer>> branchEnds = new HashSet<>();
        Set<Integer> ends;
        Set<Integer> currentStep = new HashSet<>();
        Set<Integer> nextStep = new HashSet<>();
        Set<Integer> traversed = new HashSet<>();
        Set<Integer> outNeighbors = new HashSet<>();
        Set<Integer> inNeighbors = new HashSet<>();
        int queryNumV = topology.outgoing.size();

        for (Integer start : branchStarts) {
            ends = new HashSet<>();

            currentStep.clear();
            currentStep.add(start);
            nextStep.clear();
            traversed.clear();
            traversed.add(0);

            while (!currentStep.isEmpty() && traversed.size() < queryNumV) {
                traversed.addAll(currentStep);

                for (Integer v : currentStep) {
                    outNeighbors.clear();
                    for (List<Integer> nextVertices : topology.outgoing.get(v).values()) {
                        outNeighbors.addAll(nextVertices);
                    }
                    outNeighbors.removeAll(traversed);
                    nextStep.addAll(outNeighbors);

                    inNeighbors.clear();
                    for (List<Integer> nextVertices : topology.incoming.get(v).values()) {
                        inNeighbors.addAll(nextVertices);
                    }
                    inNeighbors.removeAll(traversed);
                    nextStep.addAll(inNeighbors);

                    if (outNeighbors.isEmpty() && inNeighbors.isEmpty()) ends.add(v);
                }

                currentStep = nextStep;
                nextStep = new HashSet<>();
            }

            branchEnds.add(ends);
        }

        System.out.println("branchEnds: " + branchEnds);

        return branchEnds;
    }

    public double estimate(Query query) throws Exception {
        Set<Integer> vertexSet = new HashSet<>(src2label2dest.keySet());
        vertexSet.addAll(dest2label2src.keySet());

        System.out.println(query.toString());

        Topology topology = query.topology;

        List<Integer> singleVertex = new ArrayList<>(1);
        singleVertex.add(0);
        List<Pair<String, Integer>> singleFilter = new ArrayList<>(1);
        singleFilter.add(new Pair<>("", 0));

        Set<Integer> intersection = new HashSet<>();
        boolean changed;

        double estimation = 0;
        Map<Integer, Integer> virtual2qualified = new HashMap<>();

        Map<Integer, List<Integer>> virtual2physical = new HashMap<>();
        Set<Integer> currentStepVirtual = new HashSet<>();
        Set<Integer> nextStepVirtual = new HashSet<>();
        Set<Integer> traversed = new HashSet<>();
        int queryNumV = topology.outgoing.size();
        Set<Set<Integer>> branchEnds = getBranchEnds(topology);

        final int START = 0;

//        virtual2physical.put(START, new ArrayList<>(vertexSet));
//        virtual2physical.put(START, getSamples(new ArrayList<>(vertexSet)));
        final List<Integer> START_VERTICES = getSamples(new ArrayList<>(vertexSet));

        for (int centerPhysical : START_VERTICES) {
            currentStepVirtual.clear();
            nextStepVirtual.clear();
            virtual2qualified.clear();
            virtual2physical.clear();
            traversed.clear();
            traversed.add(START);

            singleVertex.set(0, centerPhysical);
            singleFilter.set(0, query.filters.get(START));

            if (!applyFilter(singleVertex, singleFilter)) continue;

            // check if this physical vertex has all queried outgoing edges
            if (!topology.outgoing.get(START).isEmpty()) {
                if (!src2label2dest.containsKey(centerPhysical)) continue;

                intersection.clear();
                intersection.addAll(topology.outgoing.get(START).keySet());
                changed = intersection.retainAll(src2label2dest.get(centerPhysical).keySet());
                if (changed) continue;
            }

            // check if this physical vertex has all queried incoming edges
            if (!topology.incoming.get(START).isEmpty()) {
                if (!dest2label2src.containsKey(centerPhysical)) continue;

                intersection.clear();
                intersection.addAll(topology.incoming.get(START).keySet());
                changed = intersection.retainAll(dest2label2src.get(centerPhysical).keySet());
                if (changed) continue;
            }

            for (int outLabel : topology.outgoing.get(START).keySet()) {
                for (int outVirtual : topology.outgoing.get(START).get(outLabel)) {
                    virtual2physical.putIfAbsent(outVirtual, new ArrayList<>());
                    virtual2physical.get(outVirtual).addAll(src2label2dest.get(centerPhysical).get(outLabel));
                    currentStepVirtual.add(outVirtual);
                }
            }

            for (int inLabel : topology.incoming.get(START).keySet()) {
                for (int inVirtual : topology.incoming.get(START).get(inLabel)) {
                    virtual2physical.putIfAbsent(inVirtual, new ArrayList<>());
                    virtual2physical.get(inVirtual).addAll(dest2label2src.get(centerPhysical).get(inLabel));
                    currentStepVirtual.add(inVirtual);
                }
            }

            while (!currentStepVirtual.isEmpty() && traversed.size() < queryNumV) {
                traversed.addAll(currentStepVirtual);

                for (int virtual : currentStepVirtual) {
                    virtual2qualified.put(virtual, 0);

                    for (int physical : virtual2physical.get(virtual)) {
                        singleVertex.set(0, physical);
                        singleFilter.set(0, query.filters.get(virtual));

                        if (!applyFilter(singleVertex, singleFilter)) continue;

                        // check if this physical vertex has all queried outgoing edges
                        if (!topology.outgoing.get(virtual).isEmpty()) {
                            if (!src2label2dest.containsKey(physical)) continue;

                            intersection.clear();
                            intersection.addAll(topology.outgoing.get(virtual).keySet());
                            changed = intersection.retainAll(src2label2dest.get(physical).keySet());
                            if (changed) continue;
                        }

                        // check if this physical vertex has all queried incoming edges
                        if (!topology.incoming.get(virtual).isEmpty()) {
                            if (!dest2label2src.containsKey(physical)) continue;

                            intersection.clear();
                            intersection.addAll(topology.incoming.get(virtual).keySet());
                            changed = intersection.retainAll(dest2label2src.get(physical).keySet());
                            if (changed) continue;
                        }

                        virtual2qualified.put(virtual, virtual2qualified.get(virtual) + 1);

                        for (int outLabel : topology.outgoing.get(virtual).keySet()) {
                            for (int outVirtual : topology.outgoing.get(virtual).get(outLabel)) {
                                if (traversed.contains(outVirtual)) continue;

                                virtual2physical.putIfAbsent(outVirtual, new ArrayList<>());
                                virtual2physical.get(outVirtual).addAll(src2label2dest.get(physical).get(outLabel));
                                nextStepVirtual.add(outVirtual);
                            }
                        }

                        for (int inLabel : topology.incoming.get(virtual).keySet()) {
                            for (int inVirtual : topology.incoming.get(virtual).get(inLabel)) {
                                if (traversed.contains(inVirtual)) continue;

                                virtual2physical.putIfAbsent(inVirtual, new ArrayList<>());
                                virtual2physical.get(inVirtual).addAll(dest2label2src.get(physical).get(inLabel));
                                nextStepVirtual.add(inVirtual);
                            }
                        }
                    }

//                    System.out.println("virtual: " + virtual);
//                    System.out.println("#qualified: " + virtual2qualified);
                }

                currentStepVirtual = nextStepVirtual;
                nextStepVirtual = new HashSet<>();
            }

            // TODO: if we add getSamples to branch starts, we can do
            // TODO:    - divided by branch_start_num_samples, and then
            // TODO:    - multiplied by branch_start_num_physicals
            if (traversed.size() == queryNumV) {
                int estimationForThisCenter = 1;
                for (Set<Integer> ends : branchEnds) {
                    for (Integer leaf : ends) {
                        estimationForThisCenter *= virtual2qualified.get(leaf);
                    }
                }
//                System.out.println("#qualified: " + virtual2qualified + " - " + estimationForThisCenter);
                estimation += estimationForThisCenter;
            }
        }

        return estimation / START_VERTICES.size() * vertexSet.size();
    }

    public LeafEstimation(String graphFile, String propFile) throws Exception {
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
