package CharacteristicSet;

import Common.Pair;
import Common.Path;
import Common.Query;
import Common.Topology;
import MarkovTable.AcyclicMarkovTable3;
import MarkovTable.Constants;
import MarkovTable.PropertyFilter.MT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CSetsMTCombined {
    private CharacteristicSets characteristicSets;
    private MT mt;

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

    public Pair<Integer, Pair<Map<Integer, Integer>, Map<Integer, Integer>>> getLargestStar(
            Topology topology, Set<Integer> covered, Map<Integer, Pair<Integer, Integer>> overlappedLabels) {
        Map<Integer, Integer> outLabel2dest = new HashMap<>();
        Map<Integer, Integer> inLabel2dest = new HashMap<>();

        int maxStarSize = Integer.MIN_VALUE;
        int maxStarCentral = -1;
        int starSize;
        for (int v = 0; v < topology.outgoing.size(); ++v) {
            starSize = topology.outgoing.get(v).size() + topology.incoming.get(v).size();
            if (starSize > maxStarSize) {
                maxStarSize = starSize;
                maxStarCentral = v;
            }
        }

        Integer endV;
        Map<Integer, Integer> incomingToBeRemoved = new HashMap<>();
        for (Integer outLabel : topology.outgoing.get(maxStarCentral).keySet()) {
            endV = topology.outgoing.get(maxStarCentral).get(outLabel).get(0);
            outLabel2dest.put(outLabel, endV);
            if (covered.contains(outLabel)) {
                overlappedLabels.put(outLabel, new Pair<>(maxStarCentral, endV));
            } else {
                covered.add(outLabel);
            }
            if (isLeaf(endV, topology)) {
                incomingToBeRemoved.put(endV, outLabel);
            }
        }

        Map<Integer, Integer> outgoingToBeRemoved = new HashMap<>();
        for (Integer inLabel : topology.incoming.get(maxStarCentral).keySet()) {
            endV = topology.incoming.get(maxStarCentral).get(inLabel).get(0);
            inLabel2dest.put(inLabel, endV);
            if (covered.contains(inLabel)) {
                overlappedLabels.put(inLabel, new Pair<>(endV, maxStarCentral));
            } else {
                covered.add(inLabel);
            }
            if (isLeaf(endV, topology)) {
                outgoingToBeRemoved.put(endV, inLabel);
            }
        }

        for (Integer v : incomingToBeRemoved.keySet()) {
            if (topology.incoming.get(v).get(incomingToBeRemoved.get(v)).size() == 1) {
                topology.incoming.get(v).remove(incomingToBeRemoved.get(v));
            } else {
                topology.incoming.get(v).get(incomingToBeRemoved.get(v)).remove(maxStarCentral);
            }

            if (topology.outgoing.get(maxStarCentral).get(incomingToBeRemoved.get(v)).size() == 1) {
                topology.outgoing.get(maxStarCentral).remove(incomingToBeRemoved.get(v));
            } else {
                topology.outgoing.get(maxStarCentral).get(incomingToBeRemoved.get(v)).remove(v);
            }
        }

        for (Integer v : outgoingToBeRemoved.keySet()) {
            if (topology.outgoing.get(v).get(outgoingToBeRemoved.get(v)).size() == 1) {
                topology.outgoing.get(v).remove(outgoingToBeRemoved.get(v));
            } else {
                topology.outgoing.get(v).get(outgoingToBeRemoved.get(v)).remove(maxStarCentral);
            }

            if (topology.incoming.get(maxStarCentral).get(outgoingToBeRemoved.get(v)).size() == 1) {
                topology.incoming.get(maxStarCentral).remove(outgoingToBeRemoved.get(v));
            } else {
                topology.incoming.get(maxStarCentral).get(outgoingToBeRemoved.get(v)).remove(v);
            }
        }

        return new Pair<>(maxStarCentral, new Pair<>(outLabel2dest, inLabel2dest));
    }

    private double estPath(Integer center, Map<Integer, Integer> outLabel2dest, Map<Integer,
            Integer> inLabel2dest, List<Pair<String, Integer>> filters) throws Exception {
        int entryType = -1;
        Path path = new Path(new ArrayList<>());
        List<Integer> virtualVList = new ArrayList<>();
        if (outLabel2dest.size() == 2 && inLabel2dest.size() == 0) {
            entryType = Constants.ENTRY_TYPE_3;
            for (Integer outLabel : outLabel2dest.keySet()) {
                path.append(outLabel);
                virtualVList.add(outLabel2dest.get(outLabel));
            }
            virtualVList.add(1, center);
        } else if (inLabel2dest.size() == 2 && outLabel2dest.size() == 0) {
            entryType = Constants.ENTRY_TYPE_2;
            for (Integer inLabel : inLabel2dest.keySet()) {
                path.append(inLabel);
                virtualVList.add(inLabel2dest.get(inLabel));
            }
            virtualVList.add(1, center);
        } else if (outLabel2dest.size() == 1 && inLabel2dest.size() == 1) {
            entryType = Constants.ENTRY_TYPE_1;
            for (Integer inLabel : inLabel2dest.keySet()) {
                path.append(inLabel);
                virtualVList.add(inLabel2dest.get(inLabel));
            }
            virtualVList.add(center);
            for (Integer outLabel : outLabel2dest.keySet()) {
                path.append(outLabel);
                virtualVList.add(outLabel2dest.get(outLabel));
            }
        }

        Query query = new Query(path, mt.extractFilters(filters, virtualVList));

//        System.out.println("Query2: " + query.path.toSimpleString() + ": " + query.filters);

        return mt.mts.get(2).get(entryType).get(path) * mt.computeProportion(query, entryType);
    }

    private Double[] extend(Set<Integer> coveredLabels, List<Pair<String, Integer>> filters) throws Exception {
        Set<Integer> extendedLabel = new HashSet<>();
        Map<Integer, Double> extendedLabel2minEst = new HashMap<>();
        Map<Integer, Double> extendedLabel2maxEst = new HashMap<>();
        List<Integer> vList = new ArrayList<>();
        Pair<Double, Double> cardAndProportion;
        Path path;
        Query numeratorQuery, denominatorQuery;
        for (int i = 0; i < mt.decomPaths.get(3).size(); ++i) {
            Set<Pair<LinkedHashSet<Integer>, List<Integer>>> pathsOfType = mt.decomPaths.get(3).get(i);
            for (Pair<LinkedHashSet<Integer>, List<Integer>> pathAndVList : pathsOfType) {
                path = mt.toPath(pathAndVList.key);

                vList.clear();
                denominatorQuery = mt.getOverlappedQuery(pathAndVList, coveredLabels, filters, vList);
                if (null == denominatorQuery) continue;

                int overlappedType = mt.getPathType(vList);
                numeratorQuery = new Query(path, mt.extractFilters(filters, pathAndVList.value));

                double localEst;
                if (mt.mts.get(3).get(i).containsKey(path)) {
                    localEst = mt.mts.get(3).get(i).get(path) * mt.computeProportion(numeratorQuery, i);
                } else {
                    cardAndProportion = mt.estimate3Path(numeratorQuery, i);
                    localEst = cardAndProportion.key * cardAndProportion.value;
                }

                localEst /= (mt.mts.get(2).get(overlappedType).get(denominatorQuery.path) *
                                mt.computeProportion(denominatorQuery, overlappedType));

                extendedLabel.clear();
                extendedLabel.addAll(pathAndVList.key);
                extendedLabel.removeAll(coveredLabels);
                for (Integer extended : extendedLabel) {
                    extendedLabel2minEst.put(
                        extended, Math.min(extendedLabel2minEst.getOrDefault(extended, Double.MAX_VALUE), localEst)
                    );
                    extendedLabel2maxEst.put(
                        extended, Math.max(extendedLabel2maxEst.getOrDefault(extended, 0.0), localEst)
                    );
                }
            }
        }

        coveredLabels.addAll(extendedLabel2minEst.keySet());

        Double[] estimations = new Double[]{1.0, 1.0};
        for (Double est : extendedLabel2minEst.values()) {
            estimations[0] *= est;
        }
        for (Double est : extendedLabel2maxEst.values()) {
            estimations[1] *= est;
        }

        return estimations;
    }

    private double estLabelCardinality(Path path, Pair<Integer, Integer> srcDest,
            List<Pair<String, Integer>> filters) throws Exception {
        List<Integer> vList = new ArrayList<>();
        vList.add(srcDest.key);
        vList.add(srcDest.value);
        Query query = new Query(path, mt.extractFilters(filters, vList));
        return mt.mts.get(1).get(0).get(path) * mt.computeProportion(query, 0);
    }

    public Double[] estimate(Query query) throws Exception {
        Topology topology = new Topology(query.topology);

        Double[] estimations = new Double[] {1.0, 1.0};

        int numQueryEdges = 0;
        for (Map<Integer, List<Integer>> labels : topology.outgoing) {
            for (List<Integer> dests : labels.values()) {
                numQueryEdges += dests.size();
            }
        }

        Set<Integer> covered = new HashSet<>();
        // (outLabel2dest, inLabel2dest)
        Pair<Integer, Pair<Map<Integer, Integer>, Map<Integer, Integer>>> star;
        Map<Integer, Pair<Integer, Integer>> overlappedLabel2srcdest = new HashMap<>();

        star = getLargestStar(topology, covered, overlappedLabel2srcdest);
        if (covered.size() == 2 && star.value.key.size() + star.value.value.size() == 2) {
            return Arrays.copyOfRange(mt.estimate(query), 1, 3); // min, max
        }

        estimations[0] *= characteristicSets.estStarCardinality(star.key, star.value.key,
                            star.value.value, query.filters);
        estimations[1] *= characteristicSets.estStarCardinality(star.key, star.value.key,
                            star.value.value, query.filters);

        mt.decomTo3Paths(query.topology);
        Double[] minAndMax;
        while (covered.size() < numQueryEdges) {
            minAndMax = extend(covered, query.filters);
            for (int i = 0; i < minAndMax.length; ++i) {
                estimations[i] *= minAndMax[i];
            }
        }

        return estimations;
    }

    public CSetsMTCombined(String graphFile, String propFile, String saveOrLoad,
            String mtFileString, String sampleFileString, int numMTSamples) throws Exception {
        characteristicSets = new CharacteristicSets(graphFile, propFile, saveOrLoad);
        if (saveOrLoad.contains("save")) {
            AcyclicMarkovTable3 mt3 = new AcyclicMarkovTable3(graphFile, mtFileString, sampleFileString, numMTSamples);
            mt3.build();
        } else {
            mt = new MT(mtFileString.split(","), sampleFileString.split(","), propFile);
        }
    }
}
