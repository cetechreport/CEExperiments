package CharacteristicSet;

import Common.Pair;
import Common.Query;
import Common.Topology;
import IMDB.Labels;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;

public class CharacteristicSets {
    public Map<CharacteristicSet, Set<Integer>> cSets;

    private Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    private Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();
    private Map<Integer, Map<Integer, List<Integer>>> label2src2dest = new HashMap<>();
    public Map<Integer, Integer> vid2prodYear = new HashMap<>();

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

    private int getStarCardinality(Set<Integer> outLabels, Set<Integer> inLabels) {
        int cardinality = 0;
        for (CharacteristicSet cSet : cSets.keySet()) {
            if (!cSet.containsStar(outLabels, inLabels)) continue;

            int m = 1;

            for (Integer outLabel : outLabels) {
                m *= cSet.getCount(outLabel, Constants.OUT);
            }

            for (Integer inLabel : inLabels) {
                m *= cSet.getCount(inLabel, Constants.IN);
            }

            cardinality += cSets.get(cSet).size() * m;
        }

        return cardinality;
    }

    public Pair<Integer, Pair<Map<Integer, Integer>, Map<Integer, Integer>>> getLargestStar(
            Topology topology, Set<Integer> covered, Map<Integer, Integer> leaf2numStars) {
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

        leaf2numStars.put(maxStarCentral, leaf2numStars.getOrDefault(maxStarCentral, 0) + 1);

        Integer endV;
        Map<Integer, Integer> incomingToBeRemoved = new HashMap<>();
        for (Integer outLabel : topology.outgoing.get(maxStarCentral).keySet()) {
            endV = topology.outgoing.get(maxStarCentral).get(outLabel).get(0);
            outLabel2dest.put(outLabel, endV);
            covered.add(outLabel);
            incomingToBeRemoved.put(endV, outLabel);
            leaf2numStars.put(endV, leaf2numStars.getOrDefault(endV, 0) + 1);
        }
        topology.outgoing.get(maxStarCentral).clear();

        Map<Integer, Integer> outgoingToBeRemoved = new HashMap<>();
        for (Integer inLabel : topology.incoming.get(maxStarCentral).keySet()) {
            endV = topology.incoming.get(maxStarCentral).get(inLabel).get(0);
            inLabel2dest.put(inLabel, endV);
            covered.add(inLabel);
            outgoingToBeRemoved.put(endV, inLabel);
            leaf2numStars.put(endV, leaf2numStars.getOrDefault(endV, 0) + 1);
        }
        topology.incoming.get(maxStarCentral).clear();

        for (Integer v : incomingToBeRemoved.keySet()) {
            if (topology.incoming.get(v).get(incomingToBeRemoved.get(v)).size() == 1) {
                topology.incoming.get(v).remove(incomingToBeRemoved.get(v));
            } else {
                topology.incoming.get(v).get(incomingToBeRemoved.get(v)).remove(maxStarCentral);
            }
        }

        for (Integer v : outgoingToBeRemoved.keySet()) {
            if (topology.outgoing.get(v).get(outgoingToBeRemoved.get(v)).size() == 1) {
                topology.outgoing.get(v).remove(outgoingToBeRemoved.get(v));
            } else {
                topology.outgoing.get(v).get(outgoingToBeRemoved.get(v)).remove(maxStarCentral);
            }
        }

        return new Pair<>(maxStarCentral, new Pair<>(outLabel2dest, inLabel2dest));
    }

    // TODO: maybe take inLabel/outLabel into account for ratio
    // TODO: i.e. how many of the vertices with specified inLabels/outLabels satisfy the predicate
    private double getRatio(Pair<String, Integer> filter) throws Exception {
        if (filter.key.isEmpty()) return 1.0;

        double qualified = 0;
        for (Integer vid : vid2prodYear.keySet()) {
            if (applyFilter(vid, filter)) qualified++;
        }
        return qualified / vid2prodYear.keySet().size();
    }

    private double estJoinSelectivity(Topology topology, Map<Integer, Integer> leaf2numStars) {
        Set<Integer> outLabels = new HashSet<>();
        Set<Integer> inLabels = new HashSet<>();

        double selProduct = 1.0;

        Set<Integer> allVertices = new HashSet<>(src2label2dest.keySet());
        allVertices.addAll(dest2label2src.keySet());

        for (Integer joinV : leaf2numStars.keySet()) {
            if (leaf2numStars.get(joinV) > 1) {
                outLabels.clear();
                inLabels.clear();
                outLabels.addAll(topology.outgoing.get(joinV).keySet());
                inLabels.addAll(topology.incoming.get(joinV).keySet());

                double qualified = 0;

                for (Integer v : allVertices) {
                    boolean containsAll = true;
                    for (Integer label : outLabels) {
                        containsAll = containsAll && src2label2dest.containsKey(v) &&
                            src2label2dest.get(v).containsKey(label);
                    }

                    if (!containsAll) continue;

                    for (Integer label : inLabels) {
                        containsAll = containsAll && dest2label2src.containsKey(v) &&
                            dest2label2src.get(v).containsKey(label);
                    }

                    if (containsAll) qualified++;
                }

                selProduct *= qualified / allVertices.size();
            }
        }

        return selProduct;
    }

    private double estSingleEdge(Integer center, Map<Integer, Integer> outLabel2dest,
        Map<Integer, Integer> inLabel2dest, List<Pair<String, Integer>> filters) throws Exception {

        double qualified = 0;
        if (!outLabel2dest.isEmpty()) {
            for (Integer label : outLabel2dest.keySet()) {
                for (Integer src : label2src2dest.get(label).keySet()) {
                    if (!applyFilter(src, filters.get(center))) continue;

                    for (Integer dest : label2src2dest.get(label).get(src)) {
                        if (applyFilter(dest, filters.get(outLabel2dest.get(label)))) {
                            qualified++;
                        }
                    }
                }
            }
        } else {
            for (Integer label : inLabel2dest.keySet()) {
                for (Integer src : label2src2dest.get(label).keySet()) {
                    if (!applyFilter(src, filters.get(inLabel2dest.get(label)))) continue;

                    for (Integer dest : label2src2dest.get(label).get(src)) {
                        if (applyFilter(dest, filters.get(center))) {
                            qualified++;
                        }
                    }
                }
            }
        }

        return qualified;
    }

    public double estStarCardinality(Integer center, Map<Integer, Integer> outLabel2dest,
            Map<Integer, Integer> inLabel2dest, List<Pair<String, Integer>> filters) throws Exception {
        Pair<String, Integer> filter;
        double ratio;
        double cardinality = 0;
        for (CharacteristicSet cSet : cSets.keySet()) {
            if (!cSet.containsStar(outLabel2dest.keySet(), inLabel2dest.keySet())) continue;

            double m = 1;

            for (Integer outLabel : outLabel2dest.keySet()) {
                filter = filters.get(outLabel2dest.get(outLabel));
                if (filter.value.equals(Labels.NO_FILTER)) {
                    ratio = 1.0;
                } else {
                    ratio = getRatio(filter);
                }
                m *= cSet.getCount(outLabel, Constants.OUT) * ratio;
            }

            for (Integer inLabel : inLabel2dest.keySet()) {
                filter = filters.get(inLabel2dest.get(inLabel));
                if (filter.value.equals(Labels.NO_FILTER)) {
                    ratio = 1.0;
                } else {
                    ratio = getRatio(filter);
                }
                m *= cSet.getCount(inLabel, Constants.IN) * ratio;
            }

            cardinality += cSets.get(cSet).size() * m;
        }

        return cardinality * getRatio(filters.get(center));
    }

    public double estimate(Query query) throws Exception {
        Topology topology = new Topology(query.topology);

        double estimation = 1;

        int numQueryEdges = 0;
        for (Map<Integer, List<Integer>> labels : topology.outgoing) {
            for (List<Integer> dests : labels.values()) {
                numQueryEdges += dests.size();
            }
        }

        Set<Integer> covered = new HashSet<>();
        // (outLabel2dest, inLabel2dest)
        Pair<Integer, Pair<Map<Integer, Integer>, Map<Integer, Integer>>> star;
        Map<Integer, Integer> leaf2numStars = new HashMap<>();
        while (covered.size() < numQueryEdges) {
            star = getLargestStar(topology, covered, leaf2numStars);
            if (star.value.key.size() + star.value.value.size() == 1) {
                estimation *= estSingleEdge(star.key, star.value.key, star.value.value, query.filters);
            } else {
                estimation *= estStarCardinality(star.key, star.value.key, star.value.value, query.filters);
            }
        }

        estimation *= estJoinSelectivity(query.topology, leaf2numStars);

        return estimation;
    }

    private void saveCSets() throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter("cSets.csv"));
        StringJoiner sj;
        for (CharacteristicSet cSet : cSets.keySet()) {
            sj = new StringJoiner(",");
            sj.add(cSet.toString());
            sj.add(Integer.toString(cSets.get(cSet).size()));

            for (Integer center : cSets.get(cSet)) {
                sj.add(Integer.toString(center));
            }

            writer.write(sj.toString() + "\n");
        }
        writer.close();
    }

    private void loadCSets() throws Exception {
        cSets = new HashMap<>();

        String[] cSetInfo, labelCount;
        CharacteristicSet cSet;

        BufferedReader reader = new BufferedReader(new FileReader("cSets.csv"));
        String line = reader.readLine();
        while (null != line) {
            cSetInfo = line.split(",");
            cSet = new CharacteristicSet();

            Integer numOutLabels = Integer.parseInt(cSetInfo[0]);
            for (int i = 1; i <= numOutLabels; ++i) {
                labelCount = cSetInfo[i].split("->");
                cSet.add(Integer.parseInt(labelCount[0]), Integer.parseInt(labelCount[1]), Constants.OUT);
            }

            Integer numInLabels = Integer.parseInt(cSetInfo[numOutLabels + 1]);
            for (int i = numOutLabels + 2; i <= numInLabels + numOutLabels + 1; ++i) {
                labelCount = cSetInfo[i].split("<-");
                cSet.add(Integer.parseInt(labelCount[0]), Integer.parseInt(labelCount[1]), Constants.IN);
            }

            cSets.put(cSet, new HashSet<>());

            Integer numCenters = Integer.parseInt(cSetInfo[numInLabels + numOutLabels + 2]);
            int START = numInLabels + numOutLabels + 3;
            for (int i = START; i < cSetInfo.length; ++i) {
                Integer center = Integer.parseInt(cSetInfo[i]);
                cSets.get(cSet).add(center);
            }

            // insanity check
            if (cSets.get(cSet).size() != numCenters) {
                System.err.println("ERROR: NUM_CENTERS DOES NOT MATCH");
                break;
            }

            line = reader.readLine();
        }
        reader.close();
    }

    private void breakdownAndMerge(Set<CharacteristicSet> toBreakdown, Set<CharacteristicSet> highFreq) {
        CharacteristicSet[] subsets, bestBreakdown;
        CharacteristicSet bestSub, betterSuper, bestSuper;
        List<CharacteristicSet> allSubs = new ArrayList<>();
        Set<CharacteristicSet> toRemove = new HashSet<>();

        int numCSets = toBreakdown.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (CharacteristicSet toMerge : toBreakdown) {
            progress += 100.0 / numCSets;
            System.out.print("\rBreaking Down: " + (int) progress + "%");

            allSubs.clear();
//            bestSub = null;
            for (CharacteristicSet cSet : highFreq) {
                if (cSet.isSubsetOf(toMerge)) {
                    allSubs.add(cSet);
//                    if (null == bestSub || cSet.size() > bestSub.size()) {
//                        bestSub = cSet;
//                    }
                }
            }

            bestSub = null;
            bestSuper = null;
            bestBreakdown = null;
            for (CharacteristicSet sub : allSubs) {
                subsets = toMerge.breakdown(sub);

                betterSuper = null;
                for (CharacteristicSet superSet : highFreq) {
                    if (subsets[1].isSubsetOf(superSet)) {
                        if (null == betterSuper) {
                            betterSuper = superSet;
                        } else {
                            // smaller super set is better
                            if (superSet.size() < betterSuper.size()) {
                                betterSuper = superSet;
                            // more frequent super set is better
                            } else if (superSet.size() == betterSuper.size() &&
                                       cSets.get(superSet).size() > cSets.get(betterSuper).size()) {
                                betterSuper = superSet;
                            }
                        }
                    }
                }

                if (betterSuper != null) {
                    if (null == bestBreakdown || subsets[0].size() > bestBreakdown[0].size()) {
                        bestBreakdown = subsets;
                        bestSub = sub;
                        bestSuper = betterSuper;
                    }
                }
            }

            if (bestBreakdown != null) {
                bestSub.merge(bestBreakdown[0]);
                cSets.get(bestSub).addAll(cSets.get(toMerge));
                bestSuper.merge(bestBreakdown[1]);
                cSets.get(bestSuper).addAll(cSets.get(toMerge));
                toRemove.add(toMerge);
            }
        }

        for (CharacteristicSet cSet : toRemove) {
            cSets.remove(cSet);
        }

        endTime = System.currentTimeMillis();
        System.out.println("\nBreaking Down: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void mergeCSets(int NUM_TO_KEEP) {
        PriorityQueue<Integer> minHeap = new PriorityQueue<>();

        Map<Integer, Set<CharacteristicSet>> freq2cSets = new HashMap<>();

        // get the min size of the top 10K cSets (i.e. threshold)
        for (CharacteristicSet cSet : cSets.keySet()) {
            if (minHeap.size() >= NUM_TO_KEEP && cSets.get(cSet).size() >= minHeap.peek()) {
                Integer removedFreq = minHeap.poll();
                freq2cSets.remove(removedFreq);

                minHeap.add(cSets.get(cSet).size());
                freq2cSets.putIfAbsent(cSets.get(cSet).size(), new HashSet<>());
                freq2cSets.get(cSets.get(cSet).size()).add(cSet);
            } else if (minHeap.size() < NUM_TO_KEEP) {
                minHeap.add(cSets.get(cSet).size());
                freq2cSets.putIfAbsent(cSets.get(cSet).size(), new HashSet<>());
                freq2cSets.get(cSets.get(cSet).size()).add(cSet);
            }
        }

        // put all high-freq into one set
        Set<CharacteristicSet> highFreqCSets = new HashSet<>();
        for (Set<CharacteristicSet> cSets : freq2cSets.values()) {
            highFreqCSets.addAll(cSets);
        }

        Set<CharacteristicSet> toRemove = new HashSet<>();
        Set<CharacteristicSet> toBreakdown = new HashSet<>();

        CharacteristicSet bestSuper;

        int numCSets = cSets.size();
        double progress = 0;

        // merge the rest (i.e. below the threshold)
        for (CharacteristicSet cSetToMerge : cSets.keySet()) {
            progress += 100.0 / numCSets;
            System.out.print("\rMerging: " + (int) progress + "%");

            if (highFreqCSets.contains(cSetToMerge)) continue;

            bestSuper = null;
            for (CharacteristicSet superSet : highFreqCSets) {
                if (cSetToMerge.isSubsetOf(superSet)) {
                    if (null == bestSuper) {
                        bestSuper = superSet;
                    } else {
                        // smaller super set is better
                        if (superSet.size() < bestSuper.size()) {
                            bestSuper = superSet;
                        // more frequent super set is better
                        } else if (superSet.size() == bestSuper.size() &&
                            cSets.get(superSet).size() > cSets.get(bestSuper).size()) {
                            bestSuper = superSet;
                        }
                    }
                }
            }

            if (bestSuper != null) {
                bestSuper.merge(cSetToMerge);
                cSets.get(bestSuper).addAll(cSets.get(cSetToMerge));
                toRemove.add(cSetToMerge);
            } else {
                toBreakdown.add(cSetToMerge);
            }
        }

//        breakdownAndMerge(toBreakdown, highFreqCSets);

        for (CharacteristicSet cSet : toRemove) {
            cSets.remove(cSet);
        }
    }

    public CharacteristicSets(String graphFile, String propFile, String saveOrLoad) throws Exception {
        cSets = new HashMap<>();

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

            line = csvReader.readLine();
        }
        csvReader.close();

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
        System.out.println("Loading Graph & Properties: " + ((endTime - startTime) / 1000.0) + " sec");

        if (saveOrLoad.contains("save")) {
            int numV = src2label2dest.size();
            double progress = 0;
            CharacteristicSet bestSuper;

            startTime = System.currentTimeMillis();
            // construct characteristic sets
            CharacteristicSet cSet;
            for (Integer v : src2label2dest.keySet()) {
                progress += 100.0 / numV;
                System.out.print("\rConstructing: " + (int) progress + "%");

                cSet = new CharacteristicSet(src2label2dest.get(v), dest2label2src.get(v));
                if (!cSets.containsKey(cSet)) {
                    cSets.put(cSet, new HashSet<>());
                }
                cSets.get(cSet).add(v);
            }
            endTime = System.currentTimeMillis();
            System.out.println("\ncSets Construction: " + ((endTime - startTime) / 1000.0) + " sec");

            startTime = System.currentTimeMillis();
            mergeCSets(10000);
            mergeCSets(50000);
            mergeCSets(10000);
            mergeCSets(30000);
            mergeCSets(5000);
            endTime = System.currentTimeMillis();
            System.out.println("\ncSets Merge: " + ((endTime - startTime) / 1000.0) + " sec");
            System.out.println("cSets Size: " + cSets.size());

            startTime = System.currentTimeMillis();
            saveCSets();
            endTime = System.currentTimeMillis();
            System.out.println("Saving cSets: " + ((endTime - startTime) / 1000.0) + " sec");
        } else if (saveOrLoad.contains("load")) {
            startTime = System.currentTimeMillis();
            loadCSets();
            endTime = System.currentTimeMillis();
            System.out.println("Loading cSets: " + ((endTime - startTime) / 1000.0) + " sec");
        }

//        startTime = System.currentTimeMillis();
//        mergeCSets();
//        endTime = System.currentTimeMillis();
//        System.out.println("\ncSets Merge: " + ((endTime - startTime) / 1000.0) + " sec");
//        System.out.println("Merged cSets Size: " + cSets.size());
    }
}
