package Graphflow;

import Common.Pair;
import Common.Query;
import Common.Triple;
import Common.Util;

import java.util.*;

import static java.lang.Math.ceil;

public class EstimateParallel implements Runnable {
    public enum Type {
        MIN, ALL, MAX
    }

    private int threadId;
    private int subsetSize;
    private Query query;
    private Integer patternType;
    private int formulaType;
    private int catLen;
    private String VList;
    private Integer[] vList;
    boolean random;
    public List<Triple<Set<Integer>, String, Double>> alreadyCovered = new ArrayList<>();

    List<Triple<Integer, String, Double>> minhops = new ArrayList<>();
    List<Triple<Integer, String, Double>> maxhops = new ArrayList<>();
    List<Triple<Integer, String, Double>> randomhops = new ArrayList<>();

    private Integer[][] getOverlapDecoms(Integer[][] startingDecoms, Set<String> visited, Type type, int count, int allEdgeSize) {
        Set<String> intersection =new HashSet<>();
        List<Integer[]> overlaped = new ArrayList<>();
        for (Integer[] decom : startingDecoms) {
            String edges = Catalogue.toVListString(decom);
            String[] nextVarr = edges.split(";");
            intersection.clear();
            intersection.addAll(visited);
            intersection.retainAll(Arrays.asList(nextVarr));

            switch (type) {
                case MIN:
                    if (intersection.size() == 0 || intersection.size() == nextVarr.length ||
                            ((allEdgeSize - catLen) % 2 == 1 && intersection.size() == 2 && count >= 1) ||
                            ((allEdgeSize - catLen) % 2 == 0 && intersection.size() != 1)) {
                        continue;
                    }
                    break;

                case ALL:
                    if (intersection.size() == 0 ||
                            intersection.size() == nextVarr.length) continue;
                    break;

                case MAX:
                    if (intersection.size() != catLen - 1) continue;
                    break;
            }

            overlaped.add(decom);
        }
        Integer[][] arr = new Integer[overlaped.size()][];
        return overlaped.toArray(arr);
    }

    public List<Triple<Integer, String, Double>> getAllEstimatesDFS(Type type, int subsetSize) {
        Set<String> alledges = Catalogue.toVSet(VList);
        Catalogue.computeDecomByLength(query, catLen);
        Integer[][] startingDecoms = Catalogue.getDecomByLen(catLen);
        Set<String> intersection = new HashSet<>();
        List<Triple<Integer, String, Double>> formulaByHops = new ArrayList<>();

        while (formulaByHops.size() < subsetSize) {
            Random nextRand;
            Set<String> visited;
            Integer[] extendVList;
            String nextLabelSeq, extendVListString, formula;
            String[] nextVarr;
            Pair<String, String> overlap;

            Random rand = new Random();
            Integer[] vList = startingDecoms[rand.nextInt(startingDecoms.length)];
            String startLabelSeq = Catalogue.extractPath(query.topology, vList);
            String vListString = Catalogue.toVListString(vList);
            double est = Catalogue.catalogue.get(patternType).get(vListString).get(startLabelSeq);
            Triple<Integer, String, Double> coveredAndCard = new Triple<>(0, vListString, est);

            double len = (double) alledges.size() - catLen;
            if (type == Type.MIN) {
                len = (int) ceil(len / 2);
            }

            for (int i = 0; i < len; ++i) {
                visited = Catalogue.toVSet(coveredAndCard.v2);
                nextRand = new Random();
                Integer[][] nextDecoms = getOverlapDecoms(startingDecoms, visited, type, coveredAndCard.v1, alledges.size());
                if(type == Type.MIN && nextDecoms.length == 0) {
                    break;
                }
                extendVList = nextDecoms[nextRand.nextInt(nextDecoms.length)];

                nextLabelSeq = Catalogue.extractPath(query.topology, extendVList);
                extendVListString = Catalogue.toVListString(extendVList);
                nextVarr = extendVListString.split(";");

                intersection.clear();

                intersection.addAll(visited);
                intersection.retainAll(Arrays.asList(nextVarr));

                overlap = Catalogue.getOverlap(visited, nextLabelSeq, extendVListString);
                est = coveredAndCard.v3;

                est /= Catalogue.catalogue.get(patternType).get(overlap.value).get(overlap.key);
                est *= Catalogue.catalogue.get(patternType).get(extendVListString).get(nextLabelSeq);

                int count = intersection.size() == 2? coveredAndCard.v1 + 1 : coveredAndCard.v1;

                formula = coveredAndCard.v2;
                formula += "," + extendVListString + "," + overlap.value;
                coveredAndCard = new Triple<>(count, formula, est);

                if (Catalogue.toVSet(coveredAndCard.v2).equals(alledges)) {
                    formulaByHops.add(coveredAndCard);
                    break;
                }
            }
        }
        return formulaByHops;
    }

    public void getAllEstimatesBFS() {
        Set<Triple<Set<Integer>, String, Double>> currentCoveredLabelsAndCard = new HashSet<>();
        Set<Triple<Set<Integer>, String, Double>> nextCoveredLabelsAndCard = new HashSet<>();
        String startLabelSeq, nextLabelSeq, vListString, extendVListString, formula;
        Pair<String, String> overlap;
        Set<Integer> nextLabelSet, nextCovered;
        Set<String> intersection = new HashSet<>();
        String[] nextVarr;
        Set<String> visited;
        Set<String> alledges = Catalogue.toVSet(VList);

        startLabelSeq = Catalogue.extractPath(query.topology, vList);
        vListString = Catalogue.toVListString(vList);
        double est = Catalogue.catalogue.get(patternType).get(vListString).get(startLabelSeq);

        currentCoveredLabelsAndCard.clear();
        currentCoveredLabelsAndCard.add(
                new Triple<>(Catalogue.toLabelSet(startLabelSeq), vListString, est));

        for (int i = 0; i < query.topology.src2dest2label.size() - 1 - vList.length / 2; ++i) {
            for (Integer[] extendVList : Catalogue.getDecomByLen(catLen)) {

                nextLabelSeq = Catalogue.extractPath(query.topology, extendVList);
                nextLabelSet = Catalogue.toLabelSet(nextLabelSeq);
                extendVListString = Catalogue.toVListString(extendVList);
                nextVarr = extendVListString.split(";");

                if (!Util.doesEntryFitFormula(Catalogue.getType(extendVList), formulaType)) continue;

                Set<Triple<Set<Integer>, String, Double>> finished = new HashSet<>();
                for (Triple<Set<Integer>, String, Double> coveredAndCard : currentCoveredLabelsAndCard) {
                    if (Catalogue.toVSet(coveredAndCard.v2).equals(alledges)) {
                        finished.add(coveredAndCard);
                        alreadyCovered.add(coveredAndCard);
                    }
                }
                currentCoveredLabelsAndCard.removeAll(finished);

                for (Triple<Set<Integer>, String, Double> coveredAndCard : currentCoveredLabelsAndCard) {

                    intersection.clear();

                    visited = Catalogue.toVSet(coveredAndCard.v2);
                    intersection.addAll(visited);
                    intersection.retainAll(Arrays.asList(nextVarr));
//                        System.out.println("extended: " + extendVListString);
//                        System.out.println("visited: " + Arrays.toString(visited.toArray()));
//                        System.out.println("next V arr: " + Arrays.toString(nextVarr));
//                        System.out.println("intersection: " + Arrays.toString(intersection.toArray()));
//                        System.out.println();
                    if (intersection.size() == 0 ||
                            intersection.size() == nextVarr.length) {
                        continue;
                    }

                    overlap = Catalogue.getOverlap(visited, nextLabelSeq, extendVListString);
                    est = coveredAndCard.v3;

                    est /= Catalogue.catalogue.get(patternType).get(overlap.value).get(overlap.key);
                    est *= Catalogue.catalogue.get(patternType).get(extendVListString).get(nextLabelSeq);

                    nextCovered = new HashSet<>(coveredAndCard.v1);
                    nextCovered.addAll(nextLabelSet);

                    formula = coveredAndCard.v2;
                    formula += "," + extendVListString + "," + overlap.value;
                    nextCoveredLabelsAndCard.add(new Triple<>(nextCovered, formula, est));
                }

//                 currentCoveredLabelsAndCard = nextCoveredLabelsAndCard;
//                 nextCoveredLabelsAndCard = new HashSet<>();
            }
            currentCoveredLabelsAndCard = nextCoveredLabelsAndCard;
            nextCoveredLabelsAndCard = new HashSet<>();

        }

        alreadyCovered.addAll(currentCoveredLabelsAndCard);
    }

    public void run() {
        if (random) {
            minhops = getAllEstimatesDFS(Type.MIN, subsetSize);
            maxhops = getAllEstimatesDFS(Type.MAX, subsetSize);
            randomhops = getAllEstimatesDFS(Type.ALL, subsetSize);
            return;
        }
        getAllEstimatesBFS();

    }

    public EstimateParallel(int threadId, Query query, Integer patternType, int formulaType, int catLen,
                            String VList, Integer[] vList, int subsetSize, boolean random) {
        this.threadId = threadId;

        this.query = query;
        this.patternType = patternType;
        this.formulaType = formulaType;
        this.catLen = catLen;
        this.VList = VList;
        this.vList = vList;
        this.subsetSize = subsetSize;
        this.random = random;
    }

//    public static List<Triple<Set<Integer>, String, Double>> getEsts() {
//        return alreadyCovered;
//    }
}