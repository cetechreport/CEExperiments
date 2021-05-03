package Graphflow;

import Common.Pair;
import Common.Query;
import Common.Triple;
import Common.Util;

import java.util.*;


public class EntropyParallel implements Runnable {

    private int threadId;
    private Query query;
    private Integer patternType;
    private int formulaType;
    private int catLen;
    private String VList;
    private Integer[] vList;
    public List<Triple<List<Double>, String, Double>> alreadyCovered = new ArrayList<>();
    // baseVList -> baseLabelSeq -> extVList -> extLabel -> cv/entropy
    private final Map<String, Map<String, Map<String, Map<String, Double>>>> uniformityMeasure;

    double getEntropy(Pair<String, String> overlap, String nextLabelSeq, String extendVListString) {
        String[] vlistSeq = extendVListString.split(";");
        String[] labelSeq = nextLabelSeq.split("->");
        String baseVList = overlap.value;
        String baseLabelSeq = overlap.key;
        String extVList = "";
        String extLabelSeq = "";
        for (int i = 0; i < vlistSeq.length; ++i) {
            String from = vlistSeq[i].split("-")[0];
            String to = vlistSeq[i].split("-")[1];
            if(!overlap.value.contains(vlistSeq[i])) {
                extVList += vlistSeq[i] + ";";
                extLabelSeq += labelSeq[i] + "->";
            }
        }
        extVList = extVList.substring(0, extVList.length() - 1);
        extLabelSeq = extLabelSeq.substring(0, extLabelSeq.length() - 2);
        return uniformityMeasure.get(baseVList).get(baseLabelSeq).get(extVList).get(extLabelSeq);
    }

    public void getAllEstimatesBFS() {
        Set<Triple<List<Double>, String, Double>> currentCoveredLabelsAndCard = new HashSet<>();
        Set<Triple<List<Double>, String, Double>> nextCoveredLabelsAndCard = new HashSet<>();
        String startLabelSeq, nextLabelSeq, vListString, extendVListString, formula;
        Pair<String, String> overlap;
        Set<Integer> nextLabelSet;
        ArrayList<Double> entro;
        Set<String> intersection = new HashSet<>();
        String[] nextVarr;
        Set<String> visited;
        Set<String> alledges = Catalogue.toVSet(VList);

        startLabelSeq = Catalogue.extractPath(query.topology, vList);
        vListString = Catalogue.toVListString(vList);
        double est = Catalogue.catalogue.get(patternType).get(vListString).get(startLabelSeq);

        currentCoveredLabelsAndCard.clear();
        currentCoveredLabelsAndCard.add(
                new Triple<>(new ArrayList<Double>(), vListString, est));

        for (int i = 0; i < query.topology.src2dest2label.size() - 1 - vList.length / 2; ++i) {
            for (Integer[] extendVList : Catalogue.getDecomByLen(catLen)) {

                nextLabelSeq = Catalogue.extractPath(query.topology, extendVList);
                nextLabelSet = Catalogue.toLabelSet(nextLabelSeq);
                extendVListString = Catalogue.toVListString(extendVList);
                nextVarr = extendVListString.split(";");

                if (!Util.doesEntryFitFormula(Catalogue.getType(extendVList), formulaType)) continue;

                Set<Triple<List<Double>, String, Double>> finished = new HashSet<>();
                for (Triple<List<Double>, String, Double> coveredAndCard : currentCoveredLabelsAndCard) {
                    if (Catalogue.toVSet(coveredAndCard.v2).equals(alledges)) {
                        finished.add(coveredAndCard);
                        alreadyCovered.add(coveredAndCard);
                    }
                }
                currentCoveredLabelsAndCard.removeAll(finished);

                for (Triple<List<Double>, String, Double> coveredAndCard : currentCoveredLabelsAndCard) {

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

                    double curEntro = getEntropy(overlap, nextLabelSeq, extendVListString);
                    entro = new ArrayList<Double>(coveredAndCard.v1);
                    entro.add(curEntro);

                    formula = coveredAndCard.v2;
                    formula += "," + extendVListString + "," + overlap.value;
                    nextCoveredLabelsAndCard.add(new Triple<>(entro, formula, est));
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
        getAllEstimatesBFS();
    }

    public EntropyParallel(int threadId, Query query, Integer patternType, int formulaType, int catLen,
                            String VList, Integer[] vList,
                           Map<String, Map<String, Map<String, Map<String, Double>>>> uniformityMeasure) {
        this.threadId = threadId;

        this.query = query;
        this.patternType = patternType;
        this.formulaType = formulaType;
        this.catLen = catLen;
        this.VList = VList;
        this.vList = vList;
        this.uniformityMeasure = uniformityMeasure;
    }
}