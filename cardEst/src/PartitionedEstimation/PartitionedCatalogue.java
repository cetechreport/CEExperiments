package PartitionedEstimation;

import Common.Pair;
import Common.Query;
import Common.Util;
import Graphflow.Catalogue;
import Graphflow.Constants;
import Graphflow.QueryToPatterns;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

public class PartitionedCatalogue {
    // TODO: I realized vList and hashIdComb should be re-ordered to vList -> hashIdComb
    // TODO: right now only when reading cat for estimation is ordered the above way
    // patternType -> hashIdComb -> vList -> labelSeq -> Long
    Map<Integer, Map<String, Map<String, Map<String, Long>>>> catalogues = new HashMap<>();

    // patternType -> hashIdComb -> distribution
    Map<Integer, Map<String, Distribution>> distributions;

    List<String> entries = new ArrayList<>();
    Map<String, Long> entry2card = new HashMap<>();

    private List<String> hashIdCombs = null;

    void addCat(Integer patternType, String hashIdComb, String vList, String labelSeq, Long card) {
        catalogues.putIfAbsent(patternType, new HashMap<>());
        catalogues.get(patternType).putIfAbsent(hashIdComb, new HashMap<>());
        catalogues.get(patternType).get(hashIdComb).putIfAbsent(vList, new HashMap<>());
        catalogues.get(patternType).get(hashIdComb).get(vList).put(labelSeq, card);
    }

    void persistCat(String destFile) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(destFile));
        for (Integer patternType : catalogues.keySet()) {
            for (String hashIdComb : catalogues.get(patternType).keySet()) {
                for (String vList : catalogues.get(patternType).get(hashIdComb).keySet()) {
                    Map<String, Long> labelSeq2card =
                        catalogues.get(patternType).get(hashIdComb).get(vList);

                    for (String labelSeq : labelSeq2card.keySet()) {
                        StringJoiner line = new StringJoiner(",");
                        line.add(patternType.toString()).add(hashIdComb).add(vList);
                        line.add(labelSeq).add(labelSeq2card.get(labelSeq).toString());
                        writer.write(line.toString() + "\n");
                    }
                }
            }
        }
        writer.close();
    }

    public void computeAndPersistCatMaxDeg(String destFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        List<String> maxDegs;
        Distribution dist;

        BufferedWriter writer = new BufferedWriter(new FileWriter(destFile));
        for (Integer patternType : distributions.keySet()) {
            for (String hashIdComb : distributions.get(patternType).keySet()) {
                dist = distributions.get(patternType).get(hashIdComb);
                maxDegs = dist.getMaxDegs();

                for (String line : maxDegs) {
                    writer.write(patternType + "," + hashIdComb + "," + line + "\n");
                }
            }
        }
        writer.close();

        endTime = System.currentTimeMillis();
        System.out.println("Computing CatMaxDeg: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    // assumes catLength >= 2
    public void construct(String graphFile, String queryVList, List<String> queryLabelSeqs,
        int partType, int budget, String catalogueFile, int catLength) throws Exception {

        PartitionedDistribution partitionedDistribution = new PartitionedDistribution();
        for (int len = catLength; len >= 1; --len) {
            if (len > 1) {
                partitionedDistribution.construct(
                    graphFile, queryVList, queryLabelSeqs, partType, budget, catalogueFile, len);
                this.distributions = partitionedDistribution.distributions;
            }

            long startTime = System.currentTimeMillis();
            long endTime;

            String[] info;
            Integer patternType;
            String vList, labelSeq;
            Map<String, Set<String>> vList2labelSeqs = new HashMap<>();
            Set<String> queryCatLabelSeqs;
            for (String entry : entries) {
                info = entry.split(",");
                patternType = Integer.parseInt(info[0]);
                vList = info[1];
                labelSeq = info[2];
                if (labelSeq.split("->").length != len) continue;

                vList2labelSeqs.putIfAbsent(
                    vList, Util.extractLabelSeqs(vList, queryVList, queryLabelSeqs));
                queryCatLabelSeqs = vList2labelSeqs.get(vList);
                if (!queryCatLabelSeqs.contains(labelSeq)) continue;

                if (len > 1) {
                    computeCardinality(distributions, patternType, vList, labelSeq);
                } else {
                    computeCardinalityLen1(partitionedDistribution, patternType, vList, labelSeq);
                }
            }

            endTime = System.currentTimeMillis();
            System.out.println("Catalogue Construction (len " + len + "): " +
                ((endTime - startTime) / 1000.0) + " sec");
        }
    }

    private void computeCardinalityLen1(
        PartitionedDistribution partitionedDistribution,
        Integer patternType, String vListEdge, String labelString) {
        Map<String, Map<String, Subgraph>> partitions = partitionedDistribution.getPartitions();

        Integer label = Integer.parseInt(labelString);

        long totalCardAcrossPartitions = 0;
        for (String hashIdComb : partitions.get(vListEdge).keySet()) {
            Subgraph subgraph = partitions.get(vListEdge).get(hashIdComb);

            long cardinality = 0;
            if (subgraph.label2src2dest.containsKey(label)) {
                for (Set<Integer> dests : subgraph.label2src2dest.get(label).values()) {
                    cardinality += dests.size();
                }
            }
            totalCardAcrossPartitions += cardinality;

            addCat(patternType, hashIdComb, vListEdge, labelString, cardinality);
        }

        // not equal to the cardinality in the original catalogue
        if (!entry2card.get(patternType + "," + vListEdge + "," + label).equals(totalCardAcrossPartitions)) {
            System.err.println("ERROR: sanity check failed (cardinality)");
            System.err.println("   " + vListEdge + "," + label);
            System.err.println("   " + entry2card.get(patternType + "," + vListEdge + "," + label));
            System.err.println("   " + totalCardAcrossPartitions);
        }
    }

    private void computeCardinality(Map<Integer, Map<String, Distribution>> distributions,
        Integer patternType, String vList, String labelSeq) {

        List<Pair<Pair<String, String>, Pair<String, String>>> baseAndExts =
            Util.splitToBaseAndExt(vList, labelSeq, Util.getLeaves(Util.toVList(vList)));

        Set<Long> cardinalities = new HashSet<>();

        Distribution distribution;
        String baseVList, baseLabelSeq, extVList, extLabel;
        for (Pair<Pair<String, String>, Pair<String, String>> baseAndExt : baseAndExts) {
            long totalCardAcrossPartitions = 0;
            for (String hashIdComb : distributions.get(patternType).keySet()) {
                distribution = distributions.get(patternType).get(hashIdComb);
                baseVList = baseAndExt.key.key;
                baseLabelSeq = baseAndExt.value.key;
                extVList = baseAndExt.key.value;
                extLabel = baseAndExt.value.value;
                if (!distribution.entries.containsKey(baseVList)) continue;
                if (!distribution.entries.get(baseVList).get(baseLabelSeq).containsKey(extVList)) continue;

                Histogram hist = distribution.entries
                    .get(baseVList).get(baseLabelSeq).get(extVList).get(extLabel);

                long cardinality = 0;
                for (Integer extSize : hist.histogram.keySet()) {
                    cardinality += hist.histogram.get(extSize) * extSize;
                }
                totalCardAcrossPartitions += cardinality;

                addCat(patternType, hashIdComb, vList, labelSeq, cardinality);
            }

            cardinalities.add(totalCardAcrossPartitions);
        }

        if (cardinalities.size() > 1) {
            System.err.println("ERROR: sanity check failed (across partitions)");
            System.err.println("   " + cardinalities);
            return;
        }

        // not equal to the cardinality in the original catalogue
        if (!cardinalities.contains(entry2card.get(patternType + "," + vList + "," + labelSeq))) {
            System.err.println("ERROR: sanity check failed (cardinality)");
            System.err.println("   " + vList + "," + labelSeq);
            System.err.println("   " + entry2card.get(patternType + "," + vList + "," + labelSeq));
            System.err.println("   " + cardinalities);
        }
    }

    private void readCatalogue(String catalogueFile, int maxLen) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader reader = new BufferedReader(new FileReader(catalogueFile));

        String patternType, vList, labelSeq;
        Long card;
        String line = reader.readLine();
        String[] info;
        while (line != null) {
            info = line.split(",");
            patternType = info[0];
            vList = info[1];
            labelSeq = info[2];
            card = Long.parseLong(info[3]);

            if (labelSeq.split("->").length <= maxLen) {
                String entry = patternType + "," + vList + "," + labelSeq;
                entries.add(entry);
                entry2card.put(entry, card);
            }

            line = reader.readLine();
        }
        reader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Loading Catalogue: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void readPartitionedCatalogue(String pcatFile, int catLen) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(pcatFile));
        String line = reader.readLine();
        while (line != null) {
            String[] info = line.split(",");
            Integer patternType = Integer.parseInt(info[0]);
            String hashIdComb = info[1];
            String vList = info[2];
            String labelSeq = info[3];
            Long card = Long.parseLong(info[4]);

            catalogues.putIfAbsent(patternType, new HashMap<>());
            catalogues.get(patternType).putIfAbsent(vList, new HashMap<>());
            catalogues.get(patternType).get(vList).putIfAbsent(hashIdComb, new HashMap<>());
            catalogues.get(patternType).get(vList).get(hashIdComb).put(labelSeq, card);

            line = reader.readLine();
        }
        reader.close();
    }

    private Map<String, Set<String>> getVListEdge2HashIdCombs() {
        Map<String, Set<String>> vListEdge2HashIdCombs = new HashMap<>();

        for (Map<String, Map<String, Map<String, Long>>> entries : catalogues.values()) {
            for (String vList : entries.keySet()) {
                for (String hashIdComb : entries.get(vList).keySet()) {
                    String[] hashIds = hashIdComb.split(";");
                    String[] vListEdges = vList.split(";");

                    if (vListEdges.length == 2) {
                        vListEdge2HashIdCombs.putIfAbsent(vListEdges[0], new HashSet<>());
                        vListEdge2HashIdCombs.putIfAbsent(vListEdges[1], new HashSet<>());
                        vListEdge2HashIdCombs.get(vListEdges[0]).add(hashIds[0]);
                        vListEdge2HashIdCombs.get(vListEdges[1]).add(hashIds[1]);
                    } else if (vListEdges.length == 1) {
                        vListEdge2HashIdCombs.putIfAbsent(vListEdges[0], new HashSet<>());
                        vListEdge2HashIdCombs.get(vListEdges[0]).add(hashIds[0]);
                    }
                }
            }
        }

        return vListEdge2HashIdCombs;
    }

    public void prepareEstimate(Query query, int partType) {
        Pair<String, String> vListAndLabelSeq = Util.topologyToVListAndLabelSeq(query.topology);
        vListAndLabelSeq = Util.sort(vListAndLabelSeq);
        if (hashIdCombs == null) {
            hashIdCombs = Util.getQueryHashIdCombs(
                vListAndLabelSeq.key, getVListEdge2HashIdCombs(), partType);
        }
    }

    public Double[] estimate(Query query, Integer patternType, int formulaType, int catLen) throws Exception {
        Pair<String, String> vListAndLabelSeq = Util.topologyToVListAndLabelSeq(query.topology);
        vListAndLabelSeq = Util.sort(vListAndLabelSeq);

        Map<String, Double> formula2est = new HashMap<>();
        Map<String, Double> formula2partEst;

        for (String hashIdString : hashIdCombs) {
            List<Map<String, Map<String, Long>>> partitionedCatalogue = new ArrayList<>();

            while (partitionedCatalogue.size() <= patternType) {
                partitionedCatalogue.add(new HashMap<>());
            }

            boolean hasZeroCard = false;
            for (Map<String, Map<String, Map<String, Long>>> catEntries : catalogues.values()) {
                for (String catVList : catEntries.keySet()) {
                    String catHashId =
                        Util.extractHashIdComb(catVList, vListAndLabelSeq.key, hashIdString);
                    String catLabelSeq =
                        Util.extractLabelSeq(catVList, vListAndLabelSeq.key, vListAndLabelSeq.value);

                    long catCard = catEntries.get(catVList).get(catHashId).get(catLabelSeq);
                    if (catCard == 0) {
                        hasZeroCard = true;
                        break;
                    }

                    partitionedCatalogue.get(patternType).putIfAbsent(catVList, new HashMap<>());
                    partitionedCatalogue.get(patternType).get(catVList).put(
                        catLabelSeq,
                        catEntries.get(catVList).get(catHashId).get(catLabelSeq));
                }
            }

            if (hasZeroCard) continue;

            Catalogue catalogueEstimator = new Catalogue(partitionedCatalogue);
            formula2partEst = catalogueEstimator.estimateByFormula(
                query, patternType, formulaType, catLen);

            for (String formula : formula2partEst.keySet()) {
                Double partialEst = formula2partEst.get(formula);
                formula2est.put(
                    formula,
                    formula2est.getOrDefault(formula, 0.0) + partialEst);
            }
        }

        double sum = 0, min = Double.MAX_VALUE, max = -Double.MAX_VALUE;
        for (Double est : formula2est.values()) {
            sum += est;
            min = Math.min(min, est);
            max = Math.max(max, est);
        }

        return new Double[] { sum / formula2est.size(), min, max };
    }

    public PartitionedCatalogue(String pcatFile, int catLen) throws Exception {
        readPartitionedCatalogue(pcatFile, catLen);
    }

    public PartitionedCatalogue() {}

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println("catalogueFile: " + args[1]);
        System.out.println("specifiedCatLen: " + args[2]);
        System.out.println("queryFile: " + args[3]);
        System.out.println("queryVList: " + args[4]);
        System.out.println("partType: " + args[5]);
        System.out.println("budget: " + args[6]);
        System.out.println("catDestFile: " + args[7]);
        System.out.println("catMaxDegDestFile: " + args[8]);
        System.out.println();

        String vList = args[4];
        int partType = args[5].contains("hash") ? Constants.HASH : Constants.DEG;
        int budget = Integer.parseInt(args[6]);
        int catLength = Integer.parseInt(args[2]);

        QueryToPatterns converter = new QueryToPatterns();
        List<Query> queries = converter.readQueries(args[3]);
        List<String> labelSeqs = new ArrayList<>();
        for (Query query : queries) {
            labelSeqs.add(converter.extractEdgeLabels(query.topology, Util.toVList(vList)));
        }

        PartitionedCatalogue partitionedCatalogue = new PartitionedCatalogue();
        partitionedCatalogue.readCatalogue(args[1], catLength);
        partitionedCatalogue.construct(
            args[0], vList, labelSeqs, partType, budget, args[1], catLength);
        partitionedCatalogue.persistCat(args[7]);
        partitionedCatalogue.computeAndPersistCatMaxDeg(args[8]);
    }
}
