package CEG;

import CEG.Parallel.PartitionedDistributionParallel;
import Common.Pair;
import Common.Query;
import Common.Util;
import PartitionedEstimation.Distribution;
import PartitionedEstimation.Histogram;
import PartitionedEstimation.Subgraph;

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
    // patternType -> partitionScheme -> hashIdComb -> vList -> labelSeq -> Long
    Map<Integer, Map<String, Map<String, Map<String, Map<String, Long>>>>> catalogues =
        new HashMap<>();

    // patternType -> partitionScheme -> hashIdComb -> distribution
    Map<Integer, Map<String, Map<String, Distribution>>> distributions;

    List<String> entries = new ArrayList<>();
    Map<String, Long> entry2card = new HashMap<>();

    void addCat(Integer patternType, String scheme, String hashIdComb,
        String vList, String labelSeq, Long card) {
        catalogues.putIfAbsent(patternType, new HashMap<>());
        catalogues.get(patternType).putIfAbsent(scheme, new HashMap<>());
        catalogues.get(patternType).get(scheme).putIfAbsent(hashIdComb, new HashMap<>());
        catalogues.get(patternType).get(scheme).get(hashIdComb).putIfAbsent(vList, new HashMap<>());
        catalogues.get(patternType).get(scheme).get(hashIdComb).get(vList).put(labelSeq, card);
    }

    void persistCat(String destFile) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(destFile));
        for (Integer patternType : catalogues.keySet()) {
            for (String scheme : catalogues.get(patternType).keySet()) {
                for (String hashIdComb : catalogues.get(patternType).get(scheme).keySet()) {
                    Map<String, Map<String, Long>> vList2labelSeq2card =
                        catalogues.get(patternType).get(scheme).get(hashIdComb);
                    for (String vList : vList2labelSeq2card.keySet()) {
                        for (String labelSeq : vList2labelSeq2card.get(vList).keySet()) {
                            StringJoiner line = new StringJoiner(",");
                            line.add(patternType.toString())
                                .add(scheme)
                                .add(hashIdComb)
                                .add(vList)
                                .add(labelSeq)
                                .add(vList2labelSeq2card.get(vList).get(labelSeq).toString());
                            writer.write(line.toString() + "\n");
                        }
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
            for (String scheme : distributions.get(patternType).keySet()) {
                for (String hashIdComb : distributions.get(patternType).get(scheme).keySet()) {
                    dist = distributions.get(patternType).get(scheme).get(hashIdComb);
                    maxDegs = dist.getMaxDegs();

                    for (String line : maxDegs) {
                        String csv = patternType + "," + scheme + "," + hashIdComb + "," + line;
                        writer.write(csv + "\n");
                    }
                }
            }
        }
        writer.close();

        endTime = System.currentTimeMillis();
        System.out.println("Computing CatMaxDeg: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    // assumes catLength >= 2
    public void construct(String graphFile, String queryVList, List<String> queryLabelSeqs,
        int budget, String catalogueFile, int catLength) throws Exception {

        PartitionedDistributionParallel partitionedDistribution = new PartitionedDistributionParallel();
        for (int len = catLength; len >= 1; --len) {
            if (len > 1) {
                partitionedDistribution.construct(
                    graphFile, queryVList, queryLabelSeqs, budget, catalogueFile, len);
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
        PartitionedDistributionParallel partitionedDistribution,
        Integer patternType, String vListEdge, String labelString) {
        Map<String, Map<String, Map<String, Subgraph>>> partitions =
            partitionedDistribution.getPartitions();

        Integer label = Integer.parseInt(labelString);

        for (String scheme : partitions.keySet()) {
            long totalCardAcrossPartitions = 0;
            for (String hashIdComb : partitions.get(scheme).get(vListEdge).keySet()) {
                Subgraph subgraph = partitions.get(scheme).get(vListEdge).get(hashIdComb);

                long cardinality = 0;
                if (subgraph.label2src2dest.containsKey(label)) {
                    for (Set<Integer> dests : subgraph.label2src2dest.get(label).values()) {
                        cardinality += dests.size();
                    }
                }
                totalCardAcrossPartitions += cardinality;

                addCat(patternType, scheme, hashIdComb, vListEdge, labelString, cardinality);
            }

            // not equal to the cardinality in the original catalogue
            if (!entry2card.get(patternType + "," + vListEdge + "," + label).equals(totalCardAcrossPartitions)) {

                System.err.println("ERROR: sanity check failed (cardinality)");
                System.err.println("   " + vListEdge + "," + label);
                System.err.println(
                    "   " + entry2card.get(patternType + "," + vListEdge + "," + label));
                System.err.println("   " + totalCardAcrossPartitions);
            }
        }
    }

    private void computeCardinality(
        Map<Integer, Map<String, Map<String, Distribution>>> distributions,
        Integer patternType, String vList, String labelSeq) {

        List<Pair<Pair<String, String>, Pair<String, String>>> baseAndExts =
            Util.splitToBaseAndExt(vList, labelSeq, Util.getLeaves(Util.toVList(vList)));

        Distribution distribution;
        String baseVList, baseLabelSeq, extVList, extLabel;
        for (String scheme : distributions.get(patternType).keySet()) {
            Set<Long> cardinalities = new HashSet<>();
            for (Pair<Pair<String, String>, Pair<String, String>> baseAndExt : baseAndExts) {
                long totalCardAcrossPartitions = 0;
                for (String hashIdComb : distributions.get(patternType).get(scheme).keySet()) {
                    distribution = distributions.get(patternType).get(scheme).get(hashIdComb);
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

                    addCat(patternType, scheme, hashIdComb, vList, labelSeq, cardinality);
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

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println("catalogueFile: " + args[1]);
        System.out.println("specifiedCatLen: " + args[2]);
        System.out.println("queryFile: " + args[3]);
        System.out.println("queryVList: " + args[4]);
        System.out.println("budget: " + args[5]);
        System.out.println("catDestFile: " + args[6]);
        System.out.println("catMaxDegDestFile: " + args[7]);
        System.out.println();

        String vList = args[4];
        int budget = Integer.parseInt(args[5]);
        int catLength = Integer.parseInt(args[2]);

        List<Query> queries = Query.readQueries(args[3]);
        List<String> labelSeqs = new ArrayList<>();
        for (Query query : queries) {
            labelSeqs.add(query.extractLabelSeq(Util.toVList(vList)));
        }

        PartitionedCatalogue partitionedCatalogue = new PartitionedCatalogue();
        partitionedCatalogue.readCatalogue(args[1], catLength);
        partitionedCatalogue.construct(
            args[0], vList, labelSeqs, budget, args[1], catLength);
        partitionedCatalogue.persistCat(args[6]);
        partitionedCatalogue.computeAndPersistCatMaxDeg(args[7]);
    }
}
