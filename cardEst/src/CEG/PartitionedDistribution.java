package CEG;

import Common.Pair;
import Common.Util;
import Graphflow.CatalogueEntropy;
import Graphflow.Parallel.CatalogueEntropyConstruction;
import PartitionedEstimation.Distribution;
import PartitionedEstimation.HashPartitioner;
import PartitionedEstimation.Histogram;
import PartitionedEstimation.Partitioner;
import PartitionedEstimation.Subgraph;
import PartitionedEstimation.SubgraphFormer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PartitionedDistribution extends CatalogueEntropy {
    // patternType -> partitionScheme -> hashIdComb -> distribution
    public Map<Integer, Map<String, Map<String, Distribution>>> distributions = new HashMap<>();

    // partitionScheme -> vList -> hashIdComb -> subgraph
    private Map<String, Map<String, Map<String, Subgraph>>> partitions = new HashMap<>();

    // patternType -> vList -> (labelSeq, card)
    private Map<String, Map<String, List<Pair<String, Long>>>> catalogue = new HashMap<>();

    // vList -> hashIds
    Map<String, Set<String>> partitionCombinations;

    private List<Map<Integer, List<Pair<Integer, Integer>>>> listOfLabel2srcdest;
    private List<Map<Integer, Map<Integer, List<Integer>>>> listOfSrc2label2dest;
    private List<Map<Integer, Map<Integer, List<Integer>>>> listOfDest2label2src;

    public Map<String, Map<String, Map<String, Subgraph>>> getPartitions() {
        return partitions;
    }

    public void construct(String graphFile, String queryVList, List<String> queryLabelSeqs,
        int budget, String catalogueFile, int catLength) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        CEG ceg = new CEG(queryVList, 2);
        List<Path> allPaths = ceg.getAllPaths();
        readCatalogue(catalogueFile, catLength);

        // get all the schemes
        Set<Map<Integer, Integer>> setOfSchemes = allPaths.stream()
            .map(path -> path.getPartitionScheme(budget))
            .collect(Collectors.toSet());
        List<Map<Integer, Integer>> schemes = new ArrayList<>(setOfSchemes);
        System.out.println(schemes.size() + ": " + schemes);

        Partitioner partitioner = new HashPartitioner(graphFile, queryVList, budget);
        for (int s = 0; s < schemes.size(); ++s) {
            // prepare the construction
            Map<Integer, Integer> scheme = schemes.get(s);
            String schemeString = Util.partitionSchemeToString(queryVList, scheme);
            partitions.put(schemeString, partitioner.partition(queryVList, queryLabelSeqs, scheme));
            preparePartitionComb(schemeString);

            // get total amount of work (for progress tracking)
            long totalProgress = 0;
            for (String patternType : catalogue.keySet()) {
                for (String vList : catalogue.get(patternType).keySet()) {
                    totalProgress += partitionCombinations.get(vList).size();
                }
            }

            // start constructing
            double progress = 0;
            for (String patternType : catalogue.keySet()) {
                for (String vList : catalogue.get(patternType).keySet()) {
                    Set<String> qLabelSeqs = Util.extractLabelSeqs(vList, queryVList, queryLabelSeqs);

                    List<Pair<String, Long>> entries =
                        extractLabelSeqEntries(patternType, vList, qLabelSeqs);

                    for (String hashIdComb : partitionCombinations.get(vList)) {
                        formSubgraph(schemeString, vList, hashIdComb, entries);
                        computeDist(schemeString, entries, hashIdComb);

                        progress += 100.0 / totalProgress;
                        System.out.print("\rComputing Distribution (" + s +  "): " +
                            (int) progress + "%");
                    }
                }
            }
        }

        endTime = System.currentTimeMillis();
        System.out.println("\rComputing Distribution: " +
            ((endTime - startTime) / 1000.0) + " sec");
    }

    private List<Pair<String, Long>> extractLabelSeqEntries(
        String patternType, String vList, Set<String> qLabelSeqs) {
        List<Pair<String, Long>> entries = new ArrayList<>();
        for (Pair<String, Long> labelSeqAndCard : catalogue.get(patternType).get(vList)) {
            if (qLabelSeqs.contains(labelSeqAndCard.key)) {
                entries.add(new Pair<>(
                    patternType + "," + vList + "," + labelSeqAndCard.key,
                    labelSeqAndCard.value)
                );
            }
        }
        return entries;
    }

    private void preparePartitionComb(String scheme) throws Exception {
        partitionCombinations = new HashMap<>();
        for (String patternType : catalogue.keySet()) {
            for (String vList : catalogue.get(patternType).keySet()) {
                partitionCombinations.putIfAbsent(vList, new HashSet<>());

                // assumes length = 2
                Set<String> hashIds1 = partitions.get(scheme).get(vList.split(";")[0]).keySet();
                Set<String> hashIds2 = partitions.get(scheme).get(vList.split(";")[1]).keySet();

                for (String hashId1 : hashIds1) {
                    for (String hashId2 : hashIds2) {
                        String hashIdComb = hashId1 + ";" + hashId2;
                        if (!Util.canConnect(vList, hashIdComb)) continue;

                        partitionCombinations.get(vList).add(hashIdComb);
                    }
                }
            }
        }
    }

    private void formSubgraph(String scheme, String vList, String hashIdComb,
        List<Pair<String, Long>> entries) throws Exception {
        listOfLabel2srcdest = new ArrayList<>();
        listOfSrc2label2dest = new ArrayList<>();
        listOfDest2label2src = new ArrayList<>();

        List<Thread> threads = new ArrayList<>();
        List<SubgraphFormer> runnables = new ArrayList<>();
        SubgraphFormer runnable;
        Thread thread;

        String[] vListEdges = vList.split(";");
        String[] edgeHashIds = hashIdComb.split(";");
        Pair<String, Long> entry;
        for (int i = 0; i < entries.size(); ++i) {
            entry = entries.get(i);
            runnable = new SubgraphFormer(i, entry, vListEdges, edgeHashIds, partitions.get(scheme));
            runnables.add(runnable);
            thread = new Thread(runnable);
            threads.add(thread);
            thread.start();
        }

        for (int i = 0; i < threads.size(); ++i) {
            threads.get(i).join();
            listOfLabel2srcdest.add(runnables.get(i).getLabel2srcdest());
            listOfSrc2label2dest.add(runnables.get(i).getSrc2label2dest());
            listOfDest2label2src.add(runnables.get(i).getDest2label2src());
        }
    }

    private void computeDist(
        String scheme, List<Pair<String, Long>> entries, String hashIdComb) throws Exception {
        List<Thread> threads = new ArrayList<>();
        List<CatalogueEntropyConstruction> runnables = new ArrayList<>();
        CatalogueEntropyConstruction runnable;
        Thread thread;
        int threadId = 0;

        for (int i = 0; i < entries.size(); ++i) {
            threadId++;

            runnable = new CatalogueEntropyConstruction(
                threadId,
                listOfLabel2srcdest.get(i),
                listOfSrc2label2dest.get(i),
                listOfDest2label2src.get(i),
                entries.subList(i, i + 1)
            );
            runnables.add(runnable);

            thread = new Thread(runnable);
            threads.add(thread);
            thread.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        collect(runnables, scheme, hashIdComb);
    }

    private void collect(List<CatalogueEntropyConstruction> constructors,
        String scheme, String hashIdComb) {
        Distribution distribution = new Distribution();
        Integer patternType = -1;

        for (CatalogueEntropyConstruction constructor : constructors) {
            Map<Integer, Map<String, Map<String, Map<String, Map<String, Map<Integer, Long>>>>>>
                dist = constructor.getDistribution();

            for (Integer pattern : dist.keySet()) {
                patternType = pattern;
                for (String baseVList : dist.get(patternType).keySet()) {
                    for (String baseLabelSeq : dist.get(patternType).get(baseVList).keySet()) {
                        Map<String, Map<String, Map<Integer, Long>>> ext =
                            dist.get(patternType).get(baseVList).get(baseLabelSeq);
                        for (String extVList : ext.keySet()) {
                            for (String extLabel : ext.get(extVList).keySet()) {
                                Histogram histogram = new Histogram();
                                for (Integer extSize : ext.get(extVList).get(extLabel).keySet()) {
                                    Long numInst = ext.get(extVList).get(extLabel).get(extSize);
                                    histogram.add(extSize, numInst);
                                }

                                distribution.add(
                                    baseVList, baseLabelSeq, extVList, extLabel, histogram);
                            }
                        }
                    }
                }
            }
        }

        if (patternType > 0 && !distribution.isEmpty()) {
            addDistByHash(patternType, scheme, hashIdComb, distribution);
        }
    }

    private void addDistByHash(
        Integer patternType, String scheme, String hashIdComb, Distribution distribution) {

        distributions.putIfAbsent(patternType, new HashMap<>());
        distributions.get(patternType).putIfAbsent(scheme, new HashMap<>());
        distributions.get(patternType).get(scheme).putIfAbsent(hashIdComb, new Distribution());
        Distribution updatedDist = distributions.get(patternType).get(scheme).get(hashIdComb);
        updatedDist.add(distribution);
        distributions.get(patternType).get(scheme).put(hashIdComb, updatedDist);
    }

    protected void readCatalogue(String catalogueFile, int length) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader reader = new BufferedReader(new FileReader(catalogueFile));

        String patternType, vList, labelSeq;
        Long cardinality;

        String line = reader.readLine();
        String[] info;
        while (line != null) {
            info = line.split(",");
            patternType = info[0];
            vList = info[1];
            labelSeq = info[2];
            cardinality = Long.parseLong(info[3]);

            if (labelSeq.split("->").length == length) {
                catalogue.putIfAbsent(patternType, new HashMap<>());
                catalogue.get(patternType).putIfAbsent(vList, new ArrayList<>());
                catalogue.get(patternType).get(vList).add(new Pair<>(labelSeq, cardinality));
            }

            line = reader.readLine();
        }
        reader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Loading Catalogue: " + ((endTime - startTime) / 1000.0) + " sec");
    }
}
