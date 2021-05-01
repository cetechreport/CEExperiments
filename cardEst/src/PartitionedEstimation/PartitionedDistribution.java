package PartitionedEstimation;

import Common.Pair;
import Common.Query;
import Common.Util;
import Graphflow.CatalogueEntropy;
import Graphflow.Constants;
import Graphflow.Parallel.CatalogueEntropyConstruction;
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
import java.util.stream.Collectors;

public class PartitionedDistribution extends CatalogueEntropy {
    // patternType -> hashIdComb -> distribution
    public Map<Integer, Map<String, Distribution>> distributions = new HashMap<>();

    // vList -> hashIds
    private Map<String, Set<String>> partitionCombinations = new HashMap<>();
    private Map<String, Map<String, Subgraph>> partitions;

    // patternType -> vList -> (labelSeq, card)
    private Map<String, Map<String, List<Pair<String, Long>>>> catalogue = new HashMap<>();

    private List<Map<Integer, List<Pair<Integer, Integer>>>> listOfLabel2srcdest;
    private List<Map<Integer, Map<Integer, List<Integer>>>> listOfSrc2label2dest;
    private List<Map<Integer, Map<Integer, List<Integer>>>> listOfDest2label2src;

    public Map<String, Map<String, Subgraph>> getPartitions() {
        return partitions;
    }

    private void addDistByHash(
        Integer patternType, String hashIdComb, Distribution distribution) {

        distributions.putIfAbsent(patternType, new HashMap<>());
        distributions.get(patternType).putIfAbsent(hashIdComb, new Distribution());
        Distribution updatedDist = distributions.get(patternType).get(hashIdComb);
        updatedDist.add(distribution);
        distributions.get(patternType).put(hashIdComb, updatedDist);
    }

    private void formSubgraph(String vList, String hashIdComb, List<Pair<String, Long>> entries)
        throws Exception {
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
            runnable = new SubgraphFormer(i, entry, vListEdges, edgeHashIds, partitions);
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

    private void computeDist(List<Pair<String, Long>> entries, String hashIdComb) throws Exception {
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

        collect(runnables, hashIdComb);
    }

    public void construct(String graphFile, String queryVList, List<String> queryLabelSeqs,
        int partType, int budget, String catalogueFile, int catLength) throws Exception {

        long startTime = System.currentTimeMillis();
        long endTime;

        // prepare the construction
        Partitioner partitioner;
        if (partType == Constants.HASH) {
            partitioner = new HashPartitioner(graphFile, queryVList, budget);
        } else if (partType == Constants.DEG) {
            partitioner = new DegPartitioner(graphFile, queryVList, budget);
        } else {
            System.err.println("ERROR: unrecognized partitioning type");
            return;
        }
        this.partitions = partitioner.partition(queryVList, queryLabelSeqs);
        prepare(catalogueFile, catLength, partType);

        // print # of partitions
        for (String vListEdge : partitions.keySet()) {
            System.out.println(vListEdge + ": " + partitions.get(vListEdge).keySet().size());
        }

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
                Set<String> catLabelSeqs = Util.extractLabelSeqs(vList, queryVList, queryLabelSeqs);
                List<Pair<String, Long>> entries = catalogue.get(patternType).get(vList);
                entries = entries.stream()
                    .filter(p -> catLabelSeqs.contains(p.key))
                    .peek(p -> p.key = patternType + "," + vList + "," + p.key)
                    .collect(Collectors.toList());

                for (String hashIdComb : partitionCombinations.get(vList)) {
                    formSubgraph(vList, hashIdComb, entries);
                    computeDist(entries, hashIdComb);

                    progress += 100.0 / totalProgress;
                    System.out.print("\rComputing Distribution: " + (int) progress + "%");
                }
            }
        }

        endTime = System.currentTimeMillis();
        System.out.println("\rComputing Distribution: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public void persist(String destFile) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(destFile));
        for (Integer patternType : distributions.keySet()) {
            for (String hashIdComb : distributions.get(patternType).keySet()) {
                Distribution dist = distributions.get(patternType).get(hashIdComb);
                List<String> lines = dist.toStrings();
                for (String line : lines) {
                    writer.write(patternType + "," + hashIdComb + "," + line + "\n");
                }
            }
        }
        writer.close();
    }

    private void collect(List<CatalogueEntropyConstruction> constructors, String hashIdComb) {
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

        addDistByHash(patternType, hashIdComb, distribution);
    }

    private void prepare(String catalogueFile, int length, int partType) throws Exception {
        readCatalogue(catalogueFile, length);
        for (String patternType : catalogue.keySet()) {
            for (String vList : catalogue.get(patternType).keySet()) {
                partitionCombinations.putIfAbsent(vList, new HashSet<>());

                // assumes length = 2
                Set<String> hashIds1 = partitions.get(vList.split(";")[0]).keySet();
                Set<String> hashIds2 = partitions.get(vList.split(";")[1]).keySet();

                for (String hashId1 : hashIds1) {
                    for (String hashId2 : hashIds2) {
                        String hashIdComb = hashId1 + ";" + hashId2;
                        if (partType == Constants.HASH) {
                            if (!Util.canConnect(vList, hashIdComb)) continue;
                        }

                        partitionCombinations.get(vList).add(hashIdComb);
                    }
                }
            }
        }
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

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println("catalogueFile: " + args[1]);
        System.out.println("specifiedCatLen: " + args[2]);
        System.out.println("queryFile: " + args[3]);
        System.out.println("queryVList: " + args[4]);
        System.out.println("partType: " + args[5]);
        System.out.println("budget: " + args[6]);
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

        PartitionedDistribution partitionedDistribution = new PartitionedDistribution();
        partitionedDistribution.construct(
            args[0], vList, labelSeqs, partType, budget, args[1], catLength);
    }
}
