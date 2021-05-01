package CEG.Partitioning;

import CEG.CEG;
import CEG.Path;
import Common.Pair;
import Common.Triple;
import Common.Util;
import org.apache.commons.lang3.time.StopWatch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class HashFilterCatalogue {
    Map<Integer, List<Pair<Integer, Integer>>> label2srcdest = new HashMap<>();
    Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();

    // (vList, labeSeq, card)
    List<Triple<String, String, Long>> catEntries = new ArrayList<>();

    String patternType;
    // partitionScheme -> vList -> hashIdComb -> labelSeq -> count
    Map<String, Map<String, Map<String, Map<String, Long>>>> catalogue = new HashMap<>();

    // partitionScheme -> hashIdComb -> baseVList -> baseLabelSeq -> extVList -> extLabel -> maxDeg
    Map<String, Map<String, Map<String, Map<String, Map<String, Map<String, Integer>>>>>> catalogueMaxDegs =
        new HashMap<>();

    public void construct(String graphFile, String queryVList,
        int budget, String catalogueFile, int catLength) throws Exception {
        StopWatch watch = new StopWatch();
        watch.start();

        // load data
        readGraph(graphFile);
        readCatalogue(catalogueFile, catLength);

        // get all the schemes
        CEG ceg = new CEG(queryVList, 2);
        List<Path> allPaths = ceg.getAllPaths();
        Set<Map<Integer, Integer>> setOfSchemes = allPaths.stream()
            .map(path -> path.getPartitionScheme(budget))
            .collect(Collectors.toSet());
        List<Map<Integer, Integer>> schemes = new ArrayList<>(setOfSchemes);
        System.out.println(schemes.size() + ": " + schemes);

        List<Thread> threads = new ArrayList<>();
        List<HashFilterDistributionRunnable> runnables = new ArrayList<>();
        HashFilterDistributionRunnable runnable;
        Thread thread;

        for (Map<Integer, Integer> scheme : schemes) {
            String queryScheme = Util.partitionSchemeToString(queryVList, scheme);
            for (Triple<String, String, Long> entry : catEntries) {
                String entryBudgetScheme = Util.extractScheme(entry.v1, queryVList, queryScheme);

                HashFilterDistribution hashFilterDistribution =
                    new HashFilterDistribution(label2srcdest, src2label2dest, dest2label2src);
                runnable =
                    new HashFilterDistributionRunnable(hashFilterDistribution, queryScheme, entryBudgetScheme, entry);
                runnables.add(runnable);

                thread = new Thread(runnable);
                threads.add(thread);
                thread.start();
            }
        }

        int total = schemes.size() * catEntries.size();
        double progress = 0.0;
        for (Thread t : threads) {
            t.join();
            progress += 100.0 / total;
            System.out.print("\rConstructing: " + (int) progress + "%");
        }

        for (HashFilterDistributionRunnable distRunnable : runnables) {
            HashFilterDistribution hashFilterDistribution = distRunnable.getDistribution();
            String queryScheme = distRunnable.getQueryScheme();
            Triple<String, String, Long> entry = distRunnable.getEntry();
            if (entry.v1.split(";").length > 1) {
                addCat2(entry, queryScheme, hashFilterDistribution);
                addCatMaxDeg(entry, queryScheme, hashFilterDistribution);
            } else {
                addCat1(queryScheme, hashFilterDistribution);
            }
        }

        watch.stop();
        System.out.println("\rConstructing: " + (watch.getTime() / 1000.0) + " sec");
    }

    private void addCat2(
        Triple<String, String, Long> entry, String scheme, HashFilterDistribution distribution) {
        // hashIdComb -> vList -> labelSeq -> count
        Map<String, Map<String, Map<String, Long>>> partCat = distribution.catalogue;

        for (String hashIdComb : partCat.keySet()) {
            Long cardinality = partCat.get(hashIdComb).get(entry.v1).get(entry.v2);
            addCatEntry(scheme, entry.v1, hashIdComb, entry.v2, cardinality);
        }
    }

    private void addCat1(String scheme, HashFilterDistribution distribution) {
        Map<String, Map<String, Map<String, Long>>> counts = distribution.singleEdge;

        for (String hashId : counts.keySet()) {
            for (String vList : counts.get(hashId).keySet()) {
                for (String label : counts.get(hashId).get(vList).keySet()) {
                    addCatEntry(scheme, vList, hashId, label, counts.get(hashId).get(vList).get(label));
                }
            }
        }
    }

    private void addCatEntry(String scheme, String vList, String hashIdComb, String labelSeq, Long cardinality) {
        catalogue.putIfAbsent(scheme, new HashMap<>());
        catalogue.get(scheme).putIfAbsent(vList, new HashMap<>());
        catalogue.get(scheme).get(vList).putIfAbsent(hashIdComb, new HashMap<>());
        catalogue.get(scheme).get(vList).get(hashIdComb).put(labelSeq, cardinality);
    }

    private void addCatMaxDeg(
        Triple<String, String, Long> entry, String scheme, HashFilterDistribution distribution) {
        // hashIdComb -> baseVList -> baseLabelSeq -> extVList -> extLabel -> maxdeg
        Map<String, Map<String, Map<String, Map<String, Map<String, Integer>>>>> partMaxDeg = distribution.maxDeg;

        catalogueMaxDegs.putIfAbsent(scheme, new HashMap<>());
        for (String hashIdComb : partMaxDeg.keySet()) {
            Map<String, Map<String, Map<String, Map<String, Integer>>>> maxdegOfHash = partMaxDeg.get(hashIdComb);
            catalogueMaxDegs.get(scheme).putIfAbsent(hashIdComb, new HashMap<>());
            for (String baseVList : maxdegOfHash.keySet()) {
                catalogueMaxDegs.get(scheme).get(hashIdComb).putIfAbsent(baseVList, new HashMap<>());
                for (String baseLabel : maxdegOfHash.get(baseVList).keySet()) {
                    catalogueMaxDegs.get(scheme).get(hashIdComb).get(baseVList).putIfAbsent(baseLabel, new HashMap<>());
                    for (String extVList : maxdegOfHash.get(baseVList).get(baseLabel).keySet()) {
                        catalogueMaxDegs.get(scheme).get(hashIdComb).get(baseVList).get(baseLabel)
                            .putIfAbsent(extVList, new HashMap<>());
                        for (String extLabel : maxdegOfHash.get(baseVList).get(baseLabel).get(extVList).keySet()) {
                            Integer maxDeg = maxdegOfHash.get(baseVList).get(baseLabel).get(extVList).get(extLabel);
                            Integer currentMax = catalogueMaxDegs.get(scheme).get(hashIdComb).get(baseVList)
                                .get(baseLabel).get(extVList).getOrDefault(extLabel, 0);
                            catalogueMaxDegs.get(scheme).get(hashIdComb).get(baseVList)
                                .get(baseLabel).get(extVList).put(extLabel, Math.max(currentMax, maxDeg));
                        }
                    }
                }
            }
        }
    }

    public Map<String, Set<String>> getHashIds(String vListString, Map<Integer, Integer> scheme) {
        // vListEdge -> set of (hashID of src and dest)
        Map<String, Set<String>> hashIds = new HashMap<>();

        Integer[] vList = Util.toVList(vListString);
        for (int i = 0; i < vList.length; i += 2) {
            Integer src = vList[i];
            Integer dest = vList[i + 1];
            String vListEdge = src + "-" + dest;
            if (!hashIds.containsKey(vListEdge)) {
                hashIds.put(vListEdge, new HashSet<>());
            }

            Integer srcBudget = scheme.get(src);
            Integer destBudget = scheme.get(dest);

            for (int srcHash = 0; srcHash < srcBudget; ++srcHash) {
                for (int destHash = 0; destHash < destBudget; ++destHash) {
                    String hashId = srcHash + "-" + destHash;
                    hashIds.get(vListEdge).add(hashId);
                }
            }
        }

        return hashIds;
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
                this.patternType = patternType;
                catEntries.add(new Triple<>(vList, labelSeq, card));
            }

            line = reader.readLine();
        }
        reader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Loading Catalogue: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public void persistCat(String destFile) throws Exception {
        StopWatch watch = new StopWatch();
        watch.start();

        BufferedWriter writer = new BufferedWriter(new FileWriter(destFile));
        for (String scheme : catalogue.keySet()) {
            for (String vList : catalogue.get(scheme).keySet()) {
                Map<String, Map<String, Long>> hashIdComb2labelSeq2card = catalogue.get(scheme).get(vList);
                for (String hashIdComb : hashIdComb2labelSeq2card.keySet()) {
                    for (String labelSeq : hashIdComb2labelSeq2card.get(hashIdComb).keySet()) {
                        StringJoiner line = new StringJoiner(",");
                        line.add(patternType)
                            .add(scheme)
                            .add(hashIdComb)
                            .add(vList)
                            .add(labelSeq)
                            .add(hashIdComb2labelSeq2card.get(hashIdComb).get(labelSeq).toString());
                        writer.write(line.toString() + "\n");
                    }
                }
            }
        }
        writer.close();

        watch.stop();
        System.out.println("Saving Cat: " + (watch.getTime() / 1000.0) + " sec");
    }

    public void persistCatMaxDeg(String destFile) throws Exception {
        StopWatch watch = new StopWatch();
        watch.start();

        BufferedWriter writer = new BufferedWriter(new FileWriter(destFile));
        for (String scheme : catalogueMaxDegs.keySet()) {
            for (String hashIdComb : catalogueMaxDegs.get(scheme).keySet()) {
                Map<String, Map<String, Map<String, Map<String, Integer>>>> maxDegs =
                    catalogueMaxDegs.get(scheme).get(hashIdComb);
                for (String baseVList : maxDegs.keySet()) {
                    for (String baseLabel : maxDegs.get(baseVList).keySet()) {
                        for (String extVList : maxDegs.get(baseVList).get(baseLabel).keySet()) {
                            for (String extLabel : maxDegs.get(baseVList).get(baseLabel).get(extVList).keySet()) {
                                Integer maxDeg = maxDegs.get(baseVList).get(baseLabel).get(extVList).get(extLabel);
                                StringJoiner joiner = new StringJoiner(",");
                                joiner.add(patternType).add(scheme).add(hashIdComb);
                                joiner.add(baseVList).add(baseLabel).add(extVList).add(extLabel).add(maxDeg.toString());
                                writer.write(joiner.toString() + "\n");
                            }
                        }
                    }
                }
            }
        }
        writer.close();

        watch.stop();
        System.out.println("Saving CatMaxDeg: " + (watch.getTime() / 1000.0) + " sec");
    }

    private void sanityCheck() {
        for (String scheme : catalogue.keySet()) {
            for (Triple<String, String, Long> entry : catEntries) {
                String vList = entry.v1;
                String labelSeq = entry.v2;
                Long trueCard = entry.v3;

                Long totalFromPartitions = 0L;
                for (String hashIdComb : catalogue.get(scheme).get(vList).keySet()) {
                    if (catalogue.get(scheme).get(vList).get(hashIdComb).containsKey(labelSeq)) {
                        totalFromPartitions += catalogue.get(scheme).get(vList).get(hashIdComb).get(labelSeq);
                    }
                }

                if (!trueCard.equals(totalFromPartitions)) {
                    System.err.println("ERROR: sanity check failed (unmatched cardinality)");
                    System.err.println("   partitionedTotal: " + totalFromPartitions);
                    System.err.println("   actual: " + trueCard);
                }
            }
        }
    }

    private void readGraph(String graphFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader csvReader = new BufferedReader(new FileReader(graphFile));
        int[] line;
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            line = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt).toArray();

            label2srcdest.putIfAbsent(line[1], new ArrayList<>());
            label2srcdest.get(line[1]).add(new Pair<>(line[0], line[2]));

            src2label2dest.putIfAbsent(line[0], new HashMap<>());
            src2label2dest.get(line[0]).putIfAbsent(line[1], new ArrayList<>());
            src2label2dest.get(line[0]).get(line[1]).add(line[2]);

            dest2label2src.putIfAbsent(line[2], new HashMap<>());
            dest2label2src.get(line[2]).putIfAbsent(line[1], new ArrayList<>());
            dest2label2src.get(line[2]).get(line[1]).add(line[0]);

            tripleString = csvReader.readLine();
        }

        endTime = System.currentTimeMillis();
        System.out.println("Loading Graph: " + ((endTime - startTime) / 1000.0) + " sec");

        csvReader.close();
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

        HashFilterCatalogue hashFilterCatalogue = new HashFilterCatalogue();
        hashFilterCatalogue.construct(args[0], vList, budget, args[1], catLength);
        hashFilterCatalogue.sanityCheck();
        hashFilterCatalogue.persistCat(args[6]);
        hashFilterCatalogue.persistCatMaxDeg(args[7]);
    }
}
