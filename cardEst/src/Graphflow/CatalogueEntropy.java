package Graphflow;

import Common.Pair;
import Common.Util;
import Graphflow.Parallel.CatalogueEntropyConstruction;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class CatalogueEntropy {
    protected Map<Integer, List<Pair<Integer, Integer>>> label2srcdest = new HashMap<>();
    protected Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    protected Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();

    protected List<Pair<String, Long>> selectCatalogue = new ArrayList<>();
    protected List<String> catalogueOthers = new ArrayList<>();

    // patternType -> baseVList -> baseLabelSeq -> extVList -> extLabel -> cv/entropy
    protected Map<Integer, Map<String, Map<String, Map<String, Map<String, Double>>>>>
        uniformityMeasure = new HashMap<>();

    // patternType -> baseVList -> baseLabelSeq -> extVList -> extLabel -> extSize -> #instances
    protected Map<Integer, Map<String, Map<String, Map<String, Map<String, Map<Integer, Long>>>>>>
        distribution = new HashMap<>();

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
                selectCatalogue.add(new Pair<>(patternType + "," + vList + "," + labelSeq, cardinality));
            } else {
                catalogueOthers.add(line);
            }

            line = reader.readLine();
        }
        reader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Loading Catalogue: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    protected void computeDist() throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        List<Thread> threads = new ArrayList<>();
        List<CatalogueEntropyConstruction> runnables = new ArrayList<>();
        CatalogueEntropyConstruction runnable;
        Thread thread;
        int threadId = 0;

        int NUM_ENTRY_WORKERS = 2000;

        if (selectCatalogue.size() < NUM_ENTRY_WORKERS) {
            NUM_ENTRY_WORKERS = selectCatalogue.size();
        }

        for (int i = 0; i < selectCatalogue.size(); i += selectCatalogue.size() / NUM_ENTRY_WORKERS) {
            threadId++;

            runnable = new CatalogueEntropyConstruction(
                threadId,
                label2srcdest,
                src2label2dest,
                dest2label2src,
                selectCatalogue.subList(
                    i, Math.min(selectCatalogue.size(), i + selectCatalogue.size() / NUM_ENTRY_WORKERS))
            );
            runnables.add(runnable);

            thread = new Thread(runnable);
            threads.add(thread);
            thread.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        collect(runnables);

        endTime = System.currentTimeMillis();
        System.out.println("Computing Dist: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void computeUniformityMeasure(String measure) {
        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer patternType : distribution.keySet()) {
            for (String baseVList : distribution.get(patternType).keySet()) {
                for (String baseLabelSeq : distribution.get(patternType).get(baseVList).keySet()) {
                    Map<String, Map<String, Map<Integer, Long>>> extDist =
                        distribution.get(patternType).get(baseVList).get(baseLabelSeq);

                    for (String extVList : extDist.keySet()) {
                        for (String extLabel : extDist.get(extVList).keySet()) {
                            double uniformity;
                            if (measure.equals("entropy")) {
                                uniformity = computeEntropy(extDist.get(extVList).get(extLabel));
                            } else if (measure.equals("cv")) {
                                uniformity = computeCV(extDist.get(extVList).get(extLabel));
                            } else {
                                System.out.println("ERROR: unsupported uniformity measure");
                                return;
                            }

                            addUniformityMeasure(
                                patternType, baseVList, baseLabelSeq, extVList, extLabel, uniformity);
                        }
                    }
                }
            }
        }

        endTime = System.currentTimeMillis();
        System.out.println("Computing Entropy: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void addUniformityMeasure(
        Integer patternType, String baseVList, String baseLabelSeq,
        String extVList, String extLabel, double entropy) {

        uniformityMeasure.putIfAbsent(patternType, new HashMap<>());
        uniformityMeasure.get(patternType).putIfAbsent(baseVList, new HashMap<>());
        uniformityMeasure.get(patternType).get(baseVList).putIfAbsent(baseLabelSeq, new HashMap<>());
        uniformityMeasure.get(patternType).get(baseVList).get(baseLabelSeq)
            .putIfAbsent(extVList, new HashMap<>());
        uniformityMeasure.get(patternType).get(baseVList).get(baseLabelSeq).get(extVList)
            .put(extLabel, entropy);
    }

    private void persistUniformityMeasure(String destFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedWriter writer = new BufferedWriter(new FileWriter(destFile));
        for (Integer patternType : uniformityMeasure.keySet()) {
            for (String baseVList : uniformityMeasure.get(patternType).keySet()) {
                for (String baseLabelSeq : uniformityMeasure.get(patternType).get(baseVList).keySet()) {
                    Map<String, Map<String, Double>> exts =
                        uniformityMeasure.get(patternType).get(baseVList).get(baseLabelSeq);

                    for (String extVList : exts.keySet()) {
                        for (String extLabel : exts.get(extVList).keySet()) {
                            Double uniformity = exts.get(extVList).get(extLabel);
                            StringJoiner sj = new StringJoiner(",");
                            sj.add(patternType.toString());
                            sj.add(baseVList);
                            sj.add(baseLabelSeq);
                            sj.add(extVList);
                            sj.add(extLabel);
                            sj.add(uniformity.toString());
                            writer.write(sj.toString() + "\n");
                        }
                    }
                }
            }
        }
        writer.close();

        endTime = System.currentTimeMillis();
        System.out.println("Saving Uniformity: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void collect(List<CatalogueEntropyConstruction> constructors) {
        for (CatalogueEntropyConstruction constructor : constructors) {
            Map<Integer, Map<String, Map<String, Map<String, Map<String, Map<Integer, Long>>>>>>
                dist = constructor.getDistribution();

            for (Integer pattern : dist.keySet()) {
                for (String baseVList : dist.get(pattern).keySet()) {
                    for (String baseLabelSeq : dist.get(pattern).get(baseVList).keySet()) {
                        Map<String, Map<String, Map<Integer, Long>>> ext =
                            dist.get(pattern).get(baseVList).get(baseLabelSeq);
                        for (String extVList : ext.keySet()) {
                            for (String extLabel : ext.get(extVList).keySet()) {
                                for (Integer extSize : ext.get(extVList).get(extLabel).keySet()) {
                                    Long numInst = ext.get(extVList).get(extLabel).get(extSize);
                                    addToDistribution(
                                        pattern, baseVList, baseLabelSeq, extVList, extLabel, extSize, numInst);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    protected void readGraph(String graphFile) throws Exception {
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

    private void addToDistribution(
        Integer patternType, String baseVList, String baseLabelSeq, String extVList, String extLabel,
        int extSize, long contribution) {

        distribution.putIfAbsent(patternType, new HashMap<>());
        distribution.get(patternType).putIfAbsent(baseVList, new HashMap<>());
        distribution.get(patternType).get(baseVList).putIfAbsent(baseLabelSeq, new HashMap<>());
        distribution.get(patternType).get(baseVList).get(baseLabelSeq)
            .putIfAbsent(extVList, new HashMap<>());
        distribution.get(patternType).get(baseVList).get(baseLabelSeq).get(extVList)
            .putIfAbsent(extLabel, new HashMap<>());
        long count = distribution.get(patternType).get(baseVList).get(baseLabelSeq)
            .get(extVList).get(extLabel).getOrDefault(extSize, 0L);

        distribution.get(patternType).get(baseVList).get(baseLabelSeq).get(extVList).get(extLabel)
            .put(extSize, count + contribution);
    }

    private void persistDist() throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter("dist.csv"));
        for (Integer patternType : distribution.keySet()) {
            for (String baseVList : distribution.get(patternType).keySet()) {
                for (String baseLabelSeq : distribution.get(patternType).get(baseVList).keySet()) {
                    Map<String, Map<String, Map<Integer, Long>>> extDist =
                        distribution.get(patternType).get(baseVList).get(baseLabelSeq);

                    for (String extVList : extDist.keySet()) {
                        for (String extLabel : extDist.get(extVList).keySet()) {
                            StringJoiner sj = new StringJoiner(",");
                            sj.add(patternType.toString());
                            sj.add(baseVList);
                            sj.add(baseLabelSeq);
                            sj.add(extVList);
                            sj.add(extLabel);

                            for (Integer extSize : extDist.get(extVList).get(extLabel).keySet()) {
                                sj.add(extSize.toString());
                                sj.add(extDist.get(extVList).get(extLabel).get(extSize).toString());
                            }

                            writer.write(sj.toString() + "\n");
                        }
                    }
                }
            }
        }
        writer.close();
    }

    protected List<String> sanityCheck() {
        long startTime = System.currentTimeMillis();
        long endTime;

        List<String> updatedCatalogue = new ArrayList<>();

        for (Pair<String, Long> entry : selectCatalogue) {
            String[] info = entry.key.split(",");
            Integer patternType = Integer.parseInt(info[0]);
            List<Pair<Pair<String, String>, Pair<String, String>>> baseAndExts =
                Util.splitToBaseAndExt(info[1], info[2], Util.getLeaves(Util.toVList(info[1])));

            Map<Long, List<Pair<String, String>>> cardinalities = new HashMap<>();
            for (Pair<Pair<String, String>, Pair<String, String>> baseAndExt : baseAndExts) {
                Map<Integer, Long> histogram =
                    distribution.get(patternType).get(baseAndExt.key.key).get(baseAndExt.value.key)
                        .get(baseAndExt.key.value).get(baseAndExt.value.value);

                long cardinality = 0;
                for (Integer extSize : histogram.keySet()) {
                    cardinality += histogram.get(extSize) * extSize;
                }

                cardinalities.putIfAbsent(cardinality, new ArrayList<>());
                cardinalities.get(cardinality).add(baseAndExt.key);

//                if (!entry.value.equals(cardinality)) {
//                    System.err.println("ERROR: sanity check failed");
//                    System.err.println("   " + entry.key);
//                    System.err.println("   " + baseAndExt.key.key + " :: " + baseAndExt.key.value);
//                    System.err.println("   " + cardinality + " vs. " + entry.value);
//                    return;
//                }
            }

            if (cardinalities.size() > 1) {
                System.err.println("ERROR: sanity check failed");
                System.err.println("   " + entry.key);
                System.err.println("   " + cardinalities);
                return null;
            }

            for (Long cardinality : cardinalities.keySet()) {
                updatedCatalogue.add(entry.key + "," + cardinality);
                break;
            }
        }

        endTime = System.currentTimeMillis();
        System.out.println("Sanity Pass: " + ((endTime - startTime) / 1000.0) + " sec");

        return updatedCatalogue;
    }

    private double computeCV(Map<Integer, Long> histogram) {
        double total = 0.0;
        long numDataPoints = 0;
        for (Integer bucket : histogram.keySet()) {
            total += bucket * histogram.get(bucket);
            numDataPoints += histogram.get(bucket);
        }

        double mean = total / numDataPoints;
        double sd = computeSD(histogram, mean, numDataPoints);
        return sd / mean;
    }

    private double computeSD(Map<Integer, Long> histogram, double mean, long numTotalBases) {
        double sd = 0;

        for (int entry: histogram.keySet()) {
            sd += (Math.pow(entry - mean, 2) / numTotalBases) * histogram.get(entry);
        }

        return Math.sqrt(sd);
    }

    private double computeEntropy(Map<Integer, Long> histogram) {
        double entropy = 0.0;
        double total = 0.0;
        for (Long contribution : histogram.values()) {
            total += contribution;
        }

        int numBins = 0;
        for (Long contribution : histogram.values()) {
            if (contribution.equals(0L)) continue;

            double p = contribution / total;
            entropy -= p * Math.log(p);
            numBins++;
        }

//        entropy /= numBins;

        return entropy;
    }

    protected void persistUpdatedCatalogue(List<String> updatedCatalogue) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter("updatedCatalogue.csv"));
        for (String entry : updatedCatalogue) {
            writer.write(entry + "\n");
        }

        for (String entry : catalogueOthers) {
            writer.write(entry + "\n");
        }
        writer.close();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println("catalogueFile: " + args[1]);
        System.out.println("specifiedCatLength: " + args[2]);
        System.out.println("uniformityMeasure: " + args[3]);
        System.out.println("destEntFile: " + args[4]);
        System.out.println("persistCatalogue: " + args[5]);
        System.out.println();

        int i = args[4].lastIndexOf('/');
        String cvFile, entropyFile;
        if (i == -1) {
            cvFile = "cv_" + args[4];
            entropyFile = "entropy_" + args[4];
        } else {
            cvFile = args[4].substring(0, i) + "/cv_" + args[4].substring(i + 1);
            entropyFile = args[4].substring(0, i) + "/entropy_" + args[4].substring(i + 1);
        }

        CatalogueEntropy catalogueEntropy = new CatalogueEntropy();
        catalogueEntropy.readGraph(args[0]);
        catalogueEntropy.readCatalogue(args[1], Integer.parseInt(args[2]));
        catalogueEntropy.computeDist();
        List<String> updatedCatalogue = catalogueEntropy.sanityCheck();
        catalogueEntropy.persistDist();
        if (Boolean.parseBoolean(args[5])) {
            catalogueEntropy.persistUpdatedCatalogue(updatedCatalogue);
        }

        catalogueEntropy.computeUniformityMeasure("cv");
        catalogueEntropy.persistUniformityMeasure(cvFile);
        catalogueEntropy.computeUniformityMeasure("entropy");
        catalogueEntropy.persistUniformityMeasure(entropyFile);
    }
}
