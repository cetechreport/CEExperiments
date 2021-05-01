package Pessimistic;

import Common.Pair;
import Graphflow.Constants;
import Pessimistic.Parallel.SubgraphExtractor;

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

public class SubqueryMaxDegExtractor1 {
    private Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    private Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();
    private Map<Integer, Map<Integer, List<Integer>>> label2src2dest = new HashMap<>();
    private Map<String, Integer> edge2count = new HashMap<>();

    // query topology -> decomVListString (defining decom topology) -> edge label seq -> count
    private List<Map<String, Map<String, Long>>> catalogue = new ArrayList<>();

    private List<String> subgraphFileNames = new ArrayList<>();
    private Map<String, Map<Integer, Long>> entry2maxOutDeg = new HashMap<>();
    private Map<String, Map<Integer, Long>> entry2maxInDeg = new HashMap<>();

    private void extractSubgraph() throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        List<Thread> threads = new ArrayList<>();
        SubgraphExtractor runner;
        Thread thread;

        final int NUM_ENTRY_WORKERS = 500;
        final int NUM_STARTER_WORKERS = 1;

        List<SubgraphExtractor> runnables = new ArrayList<>();
        for (int i = 0; i < NUM_ENTRY_WORKERS; ++i) {
            runnables.add(new SubgraphExtractor(i, src2label2dest, dest2label2src));
        }

        int runnerNum = 0;
        for (Map<String, Map<String, Long>> vList2entry : catalogue) {
            for (String vList : vList2entry.keySet()) {
                if (vList.split(";").length < 4) continue;
                for (String labelSeq : vList2entry.get(vList).keySet()) {
                    List<Integer> starters = new ArrayList<>(
                        label2src2dest.get(Integer.parseInt(labelSeq.split("->")[0])).keySet());
                    int numPerWorker = Math.max(1, starters.size() / NUM_STARTER_WORKERS);

                    for (int i = 0; i < starters.size(); i += numPerWorker) {
                        runner = runnables.get(runnerNum % NUM_ENTRY_WORKERS);
                        runner.addJob(vList, labelSeq, starters.subList(
                            i, Math.min(starters.size(), i + numPerWorker)
                        ));
                        subgraphFileNames.add(
                            vList + "_" + labelSeq + "_" + (runnerNum % NUM_ENTRY_WORKERS) + ".d");
                        runnerNum++;
                    }
                }
            }
        }

        endTime = System.currentTimeMillis();
        System.out.println("Worker Assignment: " + ((endTime - startTime) / 1000.0) + " sec");

        startTime = System.currentTimeMillis();
        for (Runnable runnable : runnables) {
            thread = new Thread(runnable);
            threads.add(thread);
            thread.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        endTime = System.currentTimeMillis();
        System.out.println("Subgraph Extracting: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void persistMaxDeg(String maxDegFileName) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        String filePrefix = "";
        Set<String> edges = new HashSet<>();

        for (String fileName : subgraphFileNames) {
            String[] parts = fileName.split("_");
            if (filePrefix.isEmpty() || filePrefix.equals(parts[0] + "," + parts[1])) {
                filePrefix = parts[0] + "," + parts[1];
                addEdges(fileName, edges);
                continue;
            }

            extractMaxDeg(filePrefix, edges);
            filePrefix = parts[0] + "," + parts[1];
            edges = new HashSet<>();
        }

        extractMaxDeg(filePrefix, edges);

        BufferedWriter writer = new BufferedWriter(new FileWriter(maxDegFileName));
        for (String entry : entry2maxOutDeg.keySet()) {
            Map<Integer, Long> maxOutDeg = entry2maxOutDeg.get(entry);
            Map<Integer, Long> maxInDeg = entry2maxInDeg.get(entry);
            for (Integer label : maxOutDeg.keySet()) {
                StringJoiner sj = new StringJoiner(",");
                sj.add(entry).add(Integer.toString(Constants.FORWARD))
                    .add(label.toString()).add(maxOutDeg.get(label).toString());
                writer.write(sj.toString() + "\n");
            }
            for (Integer label : maxInDeg.keySet()) {
                StringJoiner sj = new StringJoiner(",");
                sj.add(entry).add(Integer.toString(Constants.BACKWARD))
                    .add(label.toString()).add(maxInDeg.get(label).toString());
                writer.write(sj.toString() + "\n");
            }
        }
        writer.close();

        endTime = System.currentTimeMillis();
        System.out.println("Max Deg Persisting: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void extractMaxDeg(String filePrefix, Set<String> edges) {
        Map<Integer, Map<Integer, List<Integer>>> subgraphForward = new HashMap<>();
        Map<Integer, Map<Integer, List<Integer>>> subgraphBackward = new HashMap<>();

        int[] csv;
        for (String line : edges) {
            csv = Arrays.stream(line.split(",")).mapToInt(Integer::parseInt).toArray();

            for (int i = 0; i < edge2count.get(line); ++i) {
                subgraphForward.putIfAbsent(csv[0], new HashMap<>());
                subgraphForward.get(csv[0]).putIfAbsent(csv[1], new ArrayList<>());
                subgraphForward.get(csv[0]).get(csv[1]).add(csv[2]);

                subgraphBackward.putIfAbsent(csv[2], new HashMap<>());
                subgraphBackward.get(csv[2]).putIfAbsent(csv[1], new ArrayList<>());
                subgraphBackward.get(csv[2]).get(csv[1]).add(csv[0]);
            }
        }

        Pair<Map<Integer, Long>, Map<Integer, Long>> maxDeg =
            computeMaxDeg(subgraphForward, subgraphBackward);

        entry2maxOutDeg.put(filePrefix, maxDeg.key);
        entry2maxInDeg.put(filePrefix, maxDeg.value);
    }

    private void addEdges(String fileName, Set<String> edges) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(fileName));
        String line = reader.readLine();
        while (null != line) {
            edges.add(line);
            line = reader.readLine();
        }
        reader.close();
    }

    private Pair<Map<Integer, Long>, Map<Integer, Long>> computeMaxDeg(
        Map<Integer, Map<Integer, List<Integer>>> graphForward,
        Map<Integer, Map<Integer, List<Integer>>> graphBackward) {
        Map<Integer, Long> maxOutDeg = new HashMap<>();
        Map<Integer, Long> maxInDeg = new HashMap<>();

        for (Map<Integer, List<Integer>> label2dest : graphForward.values()) {
            for (Integer label : label2dest.keySet()) {
                maxOutDeg.put(label,
                    Math.max(maxOutDeg.getOrDefault(label, 0L), label2dest.get(label).size()));
            }
        }

        for (Map<Integer, List<Integer>> label2src : graphBackward.values()) {
            for (Integer label : label2src.keySet()) {
                maxInDeg.put(label,
                    Math.max(maxInDeg.getOrDefault(label, 0L), label2src.get(label).size()));
            }
        }

        return new Pair<>(maxOutDeg, maxInDeg);
    }

    private void readCatalogue(String catalogueFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader catalogueReader = new BufferedReader(new FileReader(catalogueFile));
        String[] info;
        String vList, labelSeq;
        String line = catalogueReader.readLine();
        while (null != line) {
            info = line.split(",");
            int queryType = Integer.parseInt(info[0]);
            while (catalogue.size() <= queryType) {
                catalogue.add(new HashMap<>());
            }

            vList = info[1];
            labelSeq = info[2];
            Long count = Long.parseLong(info[3]);
            catalogue.get(queryType).putIfAbsent(vList, new HashMap<>());
            catalogue.get(queryType).get(vList).put(labelSeq, count);

            line = catalogueReader.readLine();
        }
        catalogueReader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Loading Catalogue: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void readGraph(String graphFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader csvReader = new BufferedReader(new FileReader(graphFile));
        int[] line;
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            line = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt).toArray();

            src2label2dest.putIfAbsent(line[0], new HashMap<>());
            src2label2dest.get(line[0]).putIfAbsent(line[1], new ArrayList<>());
            src2label2dest.get(line[0]).get(line[1]).add(line[2]);

            dest2label2src.putIfAbsent(line[2], new HashMap<>());
            dest2label2src.get(line[2]).putIfAbsent(line[1], new ArrayList<>());
            dest2label2src.get(line[2]).get(line[1]).add(line[0]);

            label2src2dest.putIfAbsent(line[1], new HashMap<>());
            label2src2dest.get(line[1]).putIfAbsent(line[0], new ArrayList<>());
            label2src2dest.get(line[1]).get(line[0]).add(line[2]);

            edge2count.put(tripleString, edge2count.getOrDefault(tripleString, 0) + 1);

            tripleString = csvReader.readLine();
        }

        endTime = System.currentTimeMillis();
        System.out.println("Graph Loading: " + ((endTime - startTime) / 1000.0) + " sec");

        csvReader.close();
    }

    private void prepare(String graphFile, String catalogueFile) throws Exception {
        readGraph(graphFile);
        readCatalogue(catalogueFile);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println("catalogueFile: " + args[1]);
        System.out.println("maxDegFile: " + args[2]);
        System.out.println("");

        SubqueryMaxDegExtractor1 extractor = new SubqueryMaxDegExtractor1();
        extractor.prepare(args[0], args[1]);
        extractor.extractSubgraph();
        extractor.persistMaxDeg(args[2]);
    }
}
