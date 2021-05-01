package MarkovTable;

import Common.Pair;
import Common.Path;
import Common.Stat;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class Uniformity {
    private Map<Integer, List<Pair<Integer, Integer>>> label2srcdest;
    private Map<Integer, Map<Integer, List<Integer>>> src2label2dest;
    private Map<Integer, Map<Integer, List<Integer>>> dest2label2src;
    private int pathLength;
    private String baseDir;
    private boolean sampling;

    int ANY = -1;
    int SAMPLE_NUMBER = 100000;

    class PartialUniformityRunnable implements Runnable {
        int label;
        Map<Path, Long> backwardCounts;
        Map<Integer, List<List<Integer>>> currentStepNodes2prefixes;

        public PartialUniformityRunnable(int label) {
            this.label = label;
        }

        class Backward implements Runnable {
            int initSrc;

            public Backward(int initSrc) {
                this.initSrc = initSrc;
            }

            public void run() {
                Map<Integer, List<List<Integer>>> currentStepNodes2suffixes = new HashMap<>();
                boolean canBackward = dest2label2src.containsKey(initSrc);

                int pathStep = pathLength / 2 + pathLength % 2;

                Path path;

                if (pathStep > 1 && canBackward) {
                    // prepare starting labels for going backward
                    for (int backStartLabel : dest2label2src.get(initSrc).keySet()) {
                        List<Integer> startingLabel = new ArrayList<>();
                        startingLabel.add(backStartLabel);
                        for (int v : dest2label2src.get(initSrc).get(backStartLabel)) {
                            currentStepNodes2suffixes.putIfAbsent(v, new ArrayList<>());
                            currentStepNodes2suffixes.get(v).add(startingLabel);

                            if (pathStep == 2) {
                                path = new Path(startingLabel);
                                backwardCounts.put(
                                    path,
                                    backwardCounts.getOrDefault(path, 0L) + 1
                                );
                            }
                        }
                    }

                    pathStep--;
                }

                Map<Integer, List<List<Integer>>> nextStepNodes2suffixes = new HashMap<>();
                Map<Integer, List<Integer>> nextLabels2src;
                List<Integer> edgeLabelPath;

                // going backward
                while (pathStep > 1 && canBackward) {
                    for (Integer dest: currentStepNodes2suffixes.keySet()) {
                        if (dest2label2src.containsKey(dest)) {
                            nextLabels2src = dest2label2src.get(dest);
                            for (Integer nextLabel : nextLabels2src.keySet()) {
                                for (Integer src: nextLabels2src.get(nextLabel)) {
                                    for (List<Integer> suffix: currentStepNodes2suffixes.get(dest)) {
                                        edgeLabelPath = new ArrayList<>();
                                        edgeLabelPath.add(nextLabel);
                                        edgeLabelPath.addAll(suffix);
                                        nextStepNodes2suffixes.putIfAbsent(src, new ArrayList<>());
                                        nextStepNodes2suffixes.get(src).add(edgeLabelPath);

                                        if (pathStep == 2) {
                                            path = new Path(edgeLabelPath);
                                            backwardCounts.put(
                                                path,
                                                backwardCounts.getOrDefault(path, 0L) + 1
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }

                    currentStepNodes2suffixes = new HashMap<>(nextStepNodes2suffixes);
                    nextStepNodes2suffixes.clear();
                    pathStep--;
                }

//                String str = "Mid Label: " + label + "\n" + "SUFFIX" + "\n";
//                for (Map.Entry<Integer, List<List<Integer>>> entry : currentStepNodes2suffixes.entrySet()) {
//                    String suffixes = "";
//                    for (List<Integer> suffix: entry.getValue()) {
//                        suffixes += suffix.toString() + " ";
//                    }
//
//                    str += entry.getKey() + ": " + suffixes + "\n";
//                }
//                System.out.print(str);
            }
        }

        class Forward implements Runnable {
            int initDest;
            public Forward(int initDest) {
                this.initDest = initDest;
            }

            public void run() {
                // going forward
                boolean canForward = src2label2dest.containsKey(initDest);

                int pathStep = pathLength / 2 + pathLength % 2;

                if (pathStep < pathLength - 1 && canForward) {
                    // prepare starting labels for going backward
                    for (int forStartLabel : src2label2dest.get(initDest).keySet()) {
                        List<Integer> startingLabel = new ArrayList<>();
                        startingLabel.add(forStartLabel);
                        for (int v : src2label2dest.get(initDest).get(forStartLabel)) {
                            currentStepNodes2prefixes.putIfAbsent(v, new ArrayList<>());
                            currentStepNodes2prefixes.get(v).add(startingLabel);
                        }
                    }

                    pathStep++;
                }

//                System.out.println("Size: " + currentStepNodes2prefixes.size());
//
//                String str = "Mid Label: " + label + "\n";
//                str += "initDest: " + initDest + "\n";
//                for (int node: currentStepNodes2prefixes.keySet()) {
//                    String prefixes = "";
//                    for (List<Integer> prefix: currentStepNodes2prefixes.get(node)) {
//                        prefixes += prefix + " ";
//                    }
//                    str += "Node: " + node + " - " + prefixes + "\n";
//                }
//                System.out.print(str);

                Map<Integer, List<List<Integer>>> nextStepNodes2prefixes = new HashMap<>();
                Map<Integer, List<Integer>> nextLabels2dest;
                List<Integer> edgeLabelPath;

                // going forward
                while (pathStep < pathLength - 1 && canForward) {
                    for (Integer src: currentStepNodes2prefixes.keySet()) {
                        if (src2label2dest.containsKey(src)) {
                            nextLabels2dest = src2label2dest.get(src);
                            for (Integer nextLabel : nextLabels2dest.keySet()) {
                                for (Integer dest: nextLabels2dest.get(nextLabel)) {
                                    for (List<Integer> prefix: currentStepNodes2prefixes.get(src)) {
                                        edgeLabelPath = new ArrayList<>(prefix);
                                        edgeLabelPath.add(nextLabel);
                                        nextStepNodes2prefixes.putIfAbsent(dest, new ArrayList<>());
                                        nextStepNodes2prefixes.get(dest).add(edgeLabelPath);
                                    }
                                }
                            }
                        }
                    }

                    currentStepNodes2prefixes = new HashMap<>(nextStepNodes2prefixes);
                    nextStepNodes2prefixes.clear();
                    pathStep++;
                }
            }
        }

        public void run() {
            // ABC -> (#C on a particular final prefix node -> #AB prefix going into that final node)
            Map<Path, Map<Long, Long>> distributions = new HashMap<>();

            // prefixDestNode -> (prefix -> count)
            Map<Integer, Map<Path, Long>> prefixDistribution = new HashMap<>();

            // ABC -> unique dest nodes of AB that extend to C
            Map<Path, Set<Integer>> uniqueLivePrefixDests = new HashMap<>();

            // ABC -> unique dest nodes of ABC
            Map<Path, Set<Integer>> uniqueDests = new HashMap<>();

            // ABC -> unique dest nodes of prefix
            Map<Path, Set<Integer>> uniquePrefixDests = new HashMap<>();

            int initSrc, initDest;

            List<Pair<Integer, Integer>> srcdestOfLabel;
            int numSrcDestPairs = label2srcdest.get(label).size();
            if (sampling && numSrcDestPairs > SAMPLE_NUMBER) {
                System.out.println("[" + label + "]: SAMPLING (" + label2srcdest.get(label).size() + ")");

                Set<Integer> randomIndices = new HashSet<>();
                srcdestOfLabel = new ArrayList<>();

                Random random = new Random();

                int index;
                while (randomIndices.size() < SAMPLE_NUMBER) {
                     index = random.nextInt(numSrcDestPairs);
                     while (randomIndices.contains(index)) {
                         index = random.nextInt(numSrcDestPairs);
                     }
                     randomIndices.add(index);

                     srcdestOfLabel.add(label2srcdest.get(label).get(index));
                }
            } else {
                srcdestOfLabel = label2srcdest.get(label);
            }

            System.out.println("[" + label + "]: START (" + srcdestOfLabel.size() + ")");
            for (Pair<Integer, Integer> srcdest: srcdestOfLabel) {
                initSrc = srcdest.getKey();
                initDest = srcdest.getValue();

                backwardCounts = new HashMap<>();

                Runnable backRunnable = new Backward(initSrc);
                Thread backward = new Thread(backRunnable);
                backward.start();
//                System.out.println("[" + label + "][" + initSrc + "," + initDest + "]: Backward START");

                currentStepNodes2prefixes = new HashMap<>();

                Runnable forRunnable = new Forward(initDest);
                Thread forward = new Thread(forRunnable);
                forward.start();
//                System.out.println("[" + label + "][" + initSrc + "," + initDest + "]: Forward START");

                try {
                    backward.join();
//                    System.out.println("[" + label + "][" + initSrc + "," + initDest + "]: Backward DONE");

                    forward.join();
//                    System.out.println("[" + label + "][" + initSrc + "," + initDest + "]: Forward DONE");

//                    System.out.println("BACKWARD");
//                    for (Path p: backwardCounts.keySet()) {
//                        System.out.println(p.toCsv() + ":" + backwardCounts.get(p));
//                    }

//                    System.out.println("PREFIX");
//                    for (Map.Entry<Integer, List<List<Integer>>> entry : currentStepNodes2prefixes.entrySet()) {
//                        String prefixes = "";
//                        for (List<Integer> prefix: entry.getValue()) {
//                            prefixes += prefix.toString() + " ";
//                        }
//
//                        System.out.println(entry.getKey() + ": " + prefixes);
//                    }

//                    System.out.println("[" + label + "][" + initSrc + "," + initDest + "]: Extend START");
                    Path finalPath, prefixStar;
                    long suffixCount; // #C
                    long count;
                    if (currentStepNodes2prefixes.size() == 0) {
                        if (backwardCounts.size() > 0) {
                            for (Path prefix : backwardCounts.keySet()) {

                                prefixStar = new Path(prefix);
                                prefixStar.append(label);
                                prefixStar.append(ANY);

                                uniquePrefixDests.putIfAbsent(prefixStar, new HashSet<>());
                                uniquePrefixDests.get(prefixStar).add(initDest);

                                prefixDistribution.putIfAbsent(initDest, new HashMap<>());
                                count = prefixDistribution.get(initDest).getOrDefault(prefixStar, 0L)
                                    + backwardCounts.get(prefix);
                                prefixDistribution.get(initDest).put(prefixStar, count);

                                if (src2label2dest.containsKey(initDest)) {
                                    for (int suffix : src2label2dest.get(initDest).keySet()) {
                                        finalPath = new Path(prefix);
                                        finalPath.append(label);
                                        finalPath.append(suffix);

                                        distributions.putIfAbsent(finalPath, new HashMap<>());
                                        uniqueLivePrefixDests.putIfAbsent(finalPath, new HashSet<>());
                                        uniqueDests.putIfAbsent(finalPath, new HashSet<>());

                                        uniqueLivePrefixDests.get(finalPath).add(initDest);
                                        uniqueDests.get(finalPath).addAll(
                                            src2label2dest.get(initDest).get(suffix)
                                        );

                                        suffixCount = src2label2dest.get(initDest).get(suffix).size();
                                        count = distributions.get(finalPath).getOrDefault(suffixCount, 0L) +
                                            backwardCounts.get(prefix);
                                        distributions.get(finalPath).put(suffixCount, count);
                                    }
                                }
                            }
                        } else if (pathLength == 2) {
                            prefixStar = new Path(new ArrayList<>());
                            prefixStar.append(label);
                            prefixStar.append(ANY);

                            uniquePrefixDests.putIfAbsent(prefixStar, new HashSet<>());
                            uniquePrefixDests.get(prefixStar).add(initDest);

                            prefixDistribution.putIfAbsent(initDest, new HashMap<>());
                            count = prefixDistribution.get(initDest).getOrDefault(prefixStar, 0L) + 1;
                            prefixDistribution.get(initDest).put(prefixStar, count);

                            if (src2label2dest.containsKey(initDest)) {
                                for (int suffix : src2label2dest.get(initDest).keySet()) {
                                    List<Integer> edgeList = new ArrayList<>();
                                    edgeList.add(label);
                                    edgeList.add(suffix);
                                    finalPath = new Path(edgeList);

                                    distributions.putIfAbsent(finalPath, new HashMap<>());
                                    uniqueLivePrefixDests.putIfAbsent(finalPath, new HashSet<>());

                                    uniqueDests.putIfAbsent(finalPath, new HashSet<>());

                                    uniqueLivePrefixDests.get(finalPath).add(initDest);
                                    uniqueDests.get(finalPath).addAll(
                                        src2label2dest.get(initDest).get(suffix)
                                    );

                                    suffixCount = src2label2dest.get(initDest).get(suffix).size();
                                    count = distributions.get(finalPath).getOrDefault(suffixCount, 0L) + 1;
                                    distributions.get(finalPath).put(suffixCount, count);
                                }
                            }
                        }
                    } else {
                        Path prefix;
                        for (int finalNode : currentStepNodes2prefixes.keySet()) {
                            for (List<Integer> midSuffix : currentStepNodes2prefixes.get(finalNode)) {
                                for (Path midPrefix : backwardCounts.keySet()) {
                                    prefix = new Path(midPrefix);
                                    prefix.append(label);
                                    prefix.appendAll(midSuffix);

                                    prefixStar = new Path(prefix);
                                    prefixStar.append(ANY);

                                    uniquePrefixDests.putIfAbsent(prefixStar, new HashSet<>());
                                    uniquePrefixDests.get(prefixStar).add(finalNode);

                                    prefixDistribution.putIfAbsent(finalNode, new HashMap<>());
                                    count = prefixDistribution.get(finalNode).getOrDefault(prefixStar,0L)
                                        + backwardCounts.get(midPrefix);
                                    prefixDistribution.get(finalNode).put(prefixStar, count);

                                    if (src2label2dest.containsKey(finalNode)) {
                                        for (int suffix : src2label2dest.get(finalNode).keySet()) {

                                            finalPath = new Path(prefix);
                                            finalPath.append(suffix);

                                            distributions.putIfAbsent(finalPath, new
                                                HashMap<>());
                                            uniqueLivePrefixDests.putIfAbsent(finalPath, new HashSet<>());
                                            uniqueDests.putIfAbsent(finalPath, new HashSet<>());

                                            uniqueLivePrefixDests.get(finalPath).add(finalNode);
                                            uniqueDests.get(finalPath).addAll(
                                                src2label2dest.get(finalNode).get(suffix)
                                            );

                                            suffixCount = src2label2dest.get(finalNode).get(suffix).size();
                                            count = distributions.get(finalPath).getOrDefault(suffixCount, 0L) +
                                                backwardCounts.get(midPrefix);
                                            distributions.get(finalPath).put(suffixCount, count);
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
//                System.out.println("[" + label + "][" + initSrc + "," + initDest + "]: Extend DONE");
            }

            System.out.println("[" + label + "]: PROCESS DIST 0");
            Set<Integer> deadPrefixDests;
            Path prefix;
            long count;
            for (Path p: distributions.keySet()) {
                prefix = p.getPrefix();
                prefix.append(ANY);
                deadPrefixDests = new HashSet<>(uniquePrefixDests.get(prefix));
                deadPrefixDests.removeAll(uniqueLivePrefixDests.get(p));

                for (int deadPrefixDest: deadPrefixDests) {
                    count = distributions.get(p).getOrDefault(0L, 0L) +
                        prefixDistribution.get(deadPrefixDest).get(prefix);
                    distributions.get(p).put(0L, count);
                }
            }

            System.out.println("[" + label + "]: DONE");

//            for (Path p: distributions.keySet()) {
//                String histogram = "";
//                for (long entry: distributions.get(p).keySet()) {
//                    histogram += entry + "->" + distributions.get(p).get(entry) + ",";
//                }
//                System.out.println(p.toCsv() + ": " + histogram);
//            }

            try {
                String zeroPadded = String.format("%02d", label);
                BufferedWriter mtWriter = new BufferedWriter(
                    new FileWriter(baseDir + "/" + zeroPadded + ".csv")
                );

                long totalNumBasePathInc0, totalNumBasePathExc0;
                Stat stat;
                Map<Long, Long> histogram;
                for (Path p : distributions.keySet()) {
                    stat = new Stat();

                    stat.numUniqueLivePrefixDests = uniqueLivePrefixDests.getOrDefault(p, new HashSet<>()).size();
                    stat.numUniqueDests = uniqueDests.getOrDefault(p, new HashSet<>()).size();

                    stat.avgDestInDeg = computeAvgDestInDeg(uniqueDests.getOrDefault(p, new HashSet<>()));
                    stat.avgDestOutDeg = computeAvgDestOutDeg(uniqueDests.getOrDefault(p, new HashSet<>()));

                    prefix = p.getPrefix();
                    prefix.append(ANY);
                    stat.numUniquePrefixDests = uniquePrefixDests.get(prefix).size();

                    stat.maxExc0 = Long.MIN_VALUE;
                    stat.minExc0 = Long.MAX_VALUE;
                    stat.maxInc0 = Long.MIN_VALUE;
                    stat.minInc0 = Long.MAX_VALUE;
                    stat.cardinality = 0;
                    totalNumBasePathExc0 = 0;
                    totalNumBasePathInc0 = 0;

                    for (long entry : distributions.get(p).keySet()) {
                        stat.cardinality += entry * distributions.get(p).get(entry);

                        stat.maxInc0 = Math.max(stat.maxInc0, entry);
                        stat.minInc0 = Math.min(stat.minInc0, entry);
                        totalNumBasePathInc0 += distributions.get(p).get(entry);

                        if (entry != 0) {
                            stat.maxExc0 = Math.max(stat.maxExc0, entry);
                            stat.minExc0 = Math.min(stat.minExc0, entry);
                            totalNumBasePathExc0 += distributions.get(p).get(entry);
                        }
                    }

                    stat.meanInc0 = 1.0 * stat.cardinality / totalNumBasePathInc0;
                    stat.sdInc0 = computeSD(distributions.get(p), stat.meanInc0, totalNumBasePathInc0);
                    stat.cvInc0 = stat.sdInc0 / stat.meanInc0;

                    stat.meanExc0 = 1.0 * stat.cardinality / totalNumBasePathExc0;
                    histogram = distributions.get(p);
                    histogram.remove(0L);
                    stat.sdExc0 = computeSD(histogram, stat.meanExc0, totalNumBasePathExc0);
                    stat.cvExc0 = stat.sdExc0 / stat.meanExc0;

                    mtWriter.write(p.toSimpleString() + "," + stat.toCsv() + "\n");
//                    System.out.println(p.toSimpleString() + "," + stat.toCsv());
                }
                mtWriter.close();

                System.out.println("[" + label + "]: COMPLETE");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private Uniformity(String graphFile, int pathLength, String baseDir, boolean sampling) throws Exception {
        this.pathLength = pathLength;
        this.baseDir = baseDir;
        this.sampling = sampling;

        label2srcdest = new HashMap<>();
        src2label2dest = new HashMap<>();
        dest2label2src = new HashMap<>();

        BufferedReader csvReader = new BufferedReader(new FileReader(graphFile));
        int[] edge;
        String line = csvReader.readLine();
        while (null != line) {
            edge = Arrays.stream(line.split(",")).mapToInt(Integer::parseInt).toArray();

            label2srcdest.putIfAbsent(edge[1], new ArrayList<>());
            label2srcdest.get(edge[1]).add(new Pair<>(edge[0], edge[2]));

            src2label2dest.putIfAbsent(edge[0], new HashMap<>());
            src2label2dest.get(edge[0]).putIfAbsent(edge[1], new ArrayList<>());
            src2label2dest.get(edge[0]).get(edge[1]).add(edge[2]);

            dest2label2src.putIfAbsent(edge[2], new HashMap<>());
            dest2label2src.get(edge[2]).putIfAbsent(edge[1], new ArrayList<>());
            dest2label2src.get(edge[2]).get(edge[1]).add(edge[0]);

            line = csvReader.readLine();
        }

        csvReader.close();
    }

    private void build() throws Exception {
        List<Thread> threads = new ArrayList<>();
        PartialUniformityRunnable runnable;
        List<Integer> labels = new ArrayList<>(label2srcdest.keySet());
        Thread thread;

        for (int i = 0; i < labels.size(); ++i) {
            runnable = new PartialUniformityRunnable(labels.get(i));
            thread = new Thread(runnable);
            threads.add(thread);
            thread.start();
        }

        for (int i = 0; i < threads.size(); ++i) {
            threads.get(i).join();

            if (!threads.get(i).isAlive()) {
                threads.set(i, null);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String graphFile = args[0];
        int pathLength = Integer.parseInt(args[1]);
        String baseDir = args[2].replaceAll("/$", "");
        File outputDir = new File(args[2]);
        if (!outputDir.exists()) {
            outputDir.mkdir();
        }
        boolean sampling = args[3].contains("true") || args[3].contains("sampling");

        Uniformity uniformity = new Uniformity(graphFile, pathLength, baseDir, sampling);
        uniformity.build();
    }

    private double computeSD(Map<Long, Long> histogram, double mean, long totalNumBasePaths) {
        double sd = 0;

        for (long entry: histogram.keySet()) {
            sd += (Math.pow(entry - mean, 2) / totalNumBasePaths) * histogram.get(entry);
        }

        return Math.sqrt(sd);
    }

    private double computeAvgDestInDeg(Set<Integer> dests) {
        double totalInDeg = 0;
        for (int dest : dests) {
            if (!dest2label2src.containsKey(dest)) continue;

            for (int label : dest2label2src.get(dest).keySet()) {
                totalInDeg += dest2label2src.get(dest).get(label).size();
            }
        }

        return totalInDeg / dests.size();
    }

    private double computeAvgDestOutDeg(Set<Integer> dests) {
        double totalOutDeg = 0;
        for (int dest : dests) {
            if (!src2label2dest.containsKey(dest)) continue;

            for (int label : src2label2dest.get(dest).keySet()) {
                totalOutDeg += src2label2dest.get(dest).get(label).size();
            }
        }

        return totalOutDeg / dests.size();
    }
}
