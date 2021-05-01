package MarkovTable;

import Common.Pair;
import Common.Path;

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

public class ParallelMarkovTable {
    private Map<Integer, List<Pair<Integer, Integer>>> label2srcdest;
    private Map<Integer, Map<Integer, List<Integer>>> src2label2dest;
    private Map<Integer, Map<Integer, List<Integer>>> dest2label2src;
    private String baseDir;
    private int mtLength;

    class PartialMTRunnable implements Runnable {
        int label;
        Map<Path, Long> backwardCounts;
        Map<Integer, List<List<Integer>>> currentStepNodes2prefixes;
        Map<Path, Long> table;

        public PartialMTRunnable(int startingEdge) {
            this.label = startingEdge;
            this.table = new HashMap<>();
        }

        class Backward implements Runnable {
            int initSrc;

            public Backward(int initSrc) {
                this.initSrc = initSrc;
            }

            public void run() {
                Map<Integer, List<List<Integer>>> currentStepNodes2suffixes = new HashMap<>();
                boolean canBackward = dest2label2src.containsKey(initSrc);

                int pathStep = mtLength / 2 + mtLength % 2;

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

                int pathStep = mtLength / 2 + mtLength % 2;

                if (pathStep < mtLength - 1 && canForward) {
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
                while (pathStep < mtLength - 1 && canForward) {
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
            int initSrc, initDest;
            long backStartTime, backEndTime, forwardStartTime, forwardEndTime;
            long countStartTime, countEndTime, overallStartTime, overallEndTime;

            System.out.println("[" + label + "]: START (" + label2srcdest.get(label).size() + ")");
            overallStartTime = System.currentTimeMillis();
            for (Pair<Integer, Integer> srcdest: label2srcdest.get(label)) {
                initSrc = srcdest.getKey();
                initDest = srcdest.getValue();

                backwardCounts = new HashMap<>();

                Runnable backRunnable = new Backward(initSrc);
                Thread backward = new Thread(backRunnable);
                backStartTime = System.currentTimeMillis();
                backward.start();
                System.out.println("[" + label + "][" + initSrc + "," + initDest + "]: Backward START");

                currentStepNodes2prefixes = new HashMap<>();

                Runnable forRunnable = new Forward(initDest);
                Thread forward = new Thread(forRunnable);
                forwardStartTime = System.currentTimeMillis();
                forward.start();
                System.out.println("[" + label + "][" + initSrc + "," + initDest + "]: Forward START");

                try {
                    backward.join();
                    backEndTime = System.currentTimeMillis();
                    System.out.println("[" + label + "][" + initSrc + "," + initDest + "]: "
                            + "Backward " + ((backEndTime - backStartTime) / 1000.0) + " seconds");

                    forward.join();
                    forwardEndTime = System.currentTimeMillis();
                    System.out.println("[" + label + "][" + initSrc + "," + initDest + "]: "
                        + "Forward " + ((forwardEndTime - forwardStartTime) / 1000.0) + " seconds");

                    System.out.println("[" + label + "][" + initSrc + "," + initDest + "]: Count START");

                    Path finalPath;
                    long suffixCount; // #C
                    long count;

                    countStartTime = System.currentTimeMillis();
                    if (currentStepNodes2prefixes.size() == 0) {
                        if (src2label2dest.containsKey(initDest)) {
                            if (backwardCounts.size() > 0) {
                                for (Path prefix : backwardCounts.keySet()) {
                                    for (int suffix : src2label2dest.get(initDest).keySet()) {
                                        finalPath = new Path(prefix);
                                        finalPath.append(label);
                                        finalPath.append(suffix);

                                        suffixCount = src2label2dest.get(initDest).get(suffix).size();
                                        count = table.getOrDefault(finalPath, 0L) +
                                            (backwardCounts.get(prefix) * suffixCount);
                                        table.put(finalPath, count);
                                    }
                                }
                            } else if (mtLength == 2) {
                                for (int suffix : src2label2dest.get(initDest).keySet()) {
                                    List<Integer> edgeList = new ArrayList<>();
                                    edgeList.add(label);
                                    edgeList.add(suffix);
                                    finalPath = new Path(edgeList);

                                    suffixCount = src2label2dest.get(initDest).get(suffix).size();
                                    count = table.getOrDefault(finalPath, 0L) + suffixCount;
                                    table.put(finalPath, count);
                                }
                            }
                        }
                    } else {
                        Path prefix;
                        for (int finalNode : currentStepNodes2prefixes.keySet()) {
                            if (src2label2dest.containsKey(finalNode)) {
                                for (List<Integer> midSuffix : currentStepNodes2prefixes.get(finalNode)) {
                                    for (Path midPrefix : backwardCounts.keySet()) {
                                        prefix = new Path(midPrefix);
                                        prefix.append(label);
                                        prefix.appendAll(midSuffix);

                                        for (int suffix : src2label2dest.get(finalNode).keySet()) {
                                            finalPath = new Path(prefix);
                                            finalPath.append(suffix);

                                            suffixCount = src2label2dest.get(finalNode).get(suffix).size();
                                            count = table.getOrDefault(finalPath, 0L) +
                                                (backwardCounts.get(midPrefix) * suffixCount);
                                            table.put(finalPath, count);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    countEndTime = System.currentTimeMillis();
                    System.out.println("[" + label + "][" + initSrc + "," + initDest
                        + "]: Count " + ((countEndTime - countStartTime) / 1000.0) + " seconds");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            overallEndTime = System.currentTimeMillis();
            System.out.println("[" + label + "]: DONE ("
                + ((overallEndTime - overallStartTime) / 1000.0) + " seconds)");

            try {
                String zeroPadded = String.format("%02d", label);
                BufferedWriter mtWriter = new BufferedWriter(
                    new FileWriter(baseDir + "/" + zeroPadded + ".csv")
                );
                for (Path path : table.keySet()) {
                    mtWriter.write(path.toSimpleString() + "," + table.get(path) + "\n");
                }
                mtWriter.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public ParallelMarkovTable(String csvFilePath, int mtLength, String baseDir)
            throws Exception {
        this.baseDir = baseDir;
        this.mtLength = mtLength;

        label2srcdest = new HashMap<>();
        src2label2dest = new HashMap<>();
        dest2label2src = new HashMap<>();

        BufferedReader csvReader = new BufferedReader(new FileReader(csvFilePath));
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

        csvReader.close();
    }

    public void build() throws Exception {
        List<Thread> threads = new ArrayList<>();
        PartialMTRunnable runnable;
        List<Integer> startingEdges = new ArrayList<>(label2srcdest.keySet());
        Thread thread;

        for (int i = 0; i < startingEdges.size(); ++i) {
            runnable = new PartialMTRunnable(startingEdges.get(i));
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
        int mtLen = Integer.parseInt(args[1]);
        File mtTableDir = new File(args[2]);
        if (!mtTableDir.exists()) {
            mtTableDir.mkdir();
        }

        String baseDir = args[2].replaceAll("/$", "");
        ParallelMarkovTable parallelMT = new ParallelMarkovTable(graphFile, mtLen, baseDir);
        parallelMT.build();
    }
}
