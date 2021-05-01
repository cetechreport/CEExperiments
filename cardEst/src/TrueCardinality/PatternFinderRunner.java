package TrueCardinality;

import TrueCardinality.Parallel.PatternFinder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PatternFinderRunner {
    protected Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    protected Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();
    protected Map<Integer, Map<Integer, List<Integer>>> src2dest2label = new HashMap<>();
    protected Map<Integer, Map<Integer, List<Integer>>> dest2src2label = new HashMap<>();

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

            src2dest2label.putIfAbsent(line[0], new HashMap<>());
            src2dest2label.get(line[0]).putIfAbsent(line[2], new ArrayList<>());
            src2dest2label.get(line[0]).get(line[2]).add(line[1]);

            dest2src2label.putIfAbsent(line[2], new HashMap<>());
            dest2src2label.get(line[2]).putIfAbsent(line[0], new ArrayList<>());
            dest2src2label.get(line[2]).get(line[0]).add(line[1]);

            tripleString = csvReader.readLine();
        }

        endTime = System.currentTimeMillis();
        System.out.println("Graph Loading: " + ((endTime - startTime) / 1000.0) + " sec");

        csvReader.close();
    }

    protected void findPattern(String filePrefix, Integer patternType, String pattern) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        List<Thread> threads = new ArrayList<>();
        Runnable patternFinder;
        Thread thread;
        final int NUM_THREAD = 1000;
        int threadId = 0;

        List<Integer> starters = new ArrayList<>(src2label2dest.keySet());
        for (int i = 0; i < starters.size(); i += starters.size() / NUM_THREAD) {
            threadId++;
            patternFinder = new PatternFinder(
                threadId,
                filePrefix,
                src2label2dest,
                dest2label2src,
                patternType,
                pattern,
                starters.subList(i, Math.min(starters.size(), i + starters.size() / NUM_THREAD))
            );

            thread = new Thread(patternFinder);
            threads.add(thread);
            thread.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        endTime = System.currentTimeMillis();
        System.out.println("Pattern Finding: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public PatternFinderRunner(String graphFile) throws Exception {
        readGraph(graphFile);
    }

    public PatternFinderRunner() {}

    protected void run() throws Exception {
        findPattern("star", 0, "0-1;1-2;1-3;1-4;1-5;1-6");
        findPattern("fork24", 3, "0-1;0-4;2-4;3-4;4-5;4-6");
        findPattern("pi", 4, "0-1;0-2;0-3;3-6;4-6;5-6");
        findPattern("fork33", 5, "0-1;1-2;2-5;3-5;4-5;5-6");
        findPattern("fork34", 6, "0-1;1-2;2-5;3-5;4-5;5-6;5-7");
        findPattern("bifork", 7, "0-1;1-2;1-3;3-6;4-6;5-6;6-7;6-8");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println();

        PatternFinderRunner runner = new PatternFinderRunner(args[0]);
        runner.run();
    }
}
