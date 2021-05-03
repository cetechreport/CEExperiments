package TrueCardinality;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class CyclicPatternFinderRunner extends PatternFinderRunner {
    public CyclicPatternFinderRunner(String graphFile) throws Exception {
        super();
        readGraph(graphFile);
    }

    private void readGraph(String graphFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader csvReader = new BufferedReader(new FileReader(graphFile));
        int[] line;
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            line = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt).toArray();

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
        final int NUM_THREAD = 50;
        int threadId = 0;

        List<Integer> starters = new ArrayList<>(src2dest2label.keySet());
        for (int i = 0; i < starters.size(); i += starters.size() / NUM_THREAD) {
            threadId++;
            Random random = new Random(threadId);
            patternFinder = new CyclicPatternFinder(
                threadId,
                filePrefix,
                new HashMap<>(),
                new HashMap<>(),
                src2dest2label,
                dest2src2label,
                patternType,
                pattern,
                starters.subList(i, Math.min(starters.size(), i + starters.size() / NUM_THREAD)),
                random
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

    protected void run() throws Exception {
//        findPattern("301", 301, "0-1;0-2;0-3;1-2;2-3");
//        findPattern("302", 302, "0-1;0-2;1-2;2-3;2-4;3-4");
//        findPattern("303", 303, "0-1;0-2;1-2;2-3;3-4");
//        findPattern("304", 304, "0-1;0-2;0-3;1-2;1-3;2-3");
//        findPattern("305", 305, "0-1;0-3;0-4;1-2;1-5;2-3;2-4;3-5;4-5");
//        // Q2
        findPattern("310", 310, "0-1;0-2;1-3;2-3");
//        // Q3
        findPattern("311", 311, "0-1;0-2;1-3;3-2");
//        // Q4
//        findPattern("301", 301, "0-1;0-2;1-2;1-3;2-3");
//        // Q5
//        findPattern("308", 308, "0-1;0-2;1-2;1-3;3-2");
//        // Q6
//        findPattern("302", 302, "0-1;0-2;0-3;1-2;1-3;2-3");
//        // Q7
//        findPattern("303", 303, "0-1;0-2;0-3;0-4;1-2;1-3;1-4;2-3;2-4;3-4");
        // Q8
//        findPattern("304", 304, "0-1;0-2;1-2;2-3;2-4;3-4");
//        // Q9
//        findPattern("305", 305, "0-1;1-2;2-0;2-3;3-4;4-2;5-1;5-4");
//        // Q10
//        findPattern("306", 306, "0-1;0-2;1-3;2-3;3-4;3-5;4-5");
        // Q12
//        findPattern("309", 309, "0-1;1-2;2-3;3-4;4-5;5-0");
        // Q14
//        findPattern("307", 307, "0-1;0-2;0-3;0-4;0-5;0-6;1-2;1-3;1-4;1-5;1-6;2-3;2-4;2-5;2-6;3-4;3-5;3-6;4-5;4-6;5-6");

    }

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println();

        PatternFinderRunner runner = new CyclicPatternFinderRunner(args[0]);
        runner.run();
    }
}
