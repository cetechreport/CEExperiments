package TrueCardinality;

import Common.Pair;
import Common.Query;
import Common.Util;
import TrueCardinality.Parallel.CyclicCardinalityCounter;
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

public class CyclicTrueCardinality {
    Map<Integer, List<Pair<Integer, Integer>>> label2srcdest = new HashMap<>();
    Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();

    int patternType;
    List<Pair<String, String>> patterns = new ArrayList<>();

    protected void readGraph(String graphFile) throws Exception {
        StopWatch watch = new StopWatch();
        watch.start();

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

            label2srcdest.putIfAbsent(line[1], new ArrayList<>());
            label2srcdest.get(line[1]).add(new Pair<>(line[0], line[2]));

            tripleString = csvReader.readLine();
        }

        csvReader.close();

        watch.stop();
        System.out.println("Graph Loading: " + (watch.getTime() / 1000.0) + " sec");
    }

    public void readPatterns(String patternFile) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(patternFile));
        String line = reader.readLine();
        while (line != null) {
            String[] info = line.split(",");
            patternType = Integer.parseInt(info[0]);
            String vList = info[1];
            for (int i = 2; i < info.length; ++i) {
                patterns.add(new Pair<>(vList, info[i]));
            }
            line = reader.readLine();
        }
        reader.close();
    }

    public void prepare(String graphFile, String patternFile) throws Exception {
        readGraph(graphFile);
        readPatterns(patternFile);
    }

    public List<Long> evaluate() throws Exception {
        StopWatch watch = new StopWatch();
        watch.start();

        Thread thread;
        List<Thread> threads = new ArrayList<>();
        CyclicCardinalityCounter runnable;
        List<CyclicCardinalityCounter> counters = new ArrayList<>();

        for (Pair<String, String> vListAndLabelSeq : patterns) {
            runnable = new CyclicCardinalityCounter(
                patternType, label2srcdest, src2label2dest, dest2label2src, vListAndLabelSeq);
            counters.add(runnable);

            thread = new Thread(runnable);
            threads.add(thread);
            thread.start();
        }

        int numPatterns = patterns.size();
        double progress = 0;
        for (Thread t : threads) {
            t.join();

            progress += 100.0 / numPatterns;
            System.out.print("\rCounting: " + (int) progress + "%");
        }

        List<Long> cardinalities = new ArrayList<>();
        for (CyclicCardinalityCounter counter : counters) {
            cardinalities.add(counter.getCardinality());
        }

        watch.stop();
        System.out.println("\rCounting: " + (watch.getTime() / 1000.0) + " sec");

        return cardinalities;
    }

    public void persist(String destFile, List<Long> cardinalities) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(destFile));
        for (int i = 0; i < patterns.size(); ++i) {
            writer.write(patterns.get(i).toString() + "," + cardinalities.get(i) + "\n");
        }
        writer.close();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println("patternFile: " + args[1]);
        System.out.println("destFile: " + args[2]);
        System.out.println();

        CyclicTrueCardinality trueCardinality = new CyclicTrueCardinality();
        trueCardinality.prepare(args[0], args[1]);
        List<Long> cardinalities = trueCardinality.evaluate();
        trueCardinality.persist(args[2], cardinalities);
    }
}
