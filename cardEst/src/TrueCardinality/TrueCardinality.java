package TrueCardinality;

import Common.Pair;
import Common.Query;
import Common.Topology;
import Common.Util;
import Graphflow.Constants;
import IMDB.Labels;
import TrueCardinality.Parallel.PartialCount;
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
import java.util.stream.Collectors;

public class TrueCardinality {
    Map<Integer, Integer> patternType2pivot = new HashMap<Integer, Integer>() {{
        put(0, 1);
        put(3, 4);
        put(4, 3);
        put(5, 2);
        put(6, 2);
        put(7, 3);
    }};

    Map<String, Pair<String, String>> vListSplit = new HashMap<String, Pair<String, String>>() {{
        put("0-1;1-2;1-3;1-4;1-5;1-6", new Pair<>("1-3;1-2;0-1", "1-4;1-5;1-6"));
        put("0-1;0-4;2-4;3-4;4-5;4-6", new Pair<>("0-4;0-1", "2-4;3-4;4-5;4-6"));
        put("0-1;0-2;0-3;3-6;4-6;5-6", new Pair<>("0-3;0-2;0-1", "3-6;4-6;5-6"));
        put("0-1;1-2;2-5;3-5;4-5;5-6", new Pair<>("1-2;0-1", "2-5;3-5;4-5;5-6"));
        put("0-1;1-2;2-5;3-5;4-5;5-6;5-7", new Pair<>("1-2;0-1", "2-5;3-5;4-5;5-6;5-7"));
        put("0-1;1-2;1-3;3-6;4-6;5-6;6-7;6-8", new Pair<>("1-3;1-2;0-1", "3-6;4-6;5-6;6-7;6-8"));
    }};

    // end index is exclusive
    Map<String, Integer> labelSeqSplitIndex = new HashMap<String, Integer>() {{
        put("0-1;1-2;1-3;1-4;1-5;1-6", 3);
        put("0-1;0-4;2-4;3-4;4-5;4-6", 2);
        put("0-1;0-2;0-3;3-6;4-6;5-6", 3);
        put("0-1;1-2;2-5;3-5;4-5;5-6", 2);
        put("0-1;1-2;2-5;3-5;4-5;5-6;5-7", 2);
        put("0-1;1-2;1-3;3-6;4-6;5-6;6-7;6-8", 3);
    }};

    private Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    private Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();
    private Map<Integer, List<Pair<Integer, Integer>>> label2srcdest = new HashMap<>();

    List<Query> queries;
    Integer[] pivots;
    List<String> vLists;
    List<String> labelSeqs;

    Integer[][] leftVLists;
    Integer[][] rightVLists;
    Integer[][] leftLabelSeqs;
    Integer[][] rightLabelSeqs;

    protected void prepare() {
        StopWatch watch = new StopWatch();
        watch.start();

        leftVLists = new Integer[vLists.size()][];
        rightVLists = new Integer[vLists.size()][];
        leftLabelSeqs = new Integer[labelSeqs.size()][];
        rightLabelSeqs = new Integer[labelSeqs.size()][];

        for (int i = 0; i < vLists.size(); ++i) {
            leftVLists[i] = Util.toVList(vListSplit.get(vLists.get(i)).key);
            rightVLists[i] = Util.toVList(vListSplit.get(vLists.get(i)).value);
        }

        Integer[] labelSeq;
        String[] labelSeqSplit;
        for (int i = 0; i < labelSeqs.size(); ++i) {
            labelSeqSplit = labelSeqs.get(i).split("->");

            // reverse the left half of labelseq
            int leftLen = labelSeqSplitIndex.get(vLists.get(i));
            labelSeq = new Integer[leftLen];
            for (int j = 0; j < leftLen; ++j) {
                labelSeq[j] = Integer.parseInt(labelSeqSplit[leftLen - 1 - j]);
            }

            leftLabelSeqs[i] = labelSeq;

            int rightLen = labelSeqSplit.length - labelSeqSplitIndex.get(vLists.get(i));
            labelSeq = new Integer[rightLen];
            for (int j = 0; j < rightLen; ++j) {
                labelSeq[j] = Integer.parseInt(
                    labelSeqSplit[labelSeqSplitIndex.get(vLists.get(i)) + j]);
            }

            rightLabelSeqs[i] = labelSeq;
        }

        watch.stop();
        System.out.println("Preparing: " + (watch.getTime() / 1000.0) + " sec");
    }

    protected void compute(String destFile) throws Exception {
        StopWatch watch = new StopWatch();
        watch.start();

        List<Thread> threads = new ArrayList<>();
        List<PartialCount> runnables = new ArrayList<>();
        PartialCount partialCount;
        Thread thread;
        int threadId = 0;

        for (int i = 0; i < queries.size(); ++i) {
            List<Integer> starters = getStarters(i);
            int NUM_STARTER_WORKERS = 100;
            if (starters.size() < NUM_STARTER_WORKERS) {
                NUM_STARTER_WORKERS = starters.size();
            }
            for (int j = 0; j < starters.size(); j += starters.size() / NUM_STARTER_WORKERS) {
                threadId++;
                partialCount = new PartialCount(
                    threadId,
                    src2label2dest,
                    dest2label2src,
                    label2srcdest,
                    queries.get(i),
                    pivots[i],
                    leftVLists[i],
                    rightVLists[i],
                    leftLabelSeqs[i],
                    rightLabelSeqs[i],
                    starters.subList(j, Math.min(starters.size(), j + starters.size() / NUM_STARTER_WORKERS))
                );
                runnables.add(partialCount);

                thread = new Thread(partialCount);
                threads.add(thread);
                thread.start();
            }
        }

        int total = threads.size();
        double progress = 0.0;
        for (Thread t : threads) {
            t.join();

            progress += 100.0 / total;
            System.out.print("\rComputing: " + (int) progress + "%");
        }
        watch.stop();
        System.out.println("\rComputing: " + (watch.getTime() / 1000.0) + " sec");

        collect(runnables, destFile);
    }

    private List<Integer> getStarters(int i) {
        Integer leftLabel = leftLabelSeqs[i][0];
        Integer rightLabel = rightLabelSeqs[i][0];
        Integer[] leftVList = leftVLists[i];
        Integer[] rightVList = rightVLists[i];

        int firstDir;
        if (leftVList[0].equals(rightVList[0]) || leftVList[0].equals(rightVList[1])) {
            firstDir = Constants.FORWARD;
        } else {
            firstDir = Constants.BACKWARD;
        }
        int secDir;
        if (rightVList[0].equals(leftVList[0]) || rightVList[0].equals(leftVList[1])) {
            secDir = Constants.FORWARD;
        } else {
            secDir = Constants.BACKWARD;
        }

        return label2srcdest.get(leftLabel).stream()
            .filter(srcDest -> {
                if (firstDir == Constants.FORWARD && secDir == Constants.FORWARD) {
                    return src2label2dest.get(srcDest.key).containsKey(rightLabel);
                } else if (firstDir == Constants.FORWARD && secDir == Constants.BACKWARD) {
                    return dest2label2src.containsKey(srcDest.key) &&
                        dest2label2src.get(srcDest.key).containsKey(rightLabel);
                } else if (firstDir == Constants.BACKWARD && secDir == Constants.FORWARD) {
                    return src2label2dest.containsKey(srcDest.value) &&
                        src2label2dest.get(srcDest.value).containsKey(rightLabel);
                } else {
                    return dest2label2src.get(srcDest.value).containsKey(rightLabel);
                }
            })
            .map(srcDest -> {
                if (firstDir == Constants.FORWARD) {
                    return srcDest.key;
                } else {
                    return srcDest.value;
                }
            })
            .distinct()
            .collect(Collectors.toList());
    }

    private void collect(List<PartialCount> runnables, String destFile) throws Exception {
        StopWatch watch = new StopWatch();
        watch.start();

        Map<String, Long> cardinalities = new HashMap<>();
        for (PartialCount runnable : runnables) {
            String query = runnable.getQuery().toString();
            cardinalities.put(query, cardinalities.getOrDefault(query, 0L) + runnable.getCardinality());
        }

        BufferedWriter writer = new BufferedWriter(new FileWriter(destFile));
        for (Query query : queries) {
            String queryString = query.toString();
            writer.write(queryString + "," + cardinalities.get(queryString) + "\n");
        }
        writer.close();

        watch.stop();
        System.out.println("Saving: " + (watch.getTime() / 1000.0) + " sec");
    }

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

        watch.stop();
        System.out.println("Graph Loading: " + (watch.getTime() / 1000.0) + " sec");

        csvReader.close();
    }

    protected void readQueries(String queryFile) throws Exception {
        StopWatch watch = new StopWatch();
        watch.start();

        queries = new ArrayList<>();
        vLists = new ArrayList<>();
        labelSeqs = new ArrayList<>();

        BufferedReader csvReader = new BufferedReader(new FileReader(queryFile));
        String[] info;
        Integer[] vList, labelSeq;
        List<Integer> listOfPivots = new ArrayList<>();

        String line = csvReader.readLine();
        while (null != line) {
            info = line.split(",");
            vList = Util.toVList(info[1]);
            for (int i = 2; i < info.length; ++i) {
                listOfPivots.add(patternType2pivot.get(Integer.parseInt(info[0])));
                vLists.add(info[1]);
                labelSeqs.add(info[i]);

                labelSeq = Util.toLabelSeq(info[i]);
                addQuery(vList, labelSeq);
            }

            line = csvReader.readLine();
        }
        pivots = listOfPivots.toArray(new Integer[listOfPivots.size()]);

        csvReader.close();

        watch.stop();
        System.out.println("Query Loading: " + (watch.getTime() / 1000.0) + " sec");
    }

    private void addQuery(Integer[] vList, Integer[] labelSeq) {
        Topology topology = new Topology();

        for (int i = 0; i < labelSeq.length; ++i) {
            topology.addEdge(vList[i * 2], labelSeq[i], vList[i * 2 + 1]);
        }

        Query query = new Query(topology);
        queries.add(query);
    }

    TrueCardinality(String graphFile, String queryFile) throws Exception {
        readGraph(graphFile);
        readQueries(queryFile);
    }

    TrueCardinality() {}

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println("queryFile: " + args[1]);
        System.out.println("destFile: " + args[2]);
        System.out.println();

        TrueCardinality trueCardinality = new TrueCardinality(args[0], args[1]);
        trueCardinality.prepare();
        trueCardinality.compute(args[2]);
    }
}
