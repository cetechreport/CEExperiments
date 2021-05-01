package Graphflow.Parallel;

import Common.Topology;
import Graphflow.Constants;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

public class CatalogueConstructionPerVList implements Runnable {
    private int threadId;

    private Map<Integer, Map<Integer, List<Integer>>> src2label2dest;
    private Map<Integer, Map<Integer, List<Integer>>> dest2label2src;
    private Map<Integer, Map<Integer, List<Integer>>> label2src2dest;
    private Map<Integer, Map<Integer, List<Integer>>> label2dest2src;

    String line;

    final int NUM_CHILDREN_THREADS = 5;

    public void run() {
        List<Thread> threads = new ArrayList<>();
        Runnable constructPerLabelSeq;
        Thread thread;
        int id = 0;

        String[] info = line.split(",");
        Integer queryType = Integer.parseInt(info[0]);

        String[] labelSeqList;
        String vList = info[1];
        for (int i = 2; i < info.length; i += NUM_CHILDREN_THREADS) {
            labelSeqList =
                Arrays.copyOfRange(info, i, Math.min(i + NUM_CHILDREN_THREADS, info.length));
            StringJoiner sj = new StringJoiner(",");
            sj.add(queryType.toString());
            sj.add(vList);
            sj.add(String.join(",", labelSeqList));

            id++;
            constructPerLabelSeq = new CatalogueConstructionForLabelSeq(
                id, threadId, src2label2dest, dest2label2src, label2src2dest, label2dest2src, sj.toString());
            thread = new Thread(constructPerLabelSeq);
            threads.add(thread);
            thread.start();
        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public CatalogueConstructionPerVList(
        int threadId,
        Map<Integer, Map<Integer, List<Integer>>> src2label2dest,
        Map<Integer, Map<Integer, List<Integer>>> dest2label2src,
        Map<Integer, Map<Integer, List<Integer>>> label2src2dest,
        Map<Integer, Map<Integer, List<Integer>>> label2dest2src,
        String line) {

        this.threadId = threadId;
        this.src2label2dest = src2label2dest;
        this.dest2label2src = dest2label2src;
        this.label2src2dest = label2src2dest;
        this.label2dest2src = label2dest2src;
        this.line = line;
    }
}
