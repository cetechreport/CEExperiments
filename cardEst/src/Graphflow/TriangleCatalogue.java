package Graphflow;

import Common.Pair;
import Common.Query;
import Common.QueryDecomposer;
import Common.Triple;
import Common.Util;
import Graphflow.Parallel.TriangleComputation;
import Graphflow.Parallel.TwoPathComputation;
import org.apache.commons.lang3.time.StopWatch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.Random;

import static java.lang.Math.*;

public class TriangleCatalogue {
    Map<Integer, List<Pair<Integer, Integer>>> label2srcdest = new HashMap<>();
    Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();

    // vertex -> List of (neighbour vertex, edgeLabel)
    Map<Integer, List<Pair<Integer, Integer>>> neighbours = new HashMap<>();

    public static Map<String, Map<String, Double>> catalogue = new HashMap<>();
    Map<String, Map<String, Map<String, Map<String, Integer>>>> catMaxDeg = new HashMap<>();

    final int NUM_WORKERS = 32;

    public Set<String> findTriangles(String vListString) {
        Set<String> triangles = new HashSet<>();

        Integer[] vList = Util.toVList(vListString);
        for (int i = 0; i < vList.length; i += 2) {
            for (int j = i + 2; j < vList.length; j += 2) {
                for (int k = j + 2; k < vList.length; k += 2) {
                    Set<Integer> covered = new HashSet<>();
                    covered.add(vList[i]);
                    covered.add(vList[i + 1]);
                    if (covered.contains(vList[j]) || covered.contains(vList[j + 1])) {
                        covered.add(vList[j]);
                        covered.add(vList[j + 1]);
                        if (covered.contains(vList[k]) && covered.contains(vList[k + 1])) {
                            StringJoiner triangle = new StringJoiner(";");
                            triangle.add(vList[i] + "-" + vList[i + 1])
                                    .add(vList[j] + "-" + vList[j + 1])
                                    .add(vList[k] + "-" + vList[k + 1]);
                            triangles.add(triangle.toString());
                        }
                    }
                }
            }
        }

        return triangles;
    }

    public long countTriangles(String vListString, String labelSeqString) {
        Integer[] vList = Util.toVList(vListString);
        Integer[] labelSeq = Util.toLabelSeq(labelSeqString);
        String[] vListEdges = vListString.split(";");

        Map<Integer, Map<Integer, List<Integer>>> src2intersect;
        Map<Integer, Map<Integer, List<Integer>>> dest2intersect;
        Integer src2intersectLabel, dest2intersectLabel;

        if (vList[2].equals(vList[0]) || vList[3].equals(vList[0])) {
            src2intersectLabel = labelSeq[1];
            dest2intersectLabel = labelSeq[2];
            if (vList[2].equals(vList[0])) {
                src2intersect = src2label2dest;
            } else {
                src2intersect = dest2label2src;
            }

            if (vList[4].equals(vList[1])) {
                dest2intersect = src2label2dest;
            } else {
                dest2intersect = dest2label2src;
            }
        } else if (vList[2].equals(vList[1]) || vList[3].equals(vList[1])) {
            src2intersectLabel = labelSeq[2];
            dest2intersectLabel = labelSeq[1];
            if (vList[2].equals(vList[1])) {
                dest2intersect = src2label2dest;
            } else {
                dest2intersect = dest2label2src;
            }

            if (vList[4].equals(vList[0])) {
                src2intersect = src2label2dest;
            } else {
                src2intersect = dest2label2src;
            }
        } else {
            System.err.println("ERROR: not a triangle");
            System.err.println("   vList: " + vListString);
            return -1;
        }

        int label1MaxDeg = Integer.MIN_VALUE;
        long totalCount = 0L;
        for (Pair<Integer, Integer> srcdest : label2srcdest.get(labelSeq[0])) {
            if (!src2intersect.containsKey(srcdest.key)) continue;
            if (!src2intersect.get(srcdest.key).containsKey(src2intersectLabel)) continue;
            if (!dest2intersect.containsKey(srcdest.value)) continue;
            if (!dest2intersect.get(srcdest.value).containsKey(dest2intersectLabel)) continue;

            int numIntersection = getNumIntersection(
                    src2intersect.get(srcdest.key).get(src2intersectLabel),
                    dest2intersect.get(srcdest.value).get(dest2intersectLabel)
            );
            totalCount += numIntersection;

            label1MaxDeg = Math.max(label1MaxDeg, numIntersection);
        }

        Pair<String, String> sorted =
                Util.sort(new Pair<>(vListEdges[1] + ";" + vListEdges[2], labelSeq[1] + "->" + labelSeq[2]));
        addCatMaxDeg(vListEdges[0], labelSeq[0].toString(), sorted.key, sorted.value, label1MaxDeg);

        return totalCount;
    }

    public long count2Path(String vListString, String labelSeqString) {
        Integer[] vList = Util.toVList(vListString);
        Integer[] labelSeq = Util.toLabelSeq(labelSeqString);
        String[] vListEdges = vListString.split(";");

        if (vList.length == 2) { // single label
            return label2srcdest.get(labelSeq[0]).size();
        }

        Map<Integer, Map<Integer, List<Integer>>> mid2rightLeaf;
        Integer midPhysical;

        int maxDeg = Integer.MIN_VALUE;
        long totalCard = 0L;
        for (Pair<Integer, Integer> srcdest : label2srcdest.get(labelSeq[0])) {
            if (vList[0].equals(vList[2])) {
                mid2rightLeaf = src2label2dest;
                midPhysical = srcdest.key;
            } else if (vList[0].equals(vList[3])) {
                mid2rightLeaf = dest2label2src;
                midPhysical = srcdest.key;
            } else if (vList[1].equals(vList[2])) {
                mid2rightLeaf = src2label2dest;
                midPhysical = srcdest.value;
            } else if (vList[1].equals(vList[3])) {
                mid2rightLeaf = dest2label2src;
                midPhysical = srcdest.value;
            } else {
                System.err.println("ERROR: unrecognized 2-path vList");
                System.out.println("   vList: " + vListString);
                return -1;
            }

            if (!mid2rightLeaf.containsKey(midPhysical)) continue;
            if (!mid2rightLeaf.get(midPhysical).containsKey(labelSeq[1])) continue;

            totalCard += mid2rightLeaf.get(midPhysical).get(labelSeq[1]).size();

            maxDeg = Math.max(maxDeg, mid2rightLeaf.get(midPhysical).get(labelSeq[1]).size());
        }

        addCatMaxDeg(vListEdges[0], labelSeq[0].toString(), vListEdges[1], labelSeq[1].toString(), maxDeg);

        return totalCard;
    }

    public int submitTriangleTask(
            CompletionService<Triple<String, String, String>> service,
            String runType,
            String sortedTriVList, String sortedTriLabelSeq, String triangleVList, String triangleLabelSeq) {

        int submitted = 0;

        Integer[] labelSeq = Util.toLabelSeq(triangleLabelSeq);
        List<Pair<Integer, Integer>> allSrcdest = label2srcdest.get(labelSeq[0]);
        int numWorkers = Math.min(allSrcdest.size(), NUM_WORKERS);
        for (int i = 0; i < allSrcdest.size(); i += allSrcdest.size() / numWorkers) {
            TriangleComputation task = new TriangleComputation(
                    runType, sortedTriVList, sortedTriLabelSeq, triangleVList, triangleLabelSeq,
                    src2label2dest, dest2label2src,
                    allSrcdest.subList(i, Math.min(allSrcdest.size(), i + allSrcdest.size() / numWorkers)), "");
            service.submit(task);
            ++submitted;
        }

        return submitted;
    }

    public int submitAdditionalTriangleTask(
            CompletionService<Triple<String, String, String>> service,
            String runType,
            String sortedTriVList, String trig, String sortedTriLabelSeq, String triangleVList, String triangleLabelSeq) {

        int submitted = 0;

        Integer[] labelSeq = Util.toLabelSeq(triangleLabelSeq);
        List<Pair<Integer, Integer>> allSrcdest = label2srcdest.get(labelSeq[0]);
        int numWorkers = Math.min(allSrcdest.size(), NUM_WORKERS);
        for (int i = 0; i < allSrcdest.size(); i += allSrcdest.size() / numWorkers) {
            TriangleComputation task = new TriangleComputation(
                    runType, sortedTriVList, sortedTriLabelSeq, triangleVList, triangleLabelSeq,
                    src2label2dest, dest2label2src,
                    allSrcdest.subList(i, Math.min(allSrcdest.size(), i + allSrcdest.size() / numWorkers)), trig);
            service.submit(task);
            ++submitted;
        }

        return submitted;
    }

    public int submitTwoPathTask(
            CompletionService<Triple<String, String, String>> service,
            String runType,
            String sortedVList, String sortedLabelSeq, String vListString, String labelSeqString) {

        int submitted = 0;

        Integer[] labelSeq = Util.toLabelSeq(labelSeqString);
        List<Pair<Integer, Integer>> allSrcdest = label2srcdest.get(labelSeq[0]);
        int numWorkers = Math.min(allSrcdest.size(), NUM_WORKERS);
        for (int i = 0; i < allSrcdest.size(); i += allSrcdest.size() / numWorkers) {
            TwoPathComputation task = new TwoPathComputation(
                    runType, sortedVList, sortedLabelSeq, vListString, labelSeqString,
                    src2label2dest, dest2label2src, label2srcdest,
                    allSrcdest.subList(i, Math.min(allSrcdest.size(), i + allSrcdest.size() / numWorkers)), "");
            service.submit(task);
            ++submitted;
        }

        return submitted;
    }

    public int submitAdditionalTwoPathTask(
            CompletionService<Triple<String, String, String>> service,
            String runType,
            String sortedVList, String twoPath, String sortedLabelSeq, String vListString, String labelSeqString) {

        int submitted = 0;

        Integer[] labelSeq = Util.toLabelSeq(labelSeqString);
        List<Pair<Integer, Integer>> allSrcdest = label2srcdest.get(labelSeq[0]);
        int numWorkers = Math.min(allSrcdest.size(), NUM_WORKERS);
        for (int i = 0; i < allSrcdest.size(); i += allSrcdest.size() / numWorkers) {
            TwoPathComputation task = new TwoPathComputation(
                    runType, sortedVList, sortedLabelSeq, vListString, labelSeqString,
                    src2label2dest, dest2label2src, label2srcdest,
                    allSrcdest.subList(i, Math.min(allSrcdest.size(), i + allSrcdest.size() / numWorkers)), twoPath);
            service.submit(task);
            ++submitted;
        }

        return submitted;
    }


    private boolean isPath(Integer[] vList) {
        if (vList.length != 6) return false;
        Map<Integer, Integer> occurrences = new HashMap<>();
        for (Integer v : vList) {
            occurrences.put(v, occurrences.getOrDefault(v, 0) + 1);
        }

        int notOne = 0;
        for (Integer vertex : occurrences.keySet()) {
            if (occurrences.get(vertex) != 1) ++notOne;
        }

        return notOne != 1;
    }

    private String[] pathToTrig(Integer[] vList, String vListStr) {
        Map<Integer, Integer> occurrences = new HashMap<>();
        for (Integer v : vList) {
            occurrences.put(v, occurrences.getOrDefault(v, 0) + 1);
        }
        int v1 = -1;
        int v2 = -1;
        for (Integer vertex : occurrences.keySet()) {
            if (occurrences.get(vertex) == 1) {
                if (v1 == -1) v1 = vertex;
                else v2 = vertex;
            }
        }
        String[] vListStrArr = vListStr.split(";");
        String twoPath = "";
        String trig = "";
        String twoEdges = "";
        for (String edge : vListStrArr) {
            int v_1 = Integer.parseInt(edge.split("-")[0]);
            int v_2 = Integer.parseInt(edge.split("-")[1]);

            if (v_1 == v2) v_1 = v1;
            else if (v_2 == v2) v_2 = v1;
            trig += v_1 + "-" + v_2 + ";";
            if (occurrences.get(v_1) == 1 || occurrences.get(v_2) == 1) {
                twoEdges += edge + ";";
                twoPath += v_1 + "-" + v_2 + ";";
            }
        }
        String[] strs = new String[3];
        strs[0] = trig.substring(0, trig.length() - 1);
        strs[1] = twoPath.substring(0, twoPath.length() - 1);
        strs[2] = twoEdges.substring(0, twoEdges.length() - 1);
        return strs;
    }

    public void construct(String graphFile, String queryFile)
            throws Exception {

        Integer patternType = 228;
        readGraph(graphFile);

        StopWatch watch = new StopWatch();
        watch.start();

        List<Query> queries = Query.readQueries(queryFile);
        String queryVList = queries.get(0).toString().split(",")[0];
        Set<String> triangleVLists = findTriangles(queryVList);

        QueryDecomposer decomposer = new QueryDecomposer();
        decomposer.decompose(queries.get(0), 2);
//        Set<String> decoms = decomposer.decompositions.keySet();

        // set up executors
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_WORKERS);
        CompletionService<Triple<String, String, String>> service =
                new ExecutorCompletionService<>(executorService);

        // get all the entries we need
        Map<String, Set<String>> tasks = new HashMap<>();
        for (Query query : queries) {
            String queryLabelSeq = query.toString().split(",")[1];
            for (String triangleVList : triangleVLists) {
                String triangleLabelSeq = Util.extractLabelSeq(triangleVList, queryVList, queryLabelSeq);
                tasks.putIfAbsent(triangleVList, new HashSet<>());
                tasks.get(triangleVList).add(triangleLabelSeq);
            }
        }

        watch.stop();
        System.out.println("\rConstructing: " + (watch.getTime() / 1000.0) + " sec");

        watch = new StopWatch();
        watch.start();
        BufferedWriter writer = new BufferedWriter(new FileWriter("decom.csv"));

        for (String vList : tasks.keySet()) {
            for (String labelSeq : tasks.get(vList)) {
                writer.write(patternType + "," + vList + "," + labelSeq + "\n");

                Pair<String, String> rotate1 = rotate(vList, labelSeq);
                writer.write(patternType + "," + rotate1.key + "," + rotate1.value + "\n");

                Pair<String, String> rotate2 = rotate(rotate1.key, rotate1.value);
                writer.write(patternType + "," + rotate2.key + "," + rotate2.value + "\n");

            }
        }
        writer.close();

        watch.stop();
        System.out.println("Saving: " + (watch.getTime() / 1000.0) + " sec");

    }

    public void constructAcyclic(String graphFile, String queryFile, String catFile, String tmpCatFile)
            throws Exception {

        Integer patternType = 228;
        readGraph(graphFile);

        StopWatch watch = new StopWatch();
        watch.start();

        List<Query> queries = Query.readQueries(queryFile);
        String queryVList = queries.get(0).toString().split(",")[0];
        Set<String> triangleVLists = findTriangles(queryVList);

        QueryDecomposer decomposer = new QueryDecomposer();
        decomposer.decompose(queries.get(0), 2);

        // set up executors
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_WORKERS);
        CompletionService<Triple<String, String, String>> service =
                new ExecutorCompletionService<>(executorService);

        // get all the entries we need
        Map<String, Set<String>> tasks = new HashMap<>();
        for (Query query : queries) {
            String queryLabelSeq = query.toString().split(",")[1];
            for (String triangleVList : triangleVLists) {
                String triangleLabelSeq = Util.extractLabelSeq(triangleVList, queryVList, queryLabelSeq);
                tasks.putIfAbsent(triangleVList, new HashSet<>());
                tasks.get(triangleVList).add(triangleLabelSeq);
            }
        }

        watch.stop();
        System.out.println("\rConstructing: " + (watch.getTime() / 1000.0) + " sec");


        Map<String, Map<String, Double>> tmpCat = new HashMap<>();

        BufferedReader reader = new BufferedReader(new FileReader(tmpCatFile));
        String line = reader.readLine();
        while (line != null) {
            String[] info = line.split(",");
            tmpCat.putIfAbsent(info[1], new HashMap<>());
            tmpCat.get(info[1]).put(info[2], Double.parseDouble(info[3]));
            line = reader.readLine();
        }
        reader.close();


        watch = new StopWatch();
        watch.start();

        BufferedWriter writer = new BufferedWriter(new FileWriter(catFile));

        for (String vList : tasks.keySet()) {
            for (String labelSeq : tasks.get(vList)) {
                Double count1 = tmpCat.get(vList).get(labelSeq);

                Pair<String, String> rotate1 = rotate(vList, labelSeq);
                Double count2 = tmpCat.get(rotate1.key).get(rotate1.value);

                Pair<String, String> rotate2 = rotate(rotate1.key, rotate1.value);
                Double count3 = tmpCat.get(rotate2.key).get(rotate2.value);

                writer.write(patternType + "," + vList + "," + labelSeq + "," + min(min(count1, count2), count3) + "\n");
            }
        }
        writer.close();

        watch.stop();
        System.out.println("Saving: " + (watch.getTime() / 1000.0) + " sec");

    }

    private Integer[] toLabelSeq(String labelSeqString) {
        String[] splitted = labelSeqString.split("->");
        Integer[] labelSeq = new Integer[splitted.length];
        for (int i = 0; i < splitted.length; ++i) {
            labelSeq[i] = Integer.parseInt(splitted[i]);
        }
        return labelSeq;
    }

    public static Integer[] toVList(String vListString) {
        String[] splitted = vListString.split(";");
        Integer[] vList = new Integer[splitted.length * 2];
        for (int i = 0; i < splitted.length; i++) {
            String[] srcDest = splitted[i].split("-");
            vList[i * 2] = Integer.parseInt(srcDest[0]);
            vList[i * 2 + 1] = Integer.parseInt(srcDest[1]);
        }
        return vList;
    }

    public void construct2(String graphFile, String queryFile, String catFile, String catMaxDegFile)
            throws Exception {

        Integer patternType = 228;
        readGraph(graphFile);

        StopWatch watch = new StopWatch();
        watch.start();

        List<Query> queries = Query.readQueries(queryFile);
        String queryVList = queries.get(0).toString().split(",")[0];
        Set<String> triangleVLists = findTriangles(queryVList);

        QueryDecomposer decomposer = new QueryDecomposer();
        decomposer.decompose(queries.get(0), 3);
        Set<String> decoms = decomposer.decompositions.keySet();

        // set up executors
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_WORKERS);
        CompletionService<Triple<String, String, String>> service =
                new ExecutorCompletionService<>(executorService);

        // get all the entries we need
        Map<String, Set<String>> tasks = new HashMap<>();
        Map<String, Pair<String, Set<String>>> tasksAddition = new HashMap<>();
        for (Query query : queries) {
            String queryLabelSeq = query.toString().split(",")[1];
            for (String triangleVList : triangleVLists) {
                String triangleLabelSeq = Util.extractLabelSeq(triangleVList, queryVList, queryLabelSeq);
                tasks.putIfAbsent(triangleVList, new HashSet<>());
                tasks.get(triangleVList).add(triangleLabelSeq);
            }
            for (String pathVList : decoms) {
                String[] VListStr = pathVList.split(";");
                ArrayList<Integer> vArrList = new ArrayList<Integer>();
                for (String s : VListStr) {
                    String[] edge = s.split("-");
                    vArrList.add(Integer.parseInt(edge[0]));
                    vArrList.add(Integer.parseInt(edge[1]));
                }
                Integer[] vList = vArrList.toArray(new Integer[0]);
                if (!isPath(vList)) continue;
                String[] strs = pathToTrig(vList, pathVList);
                String trig = strs[0];
                String pathLabelSeq = Util.extractLabelSeq(pathVList, queryVList, queryLabelSeq);
                Pair<String, Set<String>> p = new Pair<>(trig, new HashSet<>());
                tasksAddition.putIfAbsent(pathVList, p);
                tasksAddition.get(pathVList).value.add(pathLabelSeq);
                pathLabelSeq = Util.extractLabelSeq(strs[2], queryVList, queryLabelSeq);
                p = new Pair<>(strs[1], new HashSet<>());
                tasksAddition.putIfAbsent(strs[2], p);
                tasksAddition.get(strs[2]).value.add(pathLabelSeq);
            }
        }

        // submitting to computing service
        int totalSubmitted = 0;
        for (String entryVList : tasks.keySet()) {
            if (entryVList.split(";").length == 3) {
                for (String entryLabelSeq : tasks.get(entryVList)) {
                    totalSubmitted += submitTriangleTask(
                            service, "Triangle1", entryVList, entryLabelSeq, entryVList, entryLabelSeq);
                    Pair<String, String> rotate1 = rotate(entryVList, entryLabelSeq);
                    totalSubmitted += submitTriangleTask(
                            service, "Triangle2", entryVList, entryLabelSeq, rotate1.key, rotate1.value);
                    Pair<String, String> rotate2 = rotate(rotate1.key, rotate1.value);
                    totalSubmitted += submitTriangleTask(
                            service, "Triangle3", entryVList, entryLabelSeq, rotate2.key, rotate2.value);
                }
            } else {
                for (String entryLabelSeq : tasks.get(entryVList)) {
                    totalSubmitted += submitTwoPathTask(
                            service, "Path1", entryVList, entryLabelSeq, entryVList, entryLabelSeq);
                    Pair<String, String> rotate1 = rotate(entryVList, entryLabelSeq);
                    totalSubmitted += submitTwoPathTask(
                            service, "Path2", entryVList, entryLabelSeq, rotate1.key, rotate1.value);
                }
            }
        }
        for (String entryVList : tasksAddition.keySet()) {
            if (entryVList.split(";").length == 3) {
                String trig = tasksAddition.get(entryVList).key;
                for (String entryLabelSeq : tasksAddition.get(entryVList).value) {
                    totalSubmitted += submitAdditionalTriangleTask(
                            service, "Triangle1", entryVList, trig, entryLabelSeq, entryVList, entryLabelSeq);
                    Pair<String, String> rotate1 = rotate(entryVList, entryLabelSeq);
                    Pair<String, String> trig1 = rotate(trig, entryLabelSeq);
                    totalSubmitted += submitAdditionalTriangleTask(
                            service, "Triangle2", entryVList, trig1.key, entryLabelSeq, rotate1.key, rotate1.value);
                    Pair<String, String> rotate2 = rotate(rotate1.key, rotate1.value);
                    Pair<String, String> trig2 = rotate(trig1.key, entryLabelSeq);
                    totalSubmitted += submitAdditionalTriangleTask(
                            service, "Triangle3", entryVList, trig2.key, entryLabelSeq, rotate2.key, rotate2.value);
                }
            } else {
                String twoPath = tasksAddition.get(entryVList).key;
                for (String entryLabelSeq : tasksAddition.get(entryVList).value) {
                    totalSubmitted += submitAdditionalTwoPathTask(
                            service, "Path1", entryVList, twoPath, entryLabelSeq, entryVList, entryLabelSeq);
                    Pair<String, String> rotate1 = rotate(entryVList, entryLabelSeq);
                    Pair<String, String> rotate2 = rotate(twoPath, entryLabelSeq);
                    totalSubmitted += submitAdditionalTwoPathTask(
                            service, "Path2", entryVList, rotate2.key, entryLabelSeq, rotate1.key, rotate1.value);
                }
            }
        }

        collect(service, totalSubmitted);
        executorService.shutdown();

        watch.stop();
        System.out.println("\rConstructing: " + (watch.getTime() / 1000.0) + " sec");

        watch = new StopWatch();
        watch.start();
        BufferedWriter writer = new BufferedWriter(new FileWriter(catFile));
        for (String vList : catalogue.keySet()) {
            for (String labelSeq : catalogue.get(vList).keySet()) {
                Double count = catalogue.get(vList).get(labelSeq);
                writer.write(patternType + "," + vList + "," + labelSeq + "," + count + "\n");
            }
        }
        writer.close();

        writer = new BufferedWriter(new FileWriter(catMaxDegFile));
        for (String baseVList : catMaxDeg.keySet()) {
            for (String baseLabelSeq : catMaxDeg.get(baseVList).keySet()) {
                for (String extVList : catMaxDeg.get(baseVList).get(baseLabelSeq).keySet()) {
                    for (String extLabelSeq : catMaxDeg.get(baseVList).get(baseLabelSeq).get(extVList).keySet()) {
                        Integer maxDeg = catMaxDeg.get(baseVList).get(baseLabelSeq).get(extVList).get(extLabelSeq);
                        StringJoiner sj = new StringJoiner(",");
                        sj.add(patternType.toString()).add(baseVList).add(baseLabelSeq);
                        sj.add(extVList).add(extLabelSeq).add(maxDeg.toString());
                        writer.write(sj.toString() + "\n");
                    }
                }
            }
        }
        writer.close();
        watch.stop();
        System.out.println("Saving: " + (watch.getTime() / 1000.0) + " sec");
    }

    public void construct3(String graphFile, String queryFile, String catFile, String catMaxDegFile)
            throws Exception {

        Integer patternType = 228;
        readGraph(graphFile);

        StopWatch watch = new StopWatch();
        watch.start();

        List<Query> queries = Query.readQueries(queryFile);
        String queryVList = queries.get(0).toString().split(",")[0];
        Set<String> triangleVLists = findTriangles(queryVList);

        QueryDecomposer decomposer = new QueryDecomposer();
        decomposer.decompose(queries.get(0), 3);
        Set<String> decoms = decomposer.decompositions.keySet();

        // set up executors
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_WORKERS);
        CompletionService<Triple<String, String, String>> service =
                new ExecutorCompletionService<>(executorService);

        // get all the entries we need
        Map<String, Set<String>> tasks = new HashMap<>();
        for (Query query : queries) {
            String queryLabelSeq = query.toString().split(",")[1];
            for (String triangleVList : triangleVLists) {
                String triangleLabelSeq = Util.extractLabelSeq(triangleVList, queryVList, queryLabelSeq);
                tasks.putIfAbsent(triangleVList, new HashSet<>());
                tasks.get(triangleVList).add(triangleLabelSeq);
            }
        }

        // submitting to computing service
        int totalSubmitted = 0;
        for (String entryVList : tasks.keySet()) {
            if (entryVList.split(";").length == 3) {
                for (String entryLabelSeq : tasks.get(entryVList)) {
                    totalSubmitted += submitTriangleTask(
                            service, "Triangle1", entryVList, entryLabelSeq, entryVList, entryLabelSeq);
                    Pair<String, String> rotate1 = rotate(entryVList, entryLabelSeq);
                    totalSubmitted += submitTriangleTask(
                            service, "Triangle2", entryVList, entryLabelSeq, rotate1.key, rotate1.value);
                    Pair<String, String> rotate2 = rotate(rotate1.key, rotate1.value);
                    totalSubmitted += submitTriangleTask(
                            service, "Triangle3", entryVList, entryLabelSeq, rotate2.key, rotate2.value);
                }
            } else {
                for (String entryLabelSeq : tasks.get(entryVList)) {
                    totalSubmitted += submitTwoPathTask(
                            service, "Path1", entryVList, entryLabelSeq, entryVList, entryLabelSeq);
                    Pair<String, String> rotate1 = rotate(entryVList, entryLabelSeq);
                    totalSubmitted += submitTwoPathTask(
                            service, "Path2", entryVList, entryLabelSeq, rotate1.key, rotate1.value);
                }
            }
        }
        collect(service, totalSubmitted);
        executorService.shutdown();

        watch.stop();
        System.out.println("\rConstructing: " + (watch.getTime() / 1000.0) + " sec");

        watch = new StopWatch();
        watch.start();
        BufferedWriter writer = new BufferedWriter(new FileWriter(catFile));
        for (String vList : catalogue.keySet()) {
            for (String labelSeq : catalogue.get(vList).keySet()) {
                Double count = catalogue.get(vList).get(labelSeq);
                writer.write(patternType + "," + vList + "," + labelSeq + "," + count + "\n");
            }
        }

        for (String path3String : decoms) {
            // filter from decoms all the 3-paths and calculate their corresponding extension rates
            Map<Integer, Integer> occurrences = new HashMap<>();
            Integer[] vList = toVList(path3String);
            for (Integer v: vList) {
                occurrences.put(v, occurrences.getOrDefault(v, 0) + 1);
            }
            Integer notOne = 0;
            for (Integer vertex : occurrences.keySet()) {
                if (occurrences.get(vertex) != 1) notOne += 1;
            }
            // if this pattern is a 3-path
            if (vList.length == 6 && notOne != 1) {
                Set<String> queryLabelSeqList = new HashSet<>();
                for (Query query : queries) {
                    String queryLabelSeq = query.toString().split(",")[1];
                    String labelSeqString = Util.extractLabelSeq(path3String, queryVList, queryLabelSeq);
                    queryLabelSeqList.add(labelSeqString);
                }

                for (String labelSeqString : queryLabelSeqList) {

                    writer.write(patternType + "," + path3String + "," + labelSeqString + "*" + "," + getExtensionRate(vList, labelSeqString) + "\n");
                }
            }
        }
        writer.close();

        System.out.println("Finished catalogue construction");

        writer = new BufferedWriter(new FileWriter(catMaxDegFile));
        for (String baseVList : catMaxDeg.keySet()) {
            for (String baseLabelSeq : catMaxDeg.get(baseVList).keySet()) {
                for (String extVList : catMaxDeg.get(baseVList).get(baseLabelSeq).keySet()) {
                    for (String extLabelSeq : catMaxDeg.get(baseVList).get(baseLabelSeq).get(extVList).keySet()) {
                        Integer maxDeg = catMaxDeg.get(baseVList).get(baseLabelSeq).get(extVList).get(extLabelSeq);
                        StringJoiner sj = new StringJoiner(",");
                        sj.add(patternType.toString()).add(baseVList).add(baseLabelSeq);
                        sj.add(extVList).add(extLabelSeq).add(maxDeg.toString());
                        writer.write(sj.toString() + "\n");
                    }
                }
            }
        }
        writer.close();
        watch.stop();
        System.out.println("Saving: " + (watch.getTime() / 1000.0) + " sec");
    }

    private double getExtensionRate(Integer[] vList, String labelSeqString) {
        Integer[] labelSeq = toLabelSeq(labelSeqString);
        // make sure vList and labelSeq are in order
        if (vList[0].intValue() == vList[2].intValue() || vList[0].intValue() == vList[3].intValue() || vList[1].intValue() == vList[2].intValue() || vList[1].intValue() == vList[3].intValue()) {
            if (vList[2].intValue() == vList[4].intValue() || vList[2].intValue() == vList[5].intValue() || vList[3].intValue() == vList[4].intValue() || vList[3].intValue() == vList[5].intValue()) {
            } else {
                // swap vertices
                Integer temp1 = vList[0];
                Integer temp2 = vList[1];
                vList[0] = vList[2];
                vList[1] = vList[3];
                vList[2] = temp1;
                vList[3] = temp2;
                // swap edges
                Integer temp3 = labelSeq[0];
                labelSeq[0] = labelSeq[1];
                labelSeq[1] = temp3;
            }
        } else {
            // swap vertices
            Integer temp1 = vList[2];
            Integer temp2 = vList[3];
            vList[2] = vList[4];
            vList[3] = vList[5];
            vList[4] = temp1;
            vList[5] = temp2;
            // swap edges
            Integer temp3 = labelSeq[1];
            labelSeq[1] = labelSeq[2];
            labelSeq[2] = temp3;
        }

        Long count = 0L;
        Long cycleCount = 0L;
        Integer startDirection = 0; // 0 means v0 is the src, 1 means v0 is the dest
        Map<Integer, Map<Integer, List<Integer>>> finishList = new HashMap<>();
        Integer startLabel = labelSeq[0];
        Integer finishLabel = labelSeq[2];

        // System.out.println(Arrays.toString(vList));

        // v1 <- v0 -> v2 -> v3
        if (vList[0].intValue() == vList[2].intValue() && vList[3].intValue() == vList[4].intValue()) {
            finishList = dest2label2src;
        }
        // v1 <- v0 -> v2 <- v3
        else if (vList[0].intValue() == vList[2].intValue() && vList[3].intValue() == vList[5].intValue()) {
            finishList = src2label2dest;
        }
        // v0 -> v1 -> v2 -> v3
        else if (vList[1].intValue() == vList[2].intValue() && vList[3].intValue() == vList[4].intValue()) {
            startDirection = 1;
            finishList = dest2label2src;
        }
        // v0 -> v1 -> v2 <- v3
        else if (vList[1].intValue() == vList[2].intValue() && vList[3].intValue() == vList[5].intValue()) {
            startDirection = 1;
            finishList = src2label2dest;
        }
        // v0 -> v1 <- v2 -> v3
        else if (vList[1].intValue() == vList[3].intValue() && vList[2].intValue() == vList[4].intValue()) {
            finishList = src2label2dest;
            startLabel = labelSeq[2];
            finishLabel = labelSeq[0];
        }
        // v0 -> v1 <- v2 <- v3
        else if (vList[1].intValue() == vList[3].intValue() && vList[2].intValue() == vList[5].intValue()) {
            startDirection = 1;
            finishList = src2label2dest;
            startLabel = labelSeq[2];
            finishLabel = labelSeq[0];
        }
        // v1 <- v0 <- v2 -> v3
        else if (vList[0].intValue() == vList[3].intValue() && vList[2].intValue() == vList[4].intValue()) {
            finishList = dest2label2src;
            startLabel = labelSeq[2];
            finishLabel = labelSeq[0];
        }
        // v1 <- v0 <- v2 <- v3
        else if (vList[0].intValue() == vList[3].intValue() && vList[2].intValue() == vList[5].intValue()) {
            startDirection = 1;
            finishList = dest2label2src;
            startLabel = labelSeq[2];
            finishLabel = labelSeq[0];
        }

        long totalSampleSize = 500L;
        int sampleSizePerEdge = 20;

        // System.out.println(path3String);

        // have cycle up to length = 10 in the yago queryset, therefore, use weighted extension rate as below
        // (cycle) : (weight)
        // 4-cycle: 1/7
        // 5-cycle: 1/7
        // 6-cycle: 5/7

        // 4-cycle
        Collections.shuffle(label2srcdest.get(startLabel));
        for (Pair<Integer, Integer> startPair : label2srcdest.get(startLabel)) {
            boolean breakLoop = false;
            boolean nextOption = true;
            int curCount = 0;
            Integer v0 = startPair.key;
            Integer v1 = startPair.value;
            if (startDirection == 1) {
                v0 = startPair.value;
                v1 = startPair.key;
            }

            Collections.shuffle(neighbours.get(v1));
            for (Pair<Integer, Integer> pair2 : neighbours.get(v1)) {
                Integer v2 = pair2.getKey();
                if (finishList.containsKey(v2) && finishList.get(v2).containsKey(finishLabel)) {
                    for (Integer v3 : finishList.get(v2).get(finishLabel)) {
                        curCount += 1;
                        count += 1L;
                        if (src2label2dest.containsKey(v0) && src2label2dest.get(v0).containsKey(labelSeq[1]) && src2label2dest.get(v0).get(labelSeq[1]).contains(v3)) {
                            cycleCount += 1L;
                        }
                        if (count >= totalSampleSize) {
                            breakLoop = true;
                            nextOption = false;
                            break;
                        } else if (curCount >= sampleSizePerEdge) {
                            breakLoop = true;
                            break;
                        }
                    }
                }
                if (breakLoop) break;
            }
            if (nextOption) continue;
            break;
        }

        // System.out.println(cycleCount);
        // System.out.println(count);

        double extensionRate = 0.0;

        if (count != 0) extensionRate = cycleCount * 1.0 / count;

        count = 0L;
        cycleCount = 0L;
        // 5-cycle
        Collections.shuffle(label2srcdest.get(startLabel));
        for (Pair<Integer, Integer> startPair : label2srcdest.get(startLabel)) {
            boolean breakLoop = false;
            boolean nextOption = true;
            Integer curCount = 0;
            Integer v0 = startPair.key;
            Integer v1 = startPair.value;
            if (startDirection == 1) {
                v0 = startPair.value;
                v1 = startPair.key;
            }

            Collections.shuffle(neighbours.get(v1));
            for (Pair<Integer, Integer> pair2 : neighbours.get(v1)) {
                Integer v2 = pair2.getKey();
                Collections.shuffle(neighbours.get(v2));
                for (Pair<Integer, Integer> pair3 : neighbours.get(v2)) {
                    Integer v3 = pair3.getKey();
                    if (finishList.containsKey(v3) && finishList.get(v3).containsKey(finishLabel)) {
                        for (Integer v4 : finishList.get(v3).get(finishLabel)) {
                            curCount += 1;
                            count += 1L;
                            if (src2label2dest.containsKey(v0) && src2label2dest.get(v0).containsKey(labelSeq[1]) && src2label2dest.get(v0).get(labelSeq[1]).contains(v4)) {
                                cycleCount += 1L;
                            }
                            if (count >= totalSampleSize) {
                                breakLoop = true;
                                nextOption = false;
                                break;
                            } else if (curCount >= sampleSizePerEdge) {
                                breakLoop = true;
                                break;
                            }
                        }
                    }
                    if (breakLoop) break;
                }
                if (breakLoop) break;
            }
            if (nextOption) continue;
            break;
        }
        // System.out.println(cycleCount);
        // System.out.println(count);

        if (count != 0) extensionRate += cycleCount * 1.0 / count;

        count = 0L;
        cycleCount = 0L;
        // 6-cycle
        Collections.shuffle(label2srcdest.get(startLabel));
        for (Pair<Integer, Integer> startPair : label2srcdest.get(startLabel)) {
            boolean breakLoop = false;
            boolean nextOption = true;
            Integer curCount = 0;
            Integer v0 = startPair.key;
            Integer v1 = startPair.value;
            if (startDirection == 1) {
                v0 = startPair.value;
                v1 = startPair.key;
            }

            Collections.shuffle(neighbours.get(v1));
            for (Pair<Integer, Integer> pair2 : neighbours.get(v1)) {
                Integer v2 = pair2.getKey();
                Collections.shuffle(neighbours.get(v2));
                for (Pair<Integer, Integer> pair3 : neighbours.get(v2)) {
                    Integer v3 = pair3.getKey();
                    Collections.shuffle(neighbours.get(v3));
                    for (Pair<Integer, Integer> pair4 : neighbours.get(v3)) {
                        Integer v4 = pair4.getKey();
                        if (finishList.containsKey(v4) && finishList.get(v4).containsKey(finishLabel)) {
                            for (Integer v5 : finishList.get(v4).get(finishLabel)) {
                                curCount += 1;
                                count += 1L;
                                if (src2label2dest.containsKey(v0) && src2label2dest.get(v0).containsKey(labelSeq[1]) && src2label2dest.get(v0).get(labelSeq[1]).contains(v5)) {
                                    cycleCount += 1L;
                                }
                                if (count >= totalSampleSize) {
                                    breakLoop = true;
                                    nextOption = false;
                                    break;
                                } else if (curCount >= sampleSizePerEdge) {
                                    breakLoop = true;
                                    break;
                                }
                            }
                        }
                        if (breakLoop) break;
                    }
                    if (breakLoop) break;
                }
                if (breakLoop) break;
            }
            if (nextOption) continue;
            break;
        }
        // System.out.println(cycleCount);
        // System.out.println(count);

        if (count != 0) extensionRate += cycleCount * 5.0 / count;

        extensionRate /= 7.0;

        return extensionRate;
    }

    public void construct4(String graphFile, String queryFile, String catFile, String catMaxDegFile)
            throws Exception {

        Integer patternType = 228;
        readGraph(graphFile);

        StopWatch watch = new StopWatch();
        watch.start();

        List<Query> queries = Query.readQueries(queryFile);
        String queryVList = queries.get(0).toString().split(",")[0];
        Set<String> triangleVLists = findTriangles(queryVList);

        QueryDecomposer decomposer = new QueryDecomposer();
        decomposer.decompose(queries.get(0), 3);
        Set<String> decoms = decomposer.decompositions.keySet();

        // set up executors
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_WORKERS);
        CompletionService<Triple<String, String, String>> service =
                new ExecutorCompletionService<>(executorService);

        // get all the entries we need
        Map<String, Set<String>> tasks = new HashMap<>();
        for (Query query : queries) {
            String queryLabelSeq = query.toString().split(",")[1];
            for (String triangleVList : triangleVLists) {
                String triangleLabelSeq = Util.extractLabelSeq(triangleVList, queryVList, queryLabelSeq);
                tasks.putIfAbsent(triangleVList, new HashSet<>());
                tasks.get(triangleVList).add(triangleLabelSeq);
            }
        }

        BufferedWriter writer = new BufferedWriter(new FileWriter(catFile));
        for (String entryVList : tasks.keySet()) {
            for (String entryLabelSeq : tasks.get(entryVList)) {
                // original triangle
                Integer[] vList = toVList(entryVList);
                double extensionRate = getExtensionRate(vList, entryLabelSeq);
                writer.write(patternType + "," + entryVList + "," + entryLabelSeq + "**" + "," + extensionRate + "\n");

                // rotate clockwise by 120
                Pair<String, String> rotate1 = rotate(entryVList, entryLabelSeq);
                Integer[] vList1 = toVList(rotate1.key);
                double extensionRate1 = getExtensionRate(vList1, rotate1.value);
                writer.write(patternType + "," + entryVList + "," + entryLabelSeq + "*" + "," + extensionRate1 + "\n");

                // rotate clockwise by 240
                Pair<String, String> rotate2 = rotate(rotate1.key, rotate1.value);
                Integer[] vList2 = toVList(rotate2.key);
                double extensionRate2 = getExtensionRate(vList2, rotate2.value);
                writer.write(patternType + "," + entryVList + "," + entryLabelSeq + "***" + "," + extensionRate2 + "\n");
            }
        }

        watch.stop();
        System.out.println("\rConstructing: " + (watch.getTime() / 1000.0) + " sec");

        watch = new StopWatch();
        watch.start();


        for (String path3String : decoms) {
            // filter from decoms all the 3-paths and calculate their corresponding extension rates
            Map<Integer, Integer> occurrences = new HashMap<>();
            Integer[] vList = toVList(path3String);
            for (Integer v: vList) {
                occurrences.put(v, occurrences.getOrDefault(v, 0) + 1);
            }
            Integer notOne = 0;
            for (Integer vertex : occurrences.keySet()) {
                if (occurrences.get(vertex) != 1) notOne += 1;
            }
            // if this pattern is a 3-path
            if (vList.length == 6 && notOne != 1) {
                Set<String> queryLabelSeqList = new HashSet<>();
                for (Query query : queries) {
                    String queryLabelSeq = query.toString().split(",")[1];
                    String labelSeqString = Util.extractLabelSeq(path3String, queryVList, queryLabelSeq);
                    queryLabelSeqList.add(labelSeqString);
                }

                for (String labelSeqString : queryLabelSeqList) {

                    writer.write(patternType + "," + path3String + "," + labelSeqString + "*" + "," + getExtensionRate(vList, labelSeqString) + "\n");
                }
            }
        }
        writer.close();

        System.out.println("Finished catalogue construction");

        writer = new BufferedWriter(new FileWriter(catMaxDegFile));
        for (String baseVList : catMaxDeg.keySet()) {
            for (String baseLabelSeq : catMaxDeg.get(baseVList).keySet()) {
                for (String extVList : catMaxDeg.get(baseVList).get(baseLabelSeq).keySet()) {
                    for (String extLabelSeq : catMaxDeg.get(baseVList).get(baseLabelSeq).get(extVList).keySet()) {
                        Integer maxDeg = catMaxDeg.get(baseVList).get(baseLabelSeq).get(extVList).get(extLabelSeq);
                        StringJoiner sj = new StringJoiner(",");
                        sj.add(patternType.toString()).add(baseVList).add(baseLabelSeq);
                        sj.add(extVList).add(extLabelSeq).add(maxDeg.toString());
                        writer.write(sj.toString() + "\n");
                    }
                }
            }
        }
        writer.close();
        watch.stop();
        System.out.println("Saving: " + (watch.getTime() / 1000.0) + " sec");
    }

    public void construct5(String graphFile, String queryFile, String catFile, String catMaxDegFile)
            throws Exception {

        Integer patternType = 228;
        readGraph(graphFile);

        StopWatch watch = new StopWatch();
        watch.start();

        List<Query> queries = Query.readQueries(queryFile);
        String queryVList = queries.get(0).toString().split(",")[0];
        Set<String> triangleVLists = findTriangles(queryVList);

        QueryDecomposer decomposer = new QueryDecomposer();
        decomposer.decompose(queries.get(0), 3);
        Set<String> decoms = decomposer.decompositions.keySet();

        // set up executors
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_WORKERS);
        CompletionService<Triple<String, String, String>> service =
                new ExecutorCompletionService<>(executorService);

        // get all the entries we need
        Map<String, Set<String>> tasks = new HashMap<>();
        for (Query query : queries) {
            String queryLabelSeq = query.toString().split(",")[1];
            for (String triangleVList : triangleVLists) {
                String triangleLabelSeq = Util.extractLabelSeq(triangleVList, queryVList, queryLabelSeq);
                tasks.putIfAbsent(triangleVList, new HashSet<>());
                tasks.get(triangleVList).add(triangleLabelSeq);
            }
        }

        // submitting to computing service
        int totalSubmitted = 0;
        for (String entryVList : tasks.keySet()) {
            if (entryVList.split(";").length == 3) {
                for (String entryLabelSeq : tasks.get(entryVList)) {
                    totalSubmitted += submitTriangleTask(
                            service, "Triangle1", entryVList, entryLabelSeq, entryVList, entryLabelSeq);
                    Pair<String, String> rotate1 = rotate(entryVList, entryLabelSeq);
                    totalSubmitted += submitTriangleTask(
                            service, "Triangle2", entryVList, entryLabelSeq, rotate1.key, rotate1.value);
                    Pair<String, String> rotate2 = rotate(rotate1.key, rotate1.value);
                    totalSubmitted += submitTriangleTask(
                            service, "Triangle3", entryVList, entryLabelSeq, rotate2.key, rotate2.value);
                }
            }
        }
        collect(service, totalSubmitted);
        executorService.shutdown();

        watch.stop();
        System.out.println("\rConstructing: " + (watch.getTime() / 1000.0) + " sec");

        watch = new StopWatch();
        watch.start();
        BufferedWriter writer = new BufferedWriter(new FileWriter(catFile));
        for (String vList : catalogue.keySet()) {
            for (String labelSeq : catalogue.get(vList).keySet()) {
                Double count = catalogue.get(vList).get(labelSeq);
                writer.write(patternType + "," + vList + "," + labelSeq + "," + count + "\n");
            }
        }


        System.out.println("Finished catalogue construction");
        writer.close();

        writer = new BufferedWriter(new FileWriter(catMaxDegFile));
        for (String baseVList : catMaxDeg.keySet()) {
            for (String baseLabelSeq : catMaxDeg.get(baseVList).keySet()) {
                for (String extVList : catMaxDeg.get(baseVList).get(baseLabelSeq).keySet()) {
                    for (String extLabelSeq : catMaxDeg.get(baseVList).get(baseLabelSeq).get(extVList).keySet()) {
                        Integer maxDeg = catMaxDeg.get(baseVList).get(baseLabelSeq).get(extVList).get(extLabelSeq);
                        StringJoiner sj = new StringJoiner(",");
                        sj.add(patternType.toString()).add(baseVList).add(baseLabelSeq);
                        sj.add(extVList).add(extLabelSeq).add(maxDeg.toString());
                        writer.write(sj.toString() + "\n");
                    }
                }
            }
        }
        writer.close();
        watch.stop();
        System.out.println("Saving: " + (watch.getTime() / 1000.0) + " sec");
    }

    public void construct6(String graphFile, String queryFile, String catFile, String catMaxDegFile)
            throws Exception {
        Integer patternType = 228;
        readGraph(graphFile);

        StopWatch watch = new StopWatch();
        watch.start();

        List<Query> queries = Query.readQueries(queryFile);
        String queryVList = queries.get(0).toString().split(",")[0];
        Set<String> triangleVLists = findTriangles(queryVList);

        QueryDecomposer decomposer = new QueryDecomposer();
        decomposer.decompose(queries.get(0), 3);
        Set<String> decoms = decomposer.decompositions.keySet();

        // set up executors
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_WORKERS);
        CompletionService<Triple<String, String, String>> service =
                new ExecutorCompletionService<>(executorService);

        // get all the entries we need
        Map<String, Set<String>> tasks = new HashMap<>();
        Map<String, Pair<String, Set<String>>> tasksAddition = new HashMap<>();
        for (Query query : queries) {
            String queryLabelSeq = query.toString().split(",")[1];
            for (String triangleVList : triangleVLists) {
                String triangleLabelSeq = Util.extractLabelSeq(triangleVList, queryVList, queryLabelSeq);
                tasks.putIfAbsent(triangleVList, new HashSet<>());
                tasks.get(triangleVList).add(triangleLabelSeq);
            }
            for (String pathVList : decoms) {
                String[] VListStr = pathVList.split(";");
                ArrayList<Integer> vArrList = new ArrayList<Integer>();
                for (String s : VListStr) {
                    String[] edge = s.split("-");
                    vArrList.add(Integer.parseInt(edge[0]));
                    vArrList.add(Integer.parseInt(edge[1]));
                }
                Integer[] vList = vArrList.toArray(new Integer[0]);
                if (!isPath(vList)) continue;
                String[] strs = pathToTrig(vList, pathVList);
                String trig = strs[0];
                String pathLabelSeq = Util.extractLabelSeq(pathVList, queryVList, queryLabelSeq);
                Pair<String, Set<String>> p = new Pair<>(trig, new HashSet<>());
                tasksAddition.putIfAbsent(pathVList, p);
                tasksAddition.get(pathVList).value.add(pathLabelSeq);
                pathLabelSeq = Util.extractLabelSeq(strs[2], queryVList, queryLabelSeq);
                p = new Pair<>(strs[1], new HashSet<>());
                tasksAddition.putIfAbsent(strs[2], p);
                tasksAddition.get(strs[2]).value.add(pathLabelSeq);
            }
        }

        // submitting to computing service
        int totalSubmitted = 0;
        for (String entryVList : tasks.keySet()) {
            if (entryVList.split(";").length == 3) {
                for (String entryLabelSeq : tasks.get(entryVList)) {
                    totalSubmitted += submitTriangleTask(
                            service, "Triangle1", entryVList, entryLabelSeq, entryVList, entryLabelSeq);
                    Pair<String, String> rotate1 = rotate(entryVList, entryLabelSeq);
                    totalSubmitted += submitTriangleTask(
                            service, "Triangle2", entryVList, entryLabelSeq, rotate1.key, rotate1.value);
                    Pair<String, String> rotate2 = rotate(rotate1.key, rotate1.value);
                    totalSubmitted += submitTriangleTask(
                            service, "Triangle3", entryVList, entryLabelSeq, rotate2.key, rotate2.value);
                }
            } else {
                for (String entryLabelSeq : tasks.get(entryVList)) {
                    totalSubmitted += submitTwoPathTask(
                            service, "Path1", entryVList, entryLabelSeq, entryVList, entryLabelSeq);
                    Pair<String, String> rotate1 = rotate(entryVList, entryLabelSeq);
                    totalSubmitted += submitTwoPathTask(
                            service, "Path2", entryVList, entryLabelSeq, rotate1.key, rotate1.value);
                }
            }
        }
        for (String entryVList : tasksAddition.keySet()) {
            if (entryVList.split(";").length == 3) {
                String trig = tasksAddition.get(entryVList).key;
                for (String entryLabelSeq : tasksAddition.get(entryVList).value) {
                    totalSubmitted += submitAdditionalTriangleTask(
                            service, "Triangle1", entryVList, trig, entryLabelSeq, entryVList, entryLabelSeq);
                    Pair<String, String> rotate1 = rotate(entryVList, entryLabelSeq);
                    Pair<String, String> trig1 = rotate(trig, entryLabelSeq);
                    totalSubmitted += submitAdditionalTriangleTask(
                            service, "Triangle2", entryVList, trig1.key, entryLabelSeq, rotate1.key, rotate1.value);
                    Pair<String, String> rotate2 = rotate(rotate1.key, rotate1.value);
                    Pair<String, String> trig2 = rotate(trig1.key, entryLabelSeq);
                    totalSubmitted += submitAdditionalTriangleTask(
                            service, "Triangle3", entryVList, trig2.key, entryLabelSeq, rotate2.key, rotate2.value);
                }
            } else {
                String twoPath = tasksAddition.get(entryVList).key;
                for (String entryLabelSeq : tasksAddition.get(entryVList).value) {
                    totalSubmitted += submitAdditionalTwoPathTask(
                            service, "Path1", entryVList, twoPath, entryLabelSeq, entryVList, entryLabelSeq);
                    Pair<String, String> rotate1 = rotate(entryVList, entryLabelSeq);
                    Pair<String, String> rotate2 = rotate(twoPath, entryLabelSeq);
                    totalSubmitted += submitAdditionalTwoPathTask(
                            service, "Path2", entryVList, rotate2.key, entryLabelSeq, rotate1.key, rotate1.value);
                }
            }
        }

        collect(service, totalSubmitted);
        executorService.shutdown();

        watch.stop();
        System.out.println("\rConstructing: " + (watch.getTime() / 1000.0) + " sec");

        watch = new StopWatch();
        watch.start();
        BufferedWriter writer = new BufferedWriter(new FileWriter(catFile));
        // triangles
        for (String vList : catalogue.keySet()) {
            for (String labelSeq : catalogue.get(vList).keySet()) {
                Double count = catalogue.get(vList).get(labelSeq);
                writer.write(patternType + "," + vList + "," + labelSeq + "," + count + "\n");
            }
        }
        // extension rates for triangles
        for (String entryVList : tasks.keySet()) {
            for (String entryLabelSeq : tasks.get(entryVList)) {
                // original triangle
                Integer[] vList = toVList(entryVList);
                double extensionRate = getExtensionRate(vList, entryLabelSeq);
                writer.write(patternType + "," + entryVList + "," + entryLabelSeq + "**" + "," + extensionRate + "\n");

                // rotate clockwise by 120
                Pair<String, String> rotate1 = rotate(entryVList, entryLabelSeq);
                Integer[] vList1 = toVList(rotate1.key);
                double extensionRate1 = getExtensionRate(vList1, rotate1.value);
                writer.write(patternType + "," + entryVList + "," + entryLabelSeq + "*" + "," + extensionRate1 + "\n");

                // rotate clockwise by 240
                Pair<String, String> rotate2 = rotate(rotate1.key, rotate1.value);
                Integer[] vList2 = toVList(rotate2.key);
                double extensionRate2 = getExtensionRate(vList2, rotate2.value);
                writer.write(patternType + "," + entryVList + "," + entryLabelSeq + "***" + "," + extensionRate2 + "\n");
            }
        }

        // extension rates for 3-paths
        for (String path3String : decoms) {
            // filter from decoms all the 3-paths and calculate their corresponding extension rates
            Map<Integer, Integer> occurrences = new HashMap<>();
            Integer[] vList = toVList(path3String);
            for (Integer v: vList) {
                occurrences.put(v, occurrences.getOrDefault(v, 0) + 1);
            }
            Integer notOne = 0;
            for (Integer vertex : occurrences.keySet()) {
                if (occurrences.get(vertex) != 1) notOne += 1;
            }
            // if this pattern is a 3-path
            if (vList.length == 6 && notOne != 1) {
                Set<String> queryLabelSeqList = new HashSet<>();
                for (Query query : queries) {
                    String queryLabelSeq = query.toString().split(",")[1];
                    String labelSeqString = Util.extractLabelSeq(path3String, queryVList, queryLabelSeq);
                    queryLabelSeqList.add(labelSeqString);
                }

                for (String labelSeqString : queryLabelSeqList) {

                    writer.write(patternType + "," + path3String + "," + labelSeqString + "*" + "," + getExtensionRate(vList, labelSeqString) + "\n");
                }
            }
        }


        System.out.println("Finished catalogue construction");
        writer.close();

        writer = new BufferedWriter(new FileWriter(catMaxDegFile));
        for (String baseVList : catMaxDeg.keySet()) {
            for (String baseLabelSeq : catMaxDeg.get(baseVList).keySet()) {
                for (String extVList : catMaxDeg.get(baseVList).get(baseLabelSeq).keySet()) {
                    for (String extLabelSeq : catMaxDeg.get(baseVList).get(baseLabelSeq).get(extVList).keySet()) {
                        Integer maxDeg = catMaxDeg.get(baseVList).get(baseLabelSeq).get(extVList).get(extLabelSeq);
                        StringJoiner sj = new StringJoiner(",");
                        sj.add(patternType.toString()).add(baseVList).add(baseLabelSeq);
                        sj.add(extVList).add(extLabelSeq).add(maxDeg.toString());
                        writer.write(sj.toString() + "\n");
                    }
                }
            }
        }
        writer.close();
        watch.stop();
        System.out.println("Saving: " + (watch.getTime() / 1000.0) + " sec");
    }

    private Pair<String, String> rotate(String vListString, String labelSeqString) {
        String[] vListEdges = vListString.split(";");
        String[] labelSeq = labelSeqString.split("->");
        String lastEdge = vListEdges[vListEdges.length - 1];
        String lastLabel = labelSeq[labelSeq.length - 1];

        for (int i = vListEdges.length - 2; i >= 0; --i) {
            vListEdges[i + 1] = vListEdges[i];
            labelSeq[i + 1] = labelSeq[i];
        }
        vListEdges[0] = lastEdge;
        labelSeq[0] = lastLabel;

        return new Pair<>(String.join(";", vListEdges), String.join("->", labelSeq));
    }

    public int getNumIntersection(List<Integer> vertices1, List<Integer> vertices2) {
        int count = 0;

        int i1 = 0;
        int i2 = 0;
        while (i1 < vertices1.size() && i2 < vertices2.size()) {
            if (vertices1.get(i1).equals(vertices2.get(i2))) {
                ++count;
                ++i1;
                ++i2;
            } else if (vertices1.get(i1) < vertices2.get(i2)) {
                ++i1;
            } else if (vertices1.get(i1) > vertices2.get(i2)) {
                ++i2;
            } else {
                System.err.println("ERROR: infinite loop");
                break;
            }
        }

        return count;
    }

    public void collect(CompletionService<Triple<String, String, String>> service, int total) throws Exception {
        double progress = 0;
        Map<String, Map<String, Double>> sanity2 = new HashMap<>();
        Map<String, Map<String, Double>> sanity3 = new HashMap<>();
        Map<String, Map<String, Double>> store;
        for (int i = 0; i < total; ++i) {
            Future<Triple<String, String, String>> result = service.take();
            Triple<String, String, String> entry = result.get();

            String[] catEntry = entry.v2.split(",");
            if (entry.v1.contains("1")) {
                store = catalogue;
            } else if (entry.v1.contains("2")) {
                store = sanity2;
            } else if (entry.v1.contains("3")) {
                store = sanity3;
            } else {
                System.err.println("ERROR: unrecognized run type");
                System.err.println("   result: " + entry);
                return;
            }

            // add to catalogue
            store.putIfAbsent(catEntry[0], new HashMap<>());
            store.get(catEntry[0]).put(
                    catEntry[1],
                    store.get(catEntry[0]).getOrDefault(catEntry[1], 0.0) + Double.parseDouble(catEntry[2]));

            // add to cat maxdeg
            if (entry.v3 != null) {
                String[] catMaxDegEntry = entry.v3.split(",");
                addCatMaxDeg(catMaxDegEntry[0], catMaxDegEntry[1], catMaxDegEntry[2],
                        catMaxDegEntry[3], Integer.parseInt(catMaxDegEntry[4]));
            }

            progress += 100.0 / total;
            System.out.print("\rConstructing: " + (int) progress + "%");
        }

        for (String vList : sanity2.keySet()) {
            for (String labelSeq : sanity2.get(vList).keySet()) {
                if (!catalogue.get(vList).get(labelSeq).equals(sanity2.get(vList).get(labelSeq))) {
                    System.err.println("ERROR: unmatched cardinality");
                    System.err.println("   runType: 2");
                    System.err.println("   vList: " + vList);
                    System.err.println("   labelSeq: " + labelSeq);
                    System.err.println("   firstEntry: " + catalogue.get(vList).get(labelSeq));
                    System.err.println("   sanity: " + sanity2.get(vList).get(labelSeq));
                    return;
                }
            }
        }
        for (String vList : sanity3.keySet()) {
            for (String labelSeq : sanity3.get(vList).keySet()) {
                if (!catalogue.get(vList).get(labelSeq).equals(sanity3.get(vList).get(labelSeq))) {
                    System.err.println("ERROR: unmatched cardinality");
                    System.err.println("   runType: 3");
                    System.err.println("   vList: " + vList);
                    System.err.println("   labelSeq: " + labelSeq);
                    System.err.println("   firstEntry: " + catalogue.get(vList).get(labelSeq));
                    System.err.println("   sanity: " + sanity3.get(vList).get(labelSeq));
                    return;
                }
            }
        }
    }

    private void addCatMaxDeg(
            String baseVList, String baseLabelSeq, String extVList, String extLabelSeq, Integer maxDeg) {
        catMaxDeg.putIfAbsent(baseVList, new HashMap<>());
        catMaxDeg.get(baseVList).putIfAbsent(baseLabelSeq, new HashMap<>());
        catMaxDeg.get(baseVList).get(baseLabelSeq).putIfAbsent(extVList, new HashMap<>());
        Integer current = catMaxDeg.get(baseVList).get(baseLabelSeq).get(extVList).getOrDefault(extLabelSeq, -1);
        catMaxDeg.get(baseVList).get(baseLabelSeq).get(extVList).put(extLabelSeq, Math.max(current, maxDeg));
    }

    public static Pair<String, String> getOverlap(Set<String> coveredEdges, String labelSeq, String vList) {
        String[] labelSeqSplitted = labelSeq.split("->");
        String[] vListSplitted = vList.split(";");
        StringJoiner labelSj = new StringJoiner("->");
        StringJoiner vListSj = new StringJoiner(";");

        for (int i = 0; i < vListSplitted.length; ++i) {
            if (coveredEdges.contains(vListSplitted[i])) {
                labelSj.add(labelSeqSplitted[i]);
                vListSj.add(vListSplitted[i]);
            }
        }

        return new Pair<>(labelSj.toString(), vListSj.toString());
    }

    public List<Triple<Set<String>, String, Double>> getAllEstimates(Query query) {
        Pair<String, String> vListAndLabelSeq = Util.topologyToVListAndLabelSeq(query.topology);
        vListAndLabelSeq = Util.sort(vListAndLabelSeq);
        Set<String> allEdges = Util.toVListSet(vListAndLabelSeq.key);

        List<Triple<Set<String>, String, Double>> complete = new ArrayList<>();
        Set<Triple<Set<String>, String, Double>> currentCoveredLabelsAndCard = new HashSet<>();
        Set<Triple<Set<String>, String, Double>> nextCoveredLabelsAndCard = new HashSet<>();

        Set<String> intersection = new HashSet<>();
        Pair<String, String> overlap;
        Set<String> nextCovered;
        String formula;

        Set<String> triangles = findTriangles(vListAndLabelSeq.key);
        QueryDecomposer decomposer = new QueryDecomposer();
        decomposer.decompose(query, 2);
        Set<String> trianglesAndLen2 = new HashSet<>(decomposer.decompositions.keySet());
        trianglesAndLen2.addAll(triangles);
        for (String startVList : triangles) {
            Set<String> startEdges = Util.toVListSet(startVList);
            String startLabelSeq = Util.extractLabelSeq(startVList, vListAndLabelSeq.key, vListAndLabelSeq.value);
            double est = catalogue.get(startVList).get(startLabelSeq);
            currentCoveredLabelsAndCard.clear();
            currentCoveredLabelsAndCard.add(new Triple<>(new HashSet<>(startEdges), startVList, est));

            for (int i = 0; i < allEdges.size() - startEdges.size() / 2; ++i) {
                for (String extension : trianglesAndLen2) {
                    Set<String> nextEdges = Util.toVListSet(extension);
                    String nextLabelSeq = Util.extractLabelSeq(extension, vListAndLabelSeq.key, vListAndLabelSeq.value);

                    for (Triple<Set<String>, String, Double> current : currentCoveredLabelsAndCard) {
                        if (current.v1.equals(allEdges)) {
                            complete.add(current);
                            continue;
                        } else {
                            nextCoveredLabelsAndCard.add(current);
                        }

                        intersection.clear();
                        intersection.addAll(current.v1);
                        intersection.retainAll(nextEdges);
                        if (intersection.size() == 0 || intersection.size() == nextEdges.size()) continue;

                        overlap = getOverlap(current.v1, nextLabelSeq, extension);
                        est = current.v3;
                        est /= catalogue.get(overlap.value).get(overlap.key);
                        est *= catalogue.get(extension).get(nextLabelSeq);

                        nextCovered = new HashSet<>(current.v1);
                        nextCovered.addAll(nextEdges);

                        formula = current.v2;
                        formula += "," + extension + "," + overlap.value;
                        nextCoveredLabelsAndCard.add(new Triple<>(nextCovered, formula, est));
                    }

                    currentCoveredLabelsAndCard = nextCoveredLabelsAndCard;
                    nextCoveredLabelsAndCard = new HashSet<>();
                }
            }

            complete.addAll(currentCoveredLabelsAndCard);
        }

        return complete.stream()
                .filter(triple -> triple.v1.equals(allEdges))
                .collect(Collectors.toList());
    }

    public static Set<String> toVSet(String vListString) {
        String[] split = vListString.split(",");
        Set<String> VSet = new HashSet<String>();
        for (String edges : split) {
            String[] edge_arr = edges.split(";");
            for (String edge : edge_arr) {
                VSet.add(edge);
            }
        }
        return VSet;
    }

    public List<Triple<Integer, String, Double>> getAllCyclicEstimates(Query query, String method, boolean randomSampling) throws Exception {
        Pair<String, String> vListAndLabelSeq = Util.topologyToVListAndLabelSeq(query.topology);
        vListAndLabelSeq = Util.sort(vListAndLabelSeq);
        String queryVList = vListAndLabelSeq.key;
        Set<String> alledges = Util.toVListSet(vListAndLabelSeq.key);

        // cycle-closing step indices (1 means cycle-closing, 0 means otherwise) -> formula -> est
        List<Triple<Integer, String, Double>> complete = new ArrayList<>();

        // find possible size-3 decompositions
        int catLen = 3;

        QueryDecomposer decomposer = new QueryDecomposer();
        decomposer.decompose(query, 3);
        Set<String> decoms = decomposer.decompositions.keySet();

        Set<String> triangles = findTriangles(queryVList);

        // acyclic size-3 entries
        Set<String> size3acyclic = new HashSet<>();
        for (String entry : decoms) {
            Integer[] vList = toVList(entry);
            if (vList.length == 6) size3acyclic.add(entry);
        }

        // total size-3 entries
        Set<String> size3decoms = new HashSet<>();
        size3decoms.addAll(triangles);
        size3decoms.addAll(size3acyclic);

        // System.out.println(Arrays.deepToString(size3decoms.toArray()));

        List<Thread> threads = new ArrayList<>();
        List<EstimateParallelCyclic> ests = new ArrayList<>();
        EstimateParallelCyclic est_par;
        Thread thread;
        int i = 0;

        if (method.contains("onlyExtensionRate")) {
            decoms = size3acyclic;
        } else {
            decoms = size3decoms;
        }
        for (String size3entry : decoms) {
            Integer[] startVList = toVList(size3entry);
            est_par = new EstimateParallelCyclic(i, query, catLen, queryVList, startVList, size3decoms, method, EstimateParallelCyclic.Type.ALL, randomSampling, 500000 / size3acyclic.size());
            ests.add(est_par);
            thread = new Thread(est_par);
            threads.add(thread);
            thread.start();
            i++;
        }

        for (Thread t : threads) {
            t.join();
        }

        for (EstimateParallelCyclic r : ests) {
            complete.addAll(r.alreadyCovered);
        }
        List<Triple<Integer, String, Double>> trimmed = complete.stream().distinct()
                .filter(triple -> toVSet(triple.v2).equals(alledges))
                .collect(Collectors.toList());
        int min = Integer.MAX_VALUE;
        List<Triple<Integer, String, Double>> results = new ArrayList<>();
        for (Triple<Integer, String, Double> est : trimmed) {
            if (min > est.v1) {
                min = est.v1;
                results.clear();
            }

            if (min == est.v1) {
                results.add(est);
            }
        }
        return results;
    }

    public List<List<Triple<Integer, String, Double>>> getAllCyclicEstimatesRandom(Query query, String method, boolean randomSampling) throws Exception {
        Pair<String, String> vListAndLabelSeq = Util.topologyToVListAndLabelSeq(query.topology);
        vListAndLabelSeq = Util.sort(vListAndLabelSeq);
        String queryVList = vListAndLabelSeq.key;
        Set<String> alledges = Util.toVListSet(vListAndLabelSeq.key);

        // cycle-closing step indices (1 means cycle-closing, 0 means otherwise) -> formula -> est
        List<Triple<Integer, String, Double>> minHop = new ArrayList<>();
        List<Triple<Integer, String, Double>> maxHop = new ArrayList<>();
        List<Triple<Integer, String, Double>> allHop = new ArrayList<>();

        // find possible size-3 decompositions
        int catLen = 3;

        QueryDecomposer decomposer = new QueryDecomposer();
        decomposer.decompose(query, 3);
        Set<String> decoms = decomposer.decompositions.keySet();

        Set<String> triangles = findTriangles(queryVList);

        // acyclic size-3 entries
        Set<String> size3acyclic = new HashSet<>();
        for (String entry : decoms) {
            Integer[] vList = toVList(entry);
            if (vList.length == 6) size3acyclic.add(entry);
        }

        // total size-3 entries
        Set<String> size3decoms = new HashSet<>();
        size3decoms.addAll(triangles);
        size3decoms.addAll(size3acyclic);

        // System.out.println(Arrays.deepToString(size3decoms.toArray()));

        List<Thread> threads = new ArrayList<>();
        List<EstimateParallelCyclic> ests = new ArrayList<>();
        EstimateParallelCyclic est_par;
        Thread thread;
        int i = 0;

        if (method.contains("onlyExtensionRate")) {
            for (String size3entry : size3acyclic) {
                Integer[] startVList = toVList(size3entry);
                if (i == size3acyclic.size() - 1) {
                    est_par = new EstimateParallelCyclic(i, query, catLen, queryVList, startVList, size3decoms, method, EstimateParallelCyclic.Type.ALL, randomSampling, 100000 - (size3acyclic.size() - 1) * (100000 / size3acyclic.size()));
                } else {
                    est_par = new EstimateParallelCyclic(i, query, catLen, queryVList, startVList, size3decoms, method, EstimateParallelCyclic.Type.ALL, randomSampling, 100000 / size3acyclic.size());
                }
                ests.add(est_par);
                thread = new Thread(est_par);
                threads.add(thread);
                thread.start();
                i++;
            }
        } else {
            for (String size3entry : size3decoms) {
                Integer[] startVList = toVList(size3entry);
                if (i == size3decoms.size() - 1) {
                    est_par = new EstimateParallelCyclic(i, query, catLen, queryVList, startVList, size3decoms, method, EstimateParallelCyclic.Type.ALL, randomSampling, 100000 - (size3decoms.size() - 1) * (100000 / size3decoms.size()));
                } else {
                    est_par = new EstimateParallelCyclic(i, query, catLen, queryVList, startVList, size3decoms, method, EstimateParallelCyclic.Type.ALL, randomSampling, 100000 / size3decoms.size());
                }
                ests.add(est_par);
                thread = new Thread(est_par);
                threads.add(thread);
                thread.start();
                i++;
            }
        }

        for (Thread t : threads) {
            t.join();
        }

        for (EstimateParallelCyclic r : ests) {
            minHop.addAll(r.minAlreadyCovered);
            maxHop.addAll(r.maxAlreadyCovered);
            allHop.addAll(r.allAlreadyCovered);
        }

        List<List<Triple<Integer, String, Double>>> complete = new ArrayList<>();
        complete.add(minHop.stream().distinct().filter(triple -> toVSet(triple.v2).equals(alledges)).collect(Collectors.toList()));
        complete.add(maxHop.stream().distinct().filter(triple -> toVSet(triple.v2).equals(alledges)).collect(Collectors.toList()));
        complete.add(allHop.stream().distinct().filter(triple -> toVSet(triple.v2).equals(alledges)).collect(Collectors.toList()));
        return complete;

    }

    public List<List<Triple<List<Integer>, String, Double>>> getCyclicEstimatesByCycleClosing(Query query, String method, boolean randomSampling) throws Exception {
        Pair<String, String> vListAndLabelSeq = Util.topologyToVListAndLabelSeq(query.topology);
        vListAndLabelSeq = Util.sort(vListAndLabelSeq);
        String queryVList = vListAndLabelSeq.key;
        Set<String> alledges = Util.toVListSet(vListAndLabelSeq.key);

        // formula -> est
        List<Triple<List<Integer>, String, Double>> earlyClosing = new ArrayList<>();
        List<Triple<List<Integer>, String, Double>> lateClosing = new ArrayList<>();

        // find possible size-3 decompositions
        int catLen = 3;

        QueryDecomposer decomposer = new QueryDecomposer();
        decomposer.decompose(query, 3);
        Set<String> decoms = decomposer.decompositions.keySet();

        // cyclic size-3 entries (i.e. triangles)
        Set<String> triangles = findTriangles(queryVList);

        // acyclic size-3 entries
        Set<String> size3acyclic = new HashSet<>();
        for (String entry : decoms) {
            Integer[] vList = toVList(entry);
            if (vList.length == 6) size3acyclic.add(entry);
        }

        // total size-3 entries
        Set<String> size3decoms = new HashSet<>();
        size3decoms.addAll(triangles);
        size3decoms.addAll(size3acyclic);

        // System.out.println("here2 " + Arrays.deepToString(triangles.toArray()));

        List<Thread> threads = new ArrayList<>();
        List<EstimateParallelCyclic> ests = new ArrayList<>();
        EstimateParallelCyclic est_par;
        Thread thread;
        int i = 0;

        // early cycle closing option
        if (triangles.size() != 0 && !method.contains("onlyExtensionRate")) {
            for (String size3entry : triangles) {
                Integer[] startVList = toVList(size3entry);
                if (i == triangles.size() - 1) {
                    est_par = new EstimateParallelCyclic(i, query, catLen, queryVList, startVList, size3decoms, method, EstimateParallelCyclic.Type.EARLY, randomSampling, 500000 - (triangles.size() - 1) * (500000 / triangles.size()));
                } else {
                    est_par = new EstimateParallelCyclic(i, query, catLen, queryVList, startVList, size3decoms, method, EstimateParallelCyclic.Type.EARLY, randomSampling, 500000 / triangles.size());
                }
                ests.add(est_par);
                thread = new Thread(est_par);
                threads.add(thread);
                thread.start();
                i++;
            }
        } else {
            for (String size3entry : size3acyclic) {
                Integer[] startVList = toVList(size3entry);
                if (i == size3acyclic.size() - 1) {
                    est_par = new EstimateParallelCyclic(i, query, catLen, queryVList, startVList, size3decoms, method, EstimateParallelCyclic.Type.EARLY, randomSampling, 500000 - (size3acyclic.size() - 1) * (500000 / size3acyclic.size()));
                } else {
                    est_par = new EstimateParallelCyclic(i, query, catLen, queryVList, startVList, size3decoms, method, EstimateParallelCyclic.Type.EARLY, randomSampling, 500000 / size3acyclic.size());
                }
                ests.add(est_par);
                thread = new Thread(est_par);
                threads.add(thread);
                thread.start();
                i++;
            }
        }

        i = 0;

        // late cycle-closing option
        if (size3acyclic.size() != 0 && !method.contains("onlyExtensionRate")) {
            for (String size3entry : size3acyclic) {
                Integer[] startVList = toVList(size3entry);
                if (i == size3acyclic.size() - 1){
                    est_par = new EstimateParallelCyclic(i, query, catLen, queryVList, startVList, size3decoms, method, EstimateParallelCyclic.Type.LATE, randomSampling, 500000 - (size3acyclic.size() - 1) * (500000 / size3acyclic.size()));
                } else {
                    est_par = new EstimateParallelCyclic(i, query, catLen, queryVList, startVList, size3decoms, method, EstimateParallelCyclic.Type.LATE, randomSampling, 500000 / size3acyclic.size());
                }
                ests.add(est_par);
                thread = new Thread(est_par);
                threads.add(thread);
                thread.start();
                i++;
            }
        } else {
            for (String size3entry : triangles) {
                Integer[] startVList = toVList(size3entry);
                if (i == triangles.size() - 1) {
                    est_par = new EstimateParallelCyclic(i, query, catLen, queryVList, startVList, size3decoms, method, EstimateParallelCyclic.Type.LATE, randomSampling, 500000 - (triangles.size() - 1) * (500000 / triangles.size()));
                } else {
                    est_par = new EstimateParallelCyclic(i, query, catLen, queryVList, startVList, size3decoms, method, EstimateParallelCyclic.Type.LATE, randomSampling, 500000 / triangles.size());
                }
                ests.add(est_par);
                thread = new Thread(est_par);
                threads.add(thread);
                thread.start();
                i++;
            }
        }

        for (Thread t : threads) {
            t.join();
        }

        for (EstimateParallelCyclic r : ests) {
            earlyClosing.addAll(r.earlyClosing);
            lateClosing.addAll(r.lateClosing);
        }

        List<List<Triple<List<Integer>, String, Double>>> l = new ArrayList<>();
        l.add(earlyClosing.stream().distinct()
                .filter(triple -> toVSet(triple.v2).equals(alledges))
                .collect(Collectors.toList()));
        l.add(lateClosing.stream().distinct()
                .filter(triple -> toVSet(triple.v2).equals(alledges))
                .collect(Collectors.toList()));
        return l;
    }


    public Double[] estimateWithTriangle(Query query, String method) {
        String vListString = query.toString().split(",")[0];
        List<Triple<Set<String>, String, Double>> allEstimates = getAllEstimates(query);
        Set<Double> allEst = new HashSet<>();

        // #hops -> (min, max, avg)
        Map<Integer, Triple<Double, Double, Double>> numHop2aggr = new HashMap<>();
        Map<Integer, Integer> numHops2numFormula = new HashMap<>();
        int maxHop = Integer.MIN_VALUE;
        int minHop = Integer.MAX_VALUE;
        Triple<Double, Double, Double> globalAggr = new Triple<>(Double.MAX_VALUE, Double.MIN_VALUE, 0.0);
        int numFormula = 0;
        for (Triple<Set<String>, String, Double> triple : allEstimates) {
            double est = triple.v3;
            if (allEst.contains(est)) continue;

            // ad-hoc way to make sure we don't estimate acyclic invariants
            if (vListString.equals("0-1;0-2;0-3;1-2;2-3")) {
                if (!triple.v2.contains("0-1;0-2;1-2") || !triple.v2.contains("0-2;0-3;2-3")) continue;
            } else if (vListString.equals("0-1;0-2;1-2;2-3;2-4;3-4")) {
                if (!triple.v2.contains("0-1;0-2;1-2") || !triple.v2.contains("2-3;2-4;3-4")) continue;
            } else if (vListString.equals("0-1;0-2;1-2;2-3;3-4")) {
                if (!triple.v2.contains("0-1;0-2;1-2")) continue;
            }

            allEst.add(est);

            globalAggr.v1 = Math.min(globalAggr.v1, est);
            globalAggr.v2 = Math.max(globalAggr.v2, est);
            globalAggr.v3 += est;
            numFormula++;

            int formulaLen = triple.v2.split(",").length / 2 + 1;
            numHops2numFormula.put(formulaLen, numHops2numFormula.getOrDefault(formulaLen, 0) + 1);
            numHop2aggr.putIfAbsent(formulaLen, new Triple<>(Double.MAX_VALUE, Double.MIN_VALUE, 0.0));
            maxHop = Math.max(maxHop, formulaLen);
            minHop = Math.min(minHop, formulaLen);

            Triple<Double, Double, Double> stats = numHop2aggr.get(formulaLen);
            stats.v1 = Math.min(stats.v1, est);
            stats.v2 = Math.max(stats.v2, est);
            stats.v3 += est;
            numHop2aggr.put(formulaLen, stats);
        }

        return new Double[] {
                globalAggr.v1,
                globalAggr.v2,
                globalAggr.v3 / numFormula,
                numHop2aggr.get(minHop).v1,
                numHop2aggr.get(minHop).v2,
                numHop2aggr.get(minHop).v3 / numHops2numFormula.get(minHop),
                numHop2aggr.get(maxHop).v1,
                numHop2aggr.get(maxHop).v2,
                numHop2aggr.get(maxHop).v3 / numHops2numFormula.get(maxHop)
        };
    }

    private List<Triple<Integer, String, Double>> trim(List<Triple<Integer, String, Double>> allEstimates, Query query) {

        Pair<String, String> vListAndLabelSeq = Util.topologyToVListAndLabelSeq(query.topology);
        vListAndLabelSeq = Util.sort(vListAndLabelSeq);
        String queryVList = vListAndLabelSeq.key;
        Set<String> triangles = findTriangles(queryVList);
        Integer maxTriangleContained = 0;
        List<Triple<Integer, String, Double>> maxTrigAllEstimates = new ArrayList<>();

        for (Triple<Integer, String, Double> triple : allEstimates) {
            // check the number of triangles current triple contains
            Integer numTrig = 0;
            for (String triangle : triangles) {
                if (triple.v2.contains(triangle)) {
                    numTrig += 1;
                }
            }

            if (numTrig.equals(maxTriangleContained)) {
                maxTrigAllEstimates.add(triple);
            } else if (numTrig > maxTriangleContained) {
                maxTriangleContained = numTrig;
                maxTrigAllEstimates = new ArrayList<>();
                maxTrigAllEstimates.add(triple);
            }

        }
        return maxTrigAllEstimates;
    }

    public Double[] cyclicEstimation(Query query, String method, boolean randomSampling, boolean allEst, double trueCard) throws Exception {

        if (allEst && !randomSampling) {
            List<Triple<Integer, String, Double>> allEstimates = getAllCyclicEstimates(query, method, randomSampling);

            // System.out.println(Arrays.deepToString(allEstimates.toArray()));
//            System.out.println(allEstimates.size());
//
            if (allEstimates.size() == 0) {
                return new Double[]{-1.0};
            }
            // #hops -> (min, max, avg)
            Map<Integer, Triple<Double, Double, Double>> numHop2aggr = new HashMap<>();
            Map<Integer, Integer> numHops2numFormula = new HashMap<>();
            int maxHop = Integer.MIN_VALUE;
            int minHop = Integer.MAX_VALUE;
            Triple<Double, Double, Double> globalAggr = new Triple<>(Double.MAX_VALUE, Double.MIN_VALUE, 0.0);
            int numFormula = 0;

            if (method.contains("baseline")) {
                allEstimates = trim(allEstimates, query);
            }

            double pStar = -1.0, qerr, pStarErr = Double.MAX_VALUE;
            for (Triple<Integer, String, Double> triple : allEstimates) {
                double est = triple.v3;
                if (est >= trueCard) qerr = est / trueCard;
                else qerr = trueCard / est;
                if (pStarErr > Math.abs(qerr)) {
                    pStar = est;
                    pStarErr = qerr;
                }

                globalAggr.v1 = Math.min(globalAggr.v1, est);
                globalAggr.v2 = Math.max(globalAggr.v2, est);
                globalAggr.v3 += est;
                numFormula++;

                int formulaLen = triple.v2.split(",").length / 2 + 1;
                numHops2numFormula.put(formulaLen, numHops2numFormula.getOrDefault(formulaLen, 0) + 1);
                numHop2aggr.putIfAbsent(formulaLen, new Triple<>(Double.MAX_VALUE, Double.MIN_VALUE, 0.0));
                maxHop = Math.max(maxHop, formulaLen);
                minHop = Math.min(minHop, formulaLen);

                Triple<Double, Double, Double> stats = numHop2aggr.get(formulaLen);
                stats.v1 = Math.min(stats.v1, est);
                stats.v2 = Math.max(stats.v2, est);
                stats.v3 += est;
                numHop2aggr.put(formulaLen, stats);
            }
            return new Double[] {
                    globalAggr.v1,
                    globalAggr.v2,
                    globalAggr.v3 / numFormula,
                    numHop2aggr.get(minHop).v1,
                    numHop2aggr.get(minHop).v2,
                    numHop2aggr.get(minHop).v3 / numHops2numFormula.get(minHop),
                    numHop2aggr.get(maxHop).v1,
                    numHop2aggr.get(maxHop).v2,
                    numHop2aggr.get(maxHop).v3 / numHops2numFormula.get(maxHop),
                    pStar
            };

        } else if (allEst) {
            List<List<Triple<Integer, String, Double>>> allEstimates = getAllCyclicEstimatesRandom(query, method, randomSampling);
            List<Triple<Integer, String, Double>> minHopEstimates = allEstimates.get(0);
            List<Triple<Integer, String, Double>> maxHopEstimates = allEstimates.get(1);
            List<Triple<Integer, String, Double>> allHopEstimates = allEstimates.get(2);

            // System.out.println(Arrays.deepToString(allEstimates.toArray()));
            System.out.println(minHopEstimates.size());
            System.out.println(allHopEstimates.size());
            System.out.println(maxHopEstimates.size());

            // #hops -> (min, max, avg)
            Triple<Double, Double, Double> globalAggr = new Triple<>(Double.MAX_VALUE, Double.MIN_VALUE, 0.0);
            int numFormula = 0;
            // all-hop
            for (Triple<Integer, String, Double> triple : allHopEstimates) {
                double est = triple.v3;

                globalAggr.v1 = Math.min(globalAggr.v1, est);
                globalAggr.v2 = Math.max(globalAggr.v2, est);
                globalAggr.v3 += est;
                numFormula++;
            }
            // min-hop
            Map<Integer, Triple<Double, Double, Double>> numHop2aggr = new HashMap<>();
            Map<Integer, Integer> numHops2numFormula = new HashMap<>();
            int minHop = Integer.MAX_VALUE;
            for (Triple<Integer, String, Double> triple : minHopEstimates) {
                double est = triple.v3;
                int formulaLen = triple.v2.split(",").length / 2 + 1;
                numHops2numFormula.put(formulaLen, numHops2numFormula.getOrDefault(formulaLen, 0) + 1);
                numHop2aggr.putIfAbsent(formulaLen, new Triple<>(Double.MAX_VALUE, Double.MIN_VALUE, 0.0));
                minHop = Math.min(minHop, formulaLen);

                Triple<Double, Double, Double> stats = numHop2aggr.get(formulaLen);
                stats.v1 = Math.min(stats.v1, est);
                stats.v2 = Math.max(stats.v2, est);
                stats.v3 += est;
                numHop2aggr.put(formulaLen, stats);
            }
            // max-hop
            Map<Integer, Triple<Double, Double, Double>> numHop2aggr2 = new HashMap<>();
            Map<Integer, Integer> numHops2numFormula2 = new HashMap<>();
            int maxHop = Integer.MIN_VALUE;
            for (Triple<Integer, String, Double> triple : maxHopEstimates) {
                double est = triple.v3;
                int formulaLen = triple.v2.split(",").length / 2 + 1;
                numHops2numFormula2.put(formulaLen, numHops2numFormula2.getOrDefault(formulaLen, 0) + 1);
                numHop2aggr2.putIfAbsent(formulaLen, new Triple<>(Double.MAX_VALUE, Double.MIN_VALUE, 0.0));
                maxHop = Math.max(maxHop, formulaLen);

                Triple<Double, Double, Double> stats = numHop2aggr2.get(formulaLen);
                stats.v1 = Math.min(stats.v1, est);
                stats.v2 = Math.max(stats.v2, est);
                stats.v3 += est;
                numHop2aggr2.put(formulaLen, stats);
            }
            System.out.println("min: " + numHops2numFormula.get(minHop));
            System.out.println("max: " + numHops2numFormula2.get(maxHop));
            System.out.println("all: " + numFormula);
            System.out.println(minHop);
            System.out.println(maxHop);

            return new Double[] {
                    globalAggr.v1,
                    globalAggr.v2,
                    globalAggr.v3 / numFormula,
                    numHop2aggr.get(minHop).v1,
                    numHop2aggr.get(minHop).v2,
                    numHop2aggr.get(minHop).v3 / numHops2numFormula.get(minHop),
                    numHop2aggr2.get(maxHop).v1,
                    numHop2aggr2.get(maxHop).v2,
                    numHop2aggr2.get(maxHop).v3 / numHops2numFormula2.get(maxHop)
            };
        } else {
            List<List<Triple<List<Integer>, String, Double>>> allEstimatesByCycleClosing = getCyclicEstimatesByCycleClosing(query, method, randomSampling);

            // System.out.println(Arrays.deepToString(allEstimatesByCycleClosing.toArray()));
            System.out.println(allEstimatesByCycleClosing.get(0).size());
            System.out.println(allEstimatesByCycleClosing.get(1).size());

            // early -> #hops -> (min, max, avg)
            Map<Integer, Triple<Double, Double, Double>> numHop2aggr = new HashMap<>();
            Map<Integer, Integer> numHops2numFormula = new HashMap<>();
            int maxHop = Integer.MIN_VALUE;
            int minHop = Integer.MAX_VALUE;
            Triple<Double, Double, Double> globalAggr = new Triple<>(Double.MAX_VALUE, Double.MIN_VALUE, 0.0);
            int numFormula = 0;
            for (Triple<List<Integer>, String, Double> triple : allEstimatesByCycleClosing.get(0)) {
                double est = triple.v3;

                globalAggr.v1 = Math.min(globalAggr.v1, est);
                globalAggr.v2 = Math.max(globalAggr.v2, est);
                globalAggr.v3 += est;
                numFormula++;

                int formulaLen = triple.v2.split(",").length / 2 + 1;
                numHops2numFormula.put(formulaLen, numHops2numFormula.getOrDefault(formulaLen, 0) + 1);
                numHop2aggr.putIfAbsent(formulaLen, new Triple<>(Double.MAX_VALUE, Double.MIN_VALUE, 0.0));
                maxHop = Math.max(maxHop, formulaLen);
                minHop = Math.min(minHop, formulaLen);

                Triple<Double, Double, Double> stats = numHop2aggr.get(formulaLen);
                stats.v1 = Math.min(stats.v1, est);
                stats.v2 = Math.max(stats.v2, est);
                stats.v3 += est;
                numHop2aggr.put(formulaLen, stats);
            }

            Double[] output = new Double[18];
            output[0] = globalAggr.v1;
            output[1] = globalAggr.v2;
            output[2] = globalAggr.v3 / numFormula;
            output[3] = numHop2aggr.get(minHop).v1;
            output[4] = numHop2aggr.get(minHop).v2;
            output[5] = numHop2aggr.get(minHop).v3 / numHops2numFormula.get(minHop);
            output[6] = numHop2aggr.get(maxHop).v1;
            output[7] = numHop2aggr.get(maxHop).v2;
            output[8] = numHop2aggr.get(maxHop).v3 / numHops2numFormula.get(maxHop);

            // late -> #hops -> (min, max, avg)
            numHop2aggr = new HashMap<>();
            numHops2numFormula = new HashMap<>();
            maxHop = Integer.MIN_VALUE;
            minHop = Integer.MAX_VALUE;
            globalAggr = new Triple<>(Double.MAX_VALUE, Double.MIN_VALUE, 0.0);
            numFormula = 0;
            for (Triple<List<Integer>, String, Double> triple : allEstimatesByCycleClosing.get(1)) {
                double est = triple.v3;

                globalAggr.v1 = Math.min(globalAggr.v1, est);
                globalAggr.v2 = Math.max(globalAggr.v2, est);
                globalAggr.v3 += est;
                numFormula++;

                int formulaLen = triple.v2.split(",").length / 2 + 1;
                numHops2numFormula.put(formulaLen, numHops2numFormula.getOrDefault(formulaLen, 0) + 1);
                numHop2aggr.putIfAbsent(formulaLen, new Triple<>(Double.MAX_VALUE, Double.MIN_VALUE, 0.0));
                maxHop = Math.max(maxHop, formulaLen);
                minHop = Math.min(minHop, formulaLen);

                Triple<Double, Double, Double> stats = numHop2aggr.get(formulaLen);
                stats.v1 = Math.min(stats.v1, est);
                stats.v2 = Math.max(stats.v2, est);
                stats.v3 += est;
                numHop2aggr.put(formulaLen, stats);
            }

            output[9] = globalAggr.v1;
            output[10] = globalAggr.v2;
            output[11] = globalAggr.v3 / numFormula;
            output[12] = numHop2aggr.get(minHop).v1;
            output[13] = numHop2aggr.get(minHop).v2;
            output[14] = numHop2aggr.get(minHop).v3 / numHops2numFormula.get(minHop);
            output[15] = numHop2aggr.get(maxHop).v1;
            output[16] = numHop2aggr.get(maxHop).v2;
            output[17] = numHop2aggr.get(maxHop).v3 / numHops2numFormula.get(maxHop);

            return output;
        }

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

            neighbours.putIfAbsent(line[0], new ArrayList<>());
            neighbours.get(line[0]).add(new Pair<>(line[2],line[1]));
            neighbours.putIfAbsent(line[2], new ArrayList<>());
            neighbours.get(line[2]).add(new Pair<>(line[0],(-1 * line[1]) - 1));

            tripleString = csvReader.readLine();
        }

        csvReader.close();

        watch.stop();
        System.out.println("Graph Loading: " + (watch.getTime() / 1000.0) + " sec");

        watch = new StopWatch();
        watch.start();
        for (Integer src : src2label2dest.keySet()) {
            for (Integer label : src2label2dest.get(src).keySet()) {
                Collections.sort(src2label2dest.get(src).get(label));
            }
        }
        for (Integer dest : dest2label2src.keySet()) {
            for (Integer label : dest2label2src.get(dest).keySet()) {
                Collections.sort(dest2label2src.get(dest).get(label));
            }
        }
        watch.stop();
        System.out.println("Graph Sorting: " + (watch.getTime() / 1000.0) + " sec");
    }

    public void readCatalogue(String catalogueFile) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(catalogueFile));
        String line = reader.readLine();
        while (line != null) {
            String[] info = line.split(",");
            catalogue.putIfAbsent(info[1], new HashMap<>());
            catalogue.get(info[1]).put(info[2], Double.parseDouble(info[3]));
            line = reader.readLine();
        }
        reader.close();
    }

    public TriangleCatalogue(String catFile) throws Exception {
        readCatalogue(catFile);
    }

    public TriangleCatalogue() {}

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println("queryFile: " + args[1]);
        System.out.println("catDestFile: " + args[2]);
        System.out.println("catMaxDegDestFile: " + args[3]);
        System.out.println("cycleClosingMethod: " + args[4]);
        // args[5]: tmpCatFile
        System.out.println();

        TriangleCatalogue catalogue = new TriangleCatalogue();
        String method = args[4];
        if (method.contains("acyclic1")) {
            catalogue.construct(args[0], args[1]);
        } else if (method.contains("acyclic2")) {
            catalogue.constructAcyclic(args[0], args[1], args[2], args[5]);
        } else if (method.contains("triangleClosing")) {
            catalogue.construct2(args[0], args[1], args[2], args[3]);
        } else if (method.contains("avgSampledExtensionRate")) {
            catalogue.construct3(args[0], args[1], args[2], args[3]);
        } else if (method.contains("onlyExtensionRate")) {
            catalogue.construct4(args[0], args[1], args[2], args[3]);
        } else if (method.contains("baseline")) {
            catalogue.construct5(args[0], args[1], args[2], args[3]);
        } else if (method.contains("allInclusive")) {
            catalogue.construct6(args[0], args[1], args[2], args[3]);
        }
    }
}
