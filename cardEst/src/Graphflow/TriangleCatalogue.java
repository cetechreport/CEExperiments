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

public class TriangleCatalogue {
    Map<Integer, List<Pair<Integer, Integer>>> label2srcdest = new HashMap<>();
    Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();

    Map<String, Map<String, Long>> catalogue = new HashMap<>();
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
                allSrcdest.subList(i, Math.min(allSrcdest.size(), i + allSrcdest.size() / numWorkers))
            );
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
                src2label2dest, dest2label2src,
                allSrcdest.subList(i, Math.min(allSrcdest.size(), i + allSrcdest.size() / numWorkers))
            );
            service.submit(task);
            ++submitted;
        }

        return submitted;
    }

    public void construct(String graphFile, Integer patternType, String queryFile, String catFile, String catMaxDegFile)
        throws Exception {
        readGraph(graphFile);

        StopWatch watch = new StopWatch();
        watch.start();

        List<Query> queries = Query.readQueries(queryFile);
        String queryVList = queries.get(0).toString().split(",")[0];
        Set<String> triangleVLists = findTriangles(queryVList);

        QueryDecomposer decomposer = new QueryDecomposer();
        decomposer.decompose(queries.get(0), 2);
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
            for (String pathVList : decoms) {
                String pathLabelSeq = Util.extractLabelSeq(pathVList, queryVList, queryLabelSeq);
                tasks.putIfAbsent(pathVList, new HashSet<>());
                tasks.get(pathVList).add(pathLabelSeq);
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
                Long count = catalogue.get(vList).get(labelSeq);
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
        Map<String, Map<String, Long>> sanity2 = new HashMap<>();
        Map<String, Map<String, Long>> sanity3 = new HashMap<>();
        Map<String, Map<String, Long>> store;
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
                store.get(catEntry[0]).getOrDefault(catEntry[1], 0L) + Long.parseLong(catEntry[2]));

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

    private Pair<String, String> getOverlap(Set<String> coveredEdges, String labelSeq, String vList) {
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

    public Double[] estimateWithTriangle(Query query) {
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
            double est = ((long) (triple.v3 * Math.pow(10, 6))) / Math.pow(10, 6);
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
            catalogue.get(info[1]).put(info[2], Long.parseLong(info[3]));
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
        System.out.println("patternType: " + args[1]);
        System.out.println("queryFile: " + args[2]);
        System.out.println("catDestFile: " + args[3]);
        System.out.println("catMaxDegDestFile: " + args[4]);
        System.out.println();

        TriangleCatalogue catalogue = new TriangleCatalogue();
        catalogue.construct(args[0], Integer.parseInt(args[1]), args[2], args[3], args[4]);
    }
}
