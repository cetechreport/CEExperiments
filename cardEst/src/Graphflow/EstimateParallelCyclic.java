package Graphflow;

import Common.Pair;
import Common.Query;
import Common.Triple;
import Common.Util;
import org.apache.commons.lang3.ObjectUtils;

import java.util.*;


public class EstimateParallelCyclic implements Runnable {
    public enum Type {
        EARLY, LATE, ALL
    }

    private int threadId;
    private Query query;
    private int catLen;
    private String queryVList;
    private Integer[] startVList;
    Set<String> size3decoms;
    String method;
    Type cycleClosingType;
    boolean randomSampling;
    int subsetSize;

    // earlyClosing Option
    public List<Triple<List<Integer>, String, Double>> earlyClosing = new ArrayList<>();
    // lateClosing Option
    public List<Triple<List<Integer>, String, Double>> lateClosing = new ArrayList<>();

    // cycle-closing step indices (1 means cycle-closing, 0 means otherwise) -> formula -> est
    public List<Triple<Integer, String, Double>> alreadyCovered = new ArrayList<>();
    public List<Triple<Integer, String, Double>> minAlreadyCovered = new ArrayList<>();
    public List<Triple<Integer, String, Double>> maxAlreadyCovered = new ArrayList<>();
    public List<Triple<Integer, String, Double>> allAlreadyCovered = new ArrayList<>();
    Set<Triple<Integer, String, Double>> currentCoveredLabelsAndCard = new HashSet<>();
    Set<Triple<Integer, String, Double>> nextCoveredLabelsAndCard = new HashSet<>();
    
    private enum Shape {
        STAR, PATH, TRIG
    }

    // return 0 (3-Star), 1 (3-Path), 2 (Triangle)
    private Shape getType(Integer[] vList) {
        Map<Integer, Integer> occurrences = new HashMap<>();
        for (Integer v : vList) {
            occurrences.put(v, occurrences.getOrDefault(v, 0) + 1);
        }

        int degOne = 0;
        for (Integer vertex : occurrences.keySet()) {
            if (occurrences.get(vertex) == 1) ++degOne;
        }

        if (degOne == 3) {
            return Shape.STAR;
        } else if (degOne == 2) {
            return Shape.PATH;
        } else {
            return Shape.TRIG;
        }
    }

    //returns intersection of two sets of vertices
    private Integer intersection(String curVListString, Set<String> nonOverlappedEdges) {
        Set<Integer> curCoveredVList = new HashSet<>();
        for (String edge : Catalogue.toVSet(curVListString)) {
            String[] endPoints = edge.split("-");
            curCoveredVList.add(Integer.parseInt(endPoints[0]));
            curCoveredVList.add(Integer.parseInt(endPoints[1]));
        }
        Set<Integer> extendedVList = new HashSet<>();
        for (String edge : nonOverlappedEdges) {
            String[] endPoints = edge.split("-");
            extendedVList.add(Integer.parseInt(endPoints[0]));
            extendedVList.add(Integer.parseInt(endPoints[1]));
        }

        extendedVList.retainAll(curCoveredVList);

        return extendedVList.size();
    }

    public void allCycleClosing() {
        String startLabelSeq, nextLabelSeq, vListString, formula;
        Pair<String, String> overlap;
        Set<String> intersection = new HashSet<>(), unvisited = new HashSet<>();
        String[] nextVList;
        Set<String> visited;
        Set<String> allEdges = TriangleCatalogue.toVSet(queryVList);
        int count;

        startLabelSeq = Catalogue.extractPath(query.topology, startVList);
        vListString = Catalogue.toVListString(startVList);
        double est = TriangleCatalogue.catalogue.get(vListString).get(startLabelSeq);

        currentCoveredLabelsAndCard.clear();
        nextCoveredLabelsAndCard.clear();
        currentCoveredLabelsAndCard.add(new Triple(0, vListString, est));

        for (int i = 0; i < allEdges.size() - catLen + 1; ++i) {
            for (String extension : size3decoms) {
                nextLabelSeq = Catalogue.extractPath(query.topology, TriangleCatalogue.toVList(extension));
                nextVList = extension.split(";");

                for (Triple<Integer, String, Double> current : currentCoveredLabelsAndCard) {
                    est = current.v3;
                    count = current.v1;
                    if (Catalogue.toVSet(current.v2).equals(allEdges)) {
                        alreadyCovered.add(current);
                        continue;
                    }

                    intersection.clear();
                    visited = TriangleCatalogue.toVSet(current.v2);
                    intersection.addAll(visited);
                    intersection.retainAll(Arrays.asList(nextVList));

                    if (intersection.size() == 0 || intersection.size() == nextVList.length) continue;

                    overlap = TriangleCatalogue.getOverlap(visited, nextLabelSeq, extension);

                    //unvisited edges
                    unvisited.clear();
                    unvisited.addAll(Arrays.asList(nextVList));
                    unvisited.removeAll(visited);

                    if (method.contains("acyclic") || method.contains("baseline")) {
                        if (TriangleCatalogue.catalogue.get(overlap.value) == null) continue;

                        // if overlap is a disconnected subgraph
                        Set<Integer> overlapVertices = new HashSet<>();
                        for (Integer v : TriangleCatalogue.toVList(overlap.value)) {
                            overlapVertices.add(v);
                        }
                        if (overlapVertices.size() == 4) continue;

                        est /= TriangleCatalogue.catalogue.get(overlap.value).get(overlap.key);
                        est *= TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq);
                    } else {
                        Shape shape = getType(TriangleCatalogue.toVList(extension));
                        int intersecSize = intersection(current.v2, unvisited);
                        // check for triangles, if it properly closes a cycle, proceed; otherwise, continue (i.e. skip)
                        if (shape == Shape.TRIG && unvisited.size() == 2 && intersecSize == 3) {
                            continue;
                        }
                        // check for 3-stars, if does not close a cycle, proceed; otherwise, continue (i.e. skip)
                        else if (shape == Shape.STAR && intersecSize >= 2) {
                            continue;
                        }
                        // check for 3-paths, if it does not close a cycle, proceed;
                        // if it properly closes a cycle, use triangle/extensionRate; otherwise, continue (i.e. skip)
                        else if (shape == Shape.PATH && intersecSize >= 2) {
                            if (unvisited.size() == 1) {
                                Map<Integer, Integer> occurrences = new HashMap<>();
                                for (Integer v : TriangleCatalogue.toVList(extension)) {
                                    occurrences.put(v, occurrences.getOrDefault(v, 0) + 1);
                                }
                                String[] extensionEdge = unvisited.stream().toArray(String[]::new);
                                Integer v1 = Integer.parseInt(extensionEdge[0].split("-")[0]);
                                Integer v2 = Integer.parseInt(extensionEdge[0].split("-")[1]);
                                if (!method.contains("triangleClosing") && occurrences.get(v1) == 2 && occurrences.get(v2) == 2) {
                                    // cycle-closing simulation
                                    // est /= TriangleCatalogue.catalogue.get(overlap.value).get(overlap.key);
                                    if (method.contains("avgSampledExtensionRate") || method.contains("onlyExtensionRate")) {
                                        if (TriangleCatalogue.catalogue.get(extension) == null) {
                                            System.out.println("Cannot find entry ----------------------------");
                                            break;
                                        }
                                        if (TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq + "*") == 0.0) continue;
                                        est *= TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq + "*");
                                        count += 1;
                                    } else if (method.contains("midEdgeClosing")) {
                                        est /= TriangleCatalogue.catalogue.get(overlap.value.split(";")[0]).get(overlap.key.split("->")[0]);
                                        est /= TriangleCatalogue.catalogue.get(overlap.value.split(";")[1]).get(overlap.key.split("->")[1]);
                                        est *= TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq);
                                        count += 1;
                                    }

                                } else if (method.contains("triangleClosing") && !(occurrences.get(v1) == 2 && occurrences.get(v2) == 2)) {
                                    if (TriangleCatalogue.catalogue.get("*" + extension).get(nextLabelSeq) == 0.0) continue;
                                    est /= TriangleCatalogue.catalogue.get(overlap.value).get(overlap.key);
                                    est *= TriangleCatalogue.catalogue.get("*" + extension).get(nextLabelSeq);
                                    count += 1;
                                } else continue;
                            } else continue;
                        } else {
                            if (method.contains("onlyExtensionRate") && shape == Shape.TRIG) {
                                if (unvisited.size() == 1) {
                                    if (nextVList[0] == unvisited.toArray()[0]) {
                                        if (TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq + "*") == 0) continue;
                                        est *= TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq + "*");
                                    } else if (nextVList[1] == unvisited.toArray()[0]) {
                                        if (TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq + "**") == 0) continue;
                                        est *= TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq + "**");
                                    } else {
                                        if (TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq + "***") == 0) continue;
                                        est *= TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq + "***");
                                    }
                                    count += 1;
                                } else continue;
                            } else {
                                if (TriangleCatalogue.catalogue.get(extension) == null) {
                                    System.out.println("Cannot find entry -------------------");
                                    break;
                                }
                                if (TriangleCatalogue.catalogue.get(overlap.value) == null) {
                                    System.out.println("Cannot find entry -------------------");
                                    break;
                                }
                                est /= TriangleCatalogue.catalogue.get(overlap.value).get(overlap.key);
                                est *= TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq);
                            }
                        }
                    }

                    formula = current.v2;
                    formula += "," + extension + "," + overlap.value;

                    nextCoveredLabelsAndCard.add(new Triple<>(count, formula, est));
                }

            }

            currentCoveredLabelsAndCard = nextCoveredLabelsAndCard;
            nextCoveredLabelsAndCard = new HashSet<>();

        }

        alreadyCovered.addAll(currentCoveredLabelsAndCard);
    }

    public void allCycleClosingRandom(String hop, List<Triple<Integer, String, Double>> resultSet){
        String startLabelSeq, nextLabelSeq, vListString, formula;
        int count;
        Pair<String, String> overlap;
        Set<String> intersection = new HashSet<>(), unvisited = new HashSet<>();
        String[] nextVList;
        Set<String> visited;
        Set<String> allEdges = TriangleCatalogue.toVSet(queryVList);

        startLabelSeq = Catalogue.extractPath(query.topology, startVList);
        vListString = Catalogue.toVListString(startVList);
        double est = TriangleCatalogue.catalogue.get(vListString).get(startLabelSeq);

        List<String> size3decomsList = new ArrayList<>();
        size3decomsList.addAll(size3decoms);
        currentCoveredLabelsAndCard.clear();
        nextCoveredLabelsAndCard.clear();

        while (resultSet.size() < subsetSize) {
            // System.out.println(minAlreadyCovered.size());
            Triple<Integer, String, Double> current;
            current = new Triple<>(0, vListString, est);

            for (int i = 0; i < allEdges.size() - catLen; ++i) {
                // classify the nextStep by # of overlapped edges
                // Integer indicates whether we use extension rate or not
                // 0 means do not use, 1 means use extension rate
                // 2 means a triangle
                List<Pair<Integer, String>> size3decomsOverlap1 = new ArrayList<>();
                List<Pair<Integer, String>> size3decomsOverlap2 = new ArrayList<>();
                for (String extension : size3decoms) {
                    nextVList = extension.split(";");

                    intersection.clear();
                    visited = TriangleCatalogue.toVSet(current.v2);
                    intersection.addAll(visited);
                    intersection.retainAll(Arrays.asList(nextVList));

                    if (intersection.size() == 0 || intersection.size() == nextVList.length) continue;

                    //unvisited edges
                    unvisited.clear();
                    unvisited.addAll(Arrays.asList(nextVList));
                    unvisited.removeAll(visited);

                    if (method.contains("acyclic") || method.contains("baseline")) {
                        if (intersection.size() == 2) size3decomsOverlap1.add(new Pair<>(0, extension));
                        else size3decomsOverlap2.add(new Pair<>(0, extension));
                    } else {
                        // check for triangles, if it properly closes a cycle, add to Type1; otherwise, continue (i.e. skip)
                        Shape shape = getType(TriangleCatalogue.toVList(extension));
                        int intersecSize = intersection(current.v2, unvisited);
                        if (shape == Shape.TRIG && intersection.size() == 2 && intersecSize == 3) {
                            continue;
                        } else if (shape == Shape.TRIG) {
                            if (method.contains("onlyExtensionRate")) {
                                if (intersection.size() == 2) continue;
                                else size3decomsOverlap2.add(new Pair<>(2, extension));
                            } else {
                                if (intersection.size() == 2) size3decomsOverlap1.add(new Pair<>(2, extension));
                                else size3decomsOverlap2.add(new Pair<>(2, extension));
                            }
                        }
                        // check for 3-stars, if does not close a cycle, add to Type3; otherwise, continue (i.e. skip)
                        else if (shape == Shape.STAR && intersecSize >= 2) {
                            continue;
                        }
                        // check for 3-paths, if it does not close a cycle, add to Type3;
                        // if it properly closes a cycle, add to Type2; otherwise, continue
                        else if (shape == Shape.PATH && intersecSize >= 2) {
                            if (intersection.size() == 1) {
                                Map<Integer, Integer> occurrences = new HashMap<>();
                                for (Integer v : TriangleCatalogue.toVList(extension)) {
                                    occurrences.put(v, occurrences.getOrDefault(v, 0) + 1);
                                }
                                String[] extensionEdge = intersection.stream().toArray(String[]::new);
                                Integer v1 = Integer.parseInt(extensionEdge[0].split("-")[0]);
                                Integer v2 = Integer.parseInt(extensionEdge[0].split("-")[1]);
                                if (method.contains("triangleClosing") && !(occurrences.get(v1) == 2 && occurrences.get(v2) == 2)) {
                                    // triangle cycle-closing simulation
                                    size3decomsOverlap2.add(new Pair<>(1, extension));
                                } else if (!method.contains("triangleClosing") && occurrences.get(v1) == 2 && occurrences.get(v2) == 2) {
                                    // cycle-closing simulation
                                    size3decomsOverlap2.add(new Pair<>(1, extension));
                                }
                            }
                        } else {
                            if (intersection.size() == 2) size3decomsOverlap1.add(new Pair<>(0, extension));
                            else size3decomsOverlap2.add(new Pair<>(0, extension));
                        }
                    }
                }

                Pair<Integer, String> extensionPair;
                String extension;

                List<List<Pair<Integer, String>>> size3decomsOverlap = new ArrayList<>();
                if (!hop.contains("all")) {
                    if (hop.contains("min")) {
                        size3decomsOverlap.add(size3decomsOverlap1);
                        size3decomsOverlap.add(size3decomsOverlap2);
                    } else if (hop.contains("max")){
                        size3decomsOverlap.add(size3decomsOverlap2);
                        size3decomsOverlap.add(size3decomsOverlap1);
                    }
                    if (size3decomsOverlap.get(0).size() > 0) {
                        Collections.shuffle(size3decomsOverlap1);
                        extensionPair = size3decomsOverlap1.get(0);
                        extension = extensionPair.getValue();
                    } else {
                        Collections.shuffle(size3decomsOverlap2);
                        extensionPair = size3decomsOverlap2.get(0);
                        extension = extensionPair.getValue();
                    }
                } else {
                    size3decomsOverlap1.addAll(size3decomsOverlap2);
                    Collections.shuffle(size3decomsOverlap1);
                    extensionPair = size3decomsOverlap1.get(0);
                    extension = extensionPair.getValue();
                }

                nextLabelSeq = Catalogue.extractPath(query.topology, TriangleCatalogue.toVList(extension));
                nextVList = extension.split(";");
                visited = TriangleCatalogue.toVSet(current.v2);
                overlap = TriangleCatalogue.getOverlap(visited, nextLabelSeq, extension);
                est = current.v3;
                count = current.v1;

                intersection.clear();
                intersection.addAll(Arrays.asList(nextVList));
                intersection.removeAll(visited);

                if (extensionPair.getKey() == 0) {
                    if (TriangleCatalogue.catalogue.get(overlap.value) == null) break;
                    est /= TriangleCatalogue.catalogue.get(overlap.value).get(overlap.key);
                    est *= TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq);
                } else if (extensionPair.getKey() == 1){
                    // if (TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq + "*") == 0.0) break;
                    if (method.contains("triangleClosing")) {
                        if (TriangleCatalogue.catalogue.get("*" + extension).get(nextLabelSeq) == 0.0) break;
                        est /= TriangleCatalogue.catalogue.get(overlap.value).get(overlap.key);
                        est *= TriangleCatalogue.catalogue.get("*" + extension).get(nextLabelSeq);
                        count += 1;
                    } else if (method.contains("midEdgeClosing")) {
                        est /= TriangleCatalogue.catalogue.get(overlap.value.split(";")[0]).get(overlap.key.split("->")[0]);
                        est /= TriangleCatalogue.catalogue.get(overlap.value.split(";")[1]).get(overlap.key.split("->")[1]);
                        est *= TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq);
                    } else {
                        est *= TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq + "*");
                        count += 1;
                    }
                } else if (method.contains("onlyExtensionRate")) {
                    if (intersection.size() == 1) {
                        if (nextVList[0] == intersection.toArray()[0]) {
                            // if (TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq + "*") == 0.0) break;
                            est *= TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq + "*");
                        } else if (nextVList[1] == intersection.toArray()[0]) {
                            // if (TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq + "**") == 0.0) break;
                            est *= TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq + "**");
                        } else {
                            // if (TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq + "***") == 0.0) break;
                            est *= TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq + "***");
                        }
                        count += 1;
                    } else {
                        // System.out.println(intersection);
                        break;
                    }
                } else {
                    est /= TriangleCatalogue.catalogue.get(overlap.value).get(overlap.key);
                    est *= TriangleCatalogue.catalogue.get(extension).get(nextLabelSeq);
                }
                formula = current.v2;
                formula += "," + extension + "," + overlap.value;

                current = new Triple<>(count, formula, est);
                if (Catalogue.toVSet(current.v2).equals(allEdges)) {
                    minAlreadyCovered.add(current);
                    break;
                }
            }
        }
    }

    public void run() {
        if (!randomSampling) allCycleClosing();
        else {
            allCycleClosingRandom("all", allAlreadyCovered);
            allCycleClosingRandom("min", minAlreadyCovered);
            allCycleClosingRandom("max", maxAlreadyCovered);
        }
    }


    

    public EstimateParallelCyclic(int threadId, Query query, int catLen, String queryVList,
                                  Integer[] size3entry, Set<String> size3decoms,
                                  String method, Type cycleClosingType, boolean randomSampling, int subsetSize) {
        this.threadId = threadId;
        this.query = query;
        this.catLen = catLen;
        this.queryVList = queryVList;
        this.startVList = size3entry;
        this.size3decoms = size3decoms;
        this.method = method;
        this.cycleClosingType = cycleClosingType;
        this.randomSampling = randomSampling;
        this.subsetSize = subsetSize;
    }

}