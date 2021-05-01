package IMDB;

import Common.Pair;
import Common.Path;
import Common.Query;
import Common.Topology;

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
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.TreeSet;

public class TrueCardinality6 {
    private Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    private Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();

    private Map<Integer, Integer> vid2prodYear = new HashMap<>();

    private final Set<Integer> LINK_TYPE_SET = new HashSet<>(Arrays.asList(Labels.LINK_TYPES));
    private final Set<Integer> INFO_TYPE_SET = new HashSet<>(Arrays.asList(Labels.INFO_TYPES));
    private final Set<Integer> COMP_TYPE_SET = new HashSet<>(Arrays.asList(Labels.COMPANY_TYPES));
    private final Set<Integer> ROLE_TYPE_SET = new HashSet<>(Arrays.asList(Labels.ROLE_TYPES));
    private Set<Integer> AKA = new HashSet<>();
    private Set<Integer> EPI = new HashSet<>();
    private Set<Integer> KEYWORD = new HashSet<>();

    private Map<Integer, Map<String, List<String>>> v2path2vList = new HashMap<>();
    private Map<Integer, Map<String, List<String>>> v2path2vListPivot = new HashMap<>();

    private Map<Integer, Map<Path, List<List<Integer>>>> topVList = new HashMap<>();
    private Map<Integer, Map<Path, List<List<Integer>>>> midVList = new HashMap<>();
    private Map<Integer, Map<Path, List<List<Integer>>>> bottomVList = new HashMap<>();

    private void flush(String fileName, boolean pivot) throws Exception {
        Map<Integer, Map<String, List<String>>> vertexLists;
        if (pivot) {
            vertexLists = v2path2vListPivot;
        } else {
            vertexLists = v2path2vList;
        }

        BufferedWriter fileWriter = new BufferedWriter(new FileWriter(fileName));

        int numV = vertexLists.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        StringJoiner sj;
        for (Integer startVertex : vertexLists.keySet()) {
            sj = new StringJoiner(",");
            sj.add(startVertex.toString());
            for (String path : vertexLists.get(startVertex).keySet()) {
                sj.add(path);
                sj.add(Integer.toString(vertexLists.get(startVertex).get(path).size()));
                for (String vertexList : vertexLists.get(startVertex).get(path)) {
                    sj.add(vertexList);
                }
            }

            fileWriter.write(sj.toString() + "\n");

            progress += 100.0 / numV;
            System.out.print("\rSaving " + fileName + ": " + (int) progress + "%");
        }
        fileWriter.close();

        endTime = System.currentTimeMillis();
        System.out.println("\nSaving " + fileName + ": " + ((endTime - startTime) / 1000.0) + " sec");

        vertexLists.clear();
    }

    private void starTop() {
        Set<Integer> intersection = new HashSet<>();

        String vertexList, path;

        int numV = src2label2dest.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer title1 : src2label2dest.keySet()) {
            progress += 100.0 / numV;
            System.out.print("\rProcessing STAR_TOP: " + (int) progress + "%");

            if (!src2label2dest.get(title1).containsKey(Labels.ALSO_KNOWN_AS)) continue;

            intersection.clear();
            for (Integer linkType : LINK_TYPE_SET) {
                if (src2label2dest.get(title1).containsKey(linkType)) {
                    intersection.add(linkType);
                }
            }
            if (intersection.isEmpty()) continue;

            for (Integer linkType : intersection) {
                for (Integer title2 : src2label2dest.get(title1).get(linkType)) {
                    for (Integer aka : src2label2dest.get(title1).get(Labels.ALSO_KNOWN_AS)) {
                        path = Labels.ALSO_KNOWN_AS + "->" + linkType;
                        vertexList = title1 + "-" + aka + ";" + title1 + "-" + title2;

                        v2path2vList.putIfAbsent(title1, new HashMap<>());
                        v2path2vList.get(title1).putIfAbsent(path, new ArrayList<>());
                        v2path2vList.get(title1).get(path).add(vertexList);
                    }
                }
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nProcessing STAR_TOP: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void starMid() {
        Set<Integer> intersection1 = new HashSet<>();
        Set<Integer> intersection2 = new HashSet<>();

        String vertexList, path;

        int numV = src2label2dest.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer title : src2label2dest.keySet()) {
            progress += 100.0 / numV;
            System.out.print("\rProcessing STAR_MID: " + (int) progress + "%");

            if (!dest2label2src.containsKey(title)) continue;

            intersection1.clear();
            for (Integer compType : COMP_TYPE_SET) {
                if (dest2label2src.get(title).containsKey(compType)) {
                    intersection1.add(compType);
                }
            }
            if (intersection1.isEmpty()) continue;

            intersection2.clear();
            for (Integer infoType : INFO_TYPE_SET) {
                if (src2label2dest.get(title).containsKey(infoType)) {
                    intersection2.add(infoType);
                }
            }
            if (intersection2.isEmpty()) continue;

            for (Integer compType : intersection1) {
                for (Integer infoType : intersection2) {
                    for (Integer comp : dest2label2src.get(title).get(compType)) {
                        for (Integer info : src2label2dest.get(title).get(infoType)) {
                            path = compType + "->" + infoType;
                            vertexList = comp + "-" + title + ";" + title + "-" + info;

                            v2path2vList.putIfAbsent(title, new HashMap<>());
                            v2path2vList.get(title).putIfAbsent(path, new ArrayList<>());
                            v2path2vList.get(title).get(path).add(vertexList);
                        }
                    }
                }
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nProcessing STAR_MID: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void starBottom() {
        Set<Integer> intersection1 = new HashSet<>();
        Set<Integer> intersection2 = new HashSet<>();

        String vertexList, path;

        int numV = src2label2dest.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer title1 : src2label2dest.keySet()) {
            progress += 100.0 / numV;
            System.out.print("\rProcessing STAR_BOTTOM: " + (int) progress + "%");

            intersection1.clear();
            for (Integer linkType : LINK_TYPE_SET) {
                if (src2label2dest.get(title1).containsKey(linkType)) {
                    intersection1.add(linkType);
                }
            }
            if (intersection1.isEmpty()) continue;

            intersection2.clear();
            for (Integer infoType : INFO_TYPE_SET) {
                if (src2label2dest.get(title1).containsKey(infoType)) {
                    intersection2.add(infoType);
                }
            }
            if (intersection2.isEmpty()) continue;

            for (Integer linkType : intersection1) {
                for (Integer infoType : intersection2) {
                    for (Integer title2 : src2label2dest.get(title1).get(linkType)) {
                        for (Integer info : src2label2dest.get(title1).get(infoType)) {
                            path = linkType + "->" + infoType;
                            vertexList = title1 + "-" + title2 + ";" + title1 + "-" + info;

                            v2path2vList.putIfAbsent(title1, new HashMap<>());
                            v2path2vList.get(title1).putIfAbsent(path, new ArrayList<>());
                            v2path2vList.get(title1).get(path).add(vertexList);

                            v2path2vListPivot.putIfAbsent(title2, new HashMap<>());
                            v2path2vListPivot.get(title2).putIfAbsent(path, new ArrayList<>());
                            v2path2vListPivot.get(title2).get(path).add(vertexList);
                        }
                    }
                }
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nProcessing STAR_BOTTOM: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void pathLeft() {
        Set<Integer> intersection = new HashSet<>();

        String vertexList, path;

        int numV = dest2label2src.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer title1 : dest2label2src.keySet()) {
            progress += 100.0 / numV;
            System.out.print("\rProcessing PATH_LEFT: " + (int) progress + "%");

            if (!src2label2dest.containsKey(title1)) continue;
            if (!dest2label2src.get(title1).containsKey(Labels.IS_KEYWORD_OF)) continue;

            intersection.clear();
            for (Integer linkType : LINK_TYPE_SET) {
                if (src2label2dest.get(title1).containsKey(linkType)) {
                    intersection.add(linkType);
                }
            }
            if (intersection.isEmpty()) continue;

            for (Integer linkType : intersection) {
                for (Integer keyword : dest2label2src.get(title1).get(Labels.IS_KEYWORD_OF)) {
                    for (Integer title2 : src2label2dest.get(title1).get(linkType)) {
                        path = Labels.IS_KEYWORD_OF + "->" + linkType;
                        vertexList = keyword + "-" + title1 + ";" + title1 + "-" + title2;

                        v2path2vList.putIfAbsent(title2, new HashMap<>());
                        v2path2vList.get(title2).putIfAbsent(path, new ArrayList<>());
                        v2path2vList.get(title2).get(path).add(vertexList);

                        v2path2vListPivot.putIfAbsent(title1, new HashMap<>());
                        v2path2vListPivot.get(title1).putIfAbsent(path, new ArrayList<>());
                        v2path2vListPivot.get(title1).get(path).add(vertexList);
                    }
                }
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nProcessing PATH_LEFT: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void pathMid() {
        Set<Integer> intersection = new HashSet<>();

        String vertexList, path;

        int numV = src2label2dest.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer name : src2label2dest.keySet()) {
            progress += 100.0 / numV;
            System.out.print("\rProcessing PATH_MID: " + (int) progress + "%");

            intersection.clear();
            for (Integer compType : ROLE_TYPE_SET) {
                if (src2label2dest.get(name).containsKey(compType)) {
                    intersection.add(compType);
                }
            }
            if (intersection.isEmpty()) continue;

            for (Integer roleType1 : intersection) {
                for (Integer roleType2 : intersection) {
                    if (roleType1.equals(roleType2)) continue;

                    for (Integer title1 : src2label2dest.get(name).get(roleType1)) {
                        for (Integer title2 : src2label2dest.get(name).get(roleType2)) {
                            path = roleType1 + "->" + roleType2;
                            vertexList = name + "-" + title1 + ";" + name + "-" + title2;

                            v2path2vList.putIfAbsent(name, new HashMap<>());
                            v2path2vList.get(name).putIfAbsent(path, new ArrayList<>());
                            v2path2vList.get(name).get(path).add(vertexList);
                        }
                    }
                }
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nProcessing PATH_MID: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void tShapeLeft() {
        Set<Integer> intersection = new HashSet<>();

        String vertexList, path;

        int numV = src2label2dest.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer name : src2label2dest.keySet()) {
            progress += 100.0 / numV;
            System.out.print("\rProcessing T_LEFT: " + (int) progress + "%");

            if (!src2label2dest.get(name).containsKey(Labels.ALSO_KNOWN_AS)) continue;

            intersection.clear();
            for (Integer roleType : ROLE_TYPE_SET) {
                if (src2label2dest.get(name).containsKey(roleType)) {
                    intersection.add(roleType);
                }
            }
            if (intersection.isEmpty()) continue;

            for (Integer roleType : intersection) {
                for (Integer title : src2label2dest.get(name).get(roleType)) {
                    for (Integer aka : src2label2dest.get(name).get(Labels.ALSO_KNOWN_AS)) {
                        path = Labels.ALSO_KNOWN_AS + "->" + roleType;
                        vertexList = name + "-" + aka + ";" + name + "-" + title;

                        v2path2vList.putIfAbsent(title, new HashMap<>());
                        v2path2vList.get(title).putIfAbsent(path, new ArrayList<>());
                        v2path2vList.get(title).get(path).add(vertexList);
                    }
                }
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nProcessing T_LEFT: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void tShapeRight() {
        Set<Integer> intersection1 = new HashSet<>();
        Set<Integer> intersection2 = new HashSet<>();

        String vertexList, path;

        int numV = src2label2dest.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer title1 : src2label2dest.keySet()) {
            progress += 100.0 / numV;
            System.out.print("\rProcessing T_RIGHT: " + (int) progress + "%");

            if (!dest2label2src.containsKey(title1)) continue;

            intersection1.clear();
            for (Integer linkType : LINK_TYPE_SET) {
                if (src2label2dest.get(title1).containsKey(linkType)) {
                    intersection1.add(linkType);
                }
            }
            if (intersection1.isEmpty()) continue;

            intersection2.clear();
            for (Integer compType : COMP_TYPE_SET) {
                if (dest2label2src.get(title1).containsKey(compType)) {
                    intersection2.add(compType);
                }
            }
            if (intersection2.isEmpty()) continue;

            for (Integer linkType : intersection1) {
                for (Integer compType : intersection2) {
                    for (Integer title2 : src2label2dest.get(title1).get(linkType)) {
                        for (Integer comp : dest2label2src.get(title1).get(compType)) {
                            path = compType + "->" + linkType;
                            vertexList = comp + "-" + title1 + ";" + title1 + "-" + title2;

                            v2path2vList.putIfAbsent(title2, new HashMap<>());
                            v2path2vList.get(title2).putIfAbsent(path, new ArrayList<>());
                            v2path2vList.get(title2).get(path).add(vertexList);
                        }
                    }
                }
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nProcessing T_RIGHT: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void piLeft() {
        Set<Integer> intersection = new HashSet<>();

        String vertexList, path;

        int numV = src2label2dest.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer title : src2label2dest.keySet()) {
            progress += 100.0 / numV;
            System.out.print("\rProcessing PI_LEFT: " + (int) progress + "%");

            if (!src2label2dest.get(title).containsKey(Labels.ALSO_KNOWN_AS)) continue;

            intersection.clear();
            for (Integer infoType : INFO_TYPE_SET) {
                if (src2label2dest.get(title).containsKey(infoType)) {
                    intersection.add(infoType);
                }
            }
            if (intersection.isEmpty()) continue;

            for (Integer infoType : intersection) {
                for (Integer info : src2label2dest.get(title).get(infoType)) {
                    for (Integer aka : src2label2dest.get(title).get(Labels.ALSO_KNOWN_AS)) {
                        path = Labels.ALSO_KNOWN_AS + "->" + infoType;
                        vertexList = title + "-" + aka + ";" + title + "-" + info;

                        v2path2vList.putIfAbsent(title, new HashMap<>());
                        v2path2vList.get(title).putIfAbsent(path, new ArrayList<>());
                        v2path2vList.get(title).get(path).add(vertexList);
                    }
                }
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nProcessing PI_LEFT: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void piMid() {
        Set<Integer> intersection1 = new HashSet<>();
        Set<Integer> intersection2 = new HashSet<>();

        String vertexList, path;

        int numV = dest2label2src.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer title1 : dest2label2src.keySet()) {
            progress += 100.0 / numV;
            System.out.print("\rProcessing PI_MID: " + (int) progress + "%");

            if (!src2label2dest.containsKey(title1)) continue;

            intersection1.clear();
            for (Integer linkType : LINK_TYPE_SET) {
                if (dest2label2src.get(title1).containsKey(linkType)) {
                    intersection1.add(linkType);
                }
            }
            if (intersection1.isEmpty()) continue;

            intersection2.clear();
            for (Integer linkType : LINK_TYPE_SET) {
                if (src2label2dest.get(title1).containsKey(linkType)) {
                    intersection2.add(linkType);
                }
            }
            if (intersection2.isEmpty()) continue;

            for (Integer linkType1 : intersection1) {
                for (Integer linkType2 : intersection2) {
                    for (Integer title2 : dest2label2src.get(title1).get(linkType1)) {
                        for (Integer title3 : src2label2dest.get(title1).get(linkType2)) {
                            path = linkType1 + "->" + linkType2;
                            vertexList = title2 + "-" + title1 + ";" + title1 + "-" + title3;

                            v2path2vList.putIfAbsent(title1, new HashMap<>());
                            v2path2vList.get(title1).putIfAbsent(path, new ArrayList<>());
                            v2path2vList.get(title1).get(path).add(vertexList);
                        }
                    }
                }
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nProcessing PI_MID: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void piRight() {
        Set<Integer> intersection = new HashSet<>();

        String vertexList, path;

        int numV = dest2label2src.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer title : dest2label2src.keySet()) {
            progress += 100.0 / numV;
            System.out.print("\rProcessing PI_RIGHT: " + (int) progress + "%");

            if (!dest2label2src.get(title).containsKey(Labels.IS_KEYWORD_OF)) continue;

            intersection.clear();
            for (Integer roleType : ROLE_TYPE_SET) {
                if (dest2label2src.get(title).containsKey(roleType)) {
                    intersection.add(roleType);
                }
            }
            if (intersection.isEmpty()) continue;

            for (Integer roleType : intersection) {
                for (Integer name : dest2label2src.get(title).get(roleType)) {
                    for (Integer keyword : dest2label2src.get(title).get(Labels.IS_KEYWORD_OF)) {
                        path = roleType + "->" + Labels.IS_KEYWORD_OF;
                        vertexList = name + "-" + title + ";" + keyword + "-" + title;

                        v2path2vList.putIfAbsent(title, new HashMap<>());
                        v2path2vList.get(title).putIfAbsent(path, new ArrayList<>());
                        v2path2vList.get(title).get(path).add(vertexList);
                    }
                }
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nProcessing PI_RIGHT: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void fork33Left() {
        Set<Integer> intersection = new HashSet<>();

        String vertexList, path;

        int numV = dest2label2src.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer title : dest2label2src.keySet()) {
            progress += 100.0 / numV;
            System.out.print("\rProcessing FORK33_LEFT: " + (int) progress + "%");

            if (!dest2label2src.get(title).containsKey(Labels.IS_KEYWORD_OF)) continue;

            for (Integer keyword : dest2label2src.get(title).get(Labels.IS_KEYWORD_OF)) {
                path = Labels.IS_KEYWORD_OF.toString();
                vertexList = keyword + "-" + title;

                v2path2vList.putIfAbsent(title, new HashMap<>());
                v2path2vList.get(title).putIfAbsent(path, new ArrayList<>());
                v2path2vList.get(title).get(path).add(vertexList);
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nProcessing FORK33_LEFT: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void fork33Middle() {
        Set<Integer> intersection1 = new HashSet<>();
        Set<Integer> intersection2 = new HashSet<>();

        String vertexList, path;

        int numV = src2label2dest.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer title2 : src2label2dest.keySet()) {
            progress += 100.0 / numV;
            System.out.print("\rProcessing FORK33_MIDDLE: " + (int) progress + "%");

            if (!dest2label2src.containsKey(title2)) continue;

            intersection1.clear();
            for (Integer linkType : LINK_TYPE_SET) {
                if (dest2label2src.get(title2).containsKey(linkType)) {
                    intersection1.add(linkType);
                }
            }
            if (intersection1.isEmpty()) continue;

            intersection2.clear();
            for (Integer linkType : LINK_TYPE_SET) {
                if (src2label2dest.get(title2).containsKey(linkType)) {
                    intersection2.add(linkType);
                }
            }
            if (intersection2.isEmpty()) continue;

            for (Integer linkType1 : intersection1) {
                for (Integer linkType2 : intersection2) {
                    if (linkType1.equals(linkType2)) continue;

                    for (Integer title1 : dest2label2src.get(title2).get(linkType1)) {
                        for (Integer title3 : src2label2dest.get(title2).get(linkType2)) {
                            path = linkType1 + "->" + linkType2;
                            vertexList = title1 + "-" + title2 + ";" + title2 + "-" + title3;

                            v2path2vList.putIfAbsent(title2, new HashMap<>());
                            v2path2vList.get(title2).putIfAbsent(path, new ArrayList<>());
                            v2path2vList.get(title2).get(path).add(vertexList);
                        }
                    }
                }
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nProcessing FORK33_MIDDLE: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void fork33Right() {
        Set<Integer> intersection1 = new HashSet<>();
        Set<Integer> intersection2 = new HashSet<>();

        String vertexList, path;

        int numV = src2label2dest.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer title : src2label2dest.keySet()) {
            progress += 100.0 / numV;
            System.out.print("\rProcessing FORK33_RIGHT: " + (int) progress + "%");

            if (!src2label2dest.get(title).containsKey(Labels.ALSO_KNOWN_AS)) continue;
            if (!dest2label2src.containsKey(title)) continue;

            intersection1.clear();
            for (Integer roleType : ROLE_TYPE_SET) {
                if (dest2label2src.get(title).containsKey(roleType)) {
                    intersection1.add(roleType);
                }
            }
            if (intersection1.isEmpty()) continue;

            intersection2.clear();
            for (Integer compType : COMP_TYPE_SET) {
                if (dest2label2src.get(title).containsKey(compType)) {
                    intersection2.add(compType);
                }
            }
            if (intersection2.isEmpty()) continue;

            for (Integer roleType : intersection1) {
                for (Integer compType : intersection2) {
                    for (Integer aka : src2label2dest.get(title).get(Labels.ALSO_KNOWN_AS)) {
                        for (Integer name : dest2label2src.get(title).get(roleType)) {
                            for (Integer comp : dest2label2src.get(title).get(compType)) {
                                path = compType + "->" + roleType + "->" + Labels.ALSO_KNOWN_AS;
                                vertexList = comp + "-" + title + ";" + name + "-" + title + ";"
                                    + title + "-" + aka;

                                v2path2vList.putIfAbsent(title, new HashMap<>());
                                v2path2vList.get(title).putIfAbsent(path, new ArrayList<>());
                                v2path2vList.get(title).get(path).add(vertexList);
                            }
                        }
                    }
                }
            }

//            for (Integer aka : src2label2dest.get(title).get(Labels.ALSO_KNOWN_AS)) {
//                path = Labels.ALSO_KNOWN_AS.toString();
//                vertexList = aka + "-" + title;
//
//                v2path2vList.putIfAbsent(title, new HashMap<>());
//                v2path2vList.get(title).putIfAbsent(path, new ArrayList<>());
//                v2path2vList.get(title).get(path).add(vertexList);
//            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nProcessing FORK33_RIGHT: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public void traverseAndSave() throws Exception {
//        starTop();
//        flush("star_top.csv", false);
//        starMid();
//        flush("star_mid.csv", false);
//        starBottom();
//        flush("star_bottom.csv", false);
//        flush("star_bottom_pivot.csv", true);

//        pathLeft();
//        flush("path_left.csv", false);
//        flush("path_left_pivot.csv", true);
//        pathMid();
//        flush("path_mid.csv", false);

//        tShapeLeft();
//        flush("t_left.csv", false);
//        tShapeRight();
//        flush("t_right.csv", false);
//
//        piLeft();
//        flush("pi_left.csv", false);
//        piMid();
//        flush("pi_mid.csv", false);
//        piRight();
//        flush("pi_right.csv", false);

        fork33Left();
        flush("fork33_left.csv", false);
        fork33Middle();
        flush("fork33_middle.csv", false);
        fork33Right();
        flush("fork33_right.csv", false);
    }

    private void load(String fileName1, String fileName2, String fileName3) throws Exception {
        String[] info, labelList, vertexList;
        int centralVertex, numVertexLists;
        Path path;
        List<Integer> vertexPath;
        String fileName = fileName1;
        Map<Integer, Map<Path, List<List<Integer>>>> vList = topVList;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (int h = 0; h < 3; ++h) {
            if (h == 1) {
                fileName = fileName2;
                vList = midVList;
            } else if (h == 2) {
                fileName = fileName3;
                vList = bottomVList;
            }

            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            String line = reader.readLine();
            while (null != line) {
                info = line.split(",");
                centralVertex = Integer.parseInt(info[0]);
                vList.putIfAbsent(centralVertex, new HashMap<>());

                int k = 1;
                while (k < info.length) {
                    labelList = info[k].split("->");
                    path = new Path(new ArrayList<>());
                    for (int i = 0; i < labelList.length; ++i) {
                        path.append(Integer.parseInt(labelList[i]));
                    }
                    vList.get(centralVertex).putIfAbsent(path, new ArrayList<>());

                    numVertexLists = Integer.parseInt(info[k + 1]);
                    k++;
                    for (int i = 1; i <= numVertexLists; ++i) {
                        vertexList = info[k + i].split(";");
                        vertexPath = new ArrayList<>();
                        for (int j = 0; j < vertexList.length; ++j) {
                            for (String v : vertexList[j].split("-")) {
                                vertexPath.add(Integer.parseInt(v));
                            }
                        }
                        vList.get(centralVertex).get(path).add(vertexPath);
                    }

                    k += numVertexLists + 1;
                }

                line = reader.readLine();
            }
            reader.close();
        }

        endTime = System.currentTimeMillis();
        System.out.println("Loading " + fileName1 + ", " + fileName2 + " & " + fileName3 + ": " +
                ((endTime - startTime) / 1000.0) + " sec");
    }

    private void loadProperties(String propFile) throws Exception {
        String[] properties;

        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader tsvReader = new BufferedReader(new FileReader(propFile));
        tsvReader.readLine(); // Header

        String line = tsvReader.readLine();
        while (null != line) {
            properties = line.split("\t");
            if (!properties[Labels.PROD_YEAR_INDEX].isEmpty()) {
                vid2prodYear.put(
                    Integer.parseInt(properties[0]), Integer.parseInt(properties[Labels.PROD_YEAR_INDEX])
                );
            }
            line = tsvReader.readLine();
        }
        tsvReader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Loading properties: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private boolean applyFilter(List<Integer> sample, List<Pair<String, Integer>> filters)
        throws Exception {
        int vid, literal;
        String operator;

        boolean qualified = true;

        for (int i = 0; i < sample.size(); ++i) {
            operator = filters.get(i).key;
            if (operator.isEmpty()) continue;

            vid = sample.get(i);
            literal = filters.get(i).value;

            if (!vid2prodYear.containsKey(vid)) continue;

            switch (operator) {
                case "<":
                    qualified = qualified && vid2prodYear.get(vid) < literal;
                    break;
                case ">":
                    qualified = qualified && vid2prodYear.get(vid) > literal;
                    break;
                case "<=":
                    qualified = qualified && vid2prodYear.get(vid) <= literal;
                    break;
                case ">=":
                    qualified = qualified && vid2prodYear.get(vid) >= literal;
                    break;
                case "=":
                    qualified = qualified && vid2prodYear.get(vid) == literal;
                    break;
                default:
                    throw new Exception("ERROR: unrecognized operator: " + operator);
            }
        }

        return qualified;
    }

    private List<Pair<String, Integer>> extractFilters(
            List<Pair<String, Integer>> filters, int[] vertexList) {

        List<Pair<String, Integer>> extracted = new ArrayList<>();
        for (Integer rVertex : vertexList) {
            extracted.add(filters.get(rVertex));
        }

        return extracted;
    }

    private Path extractPath(Topology topology, int[] vertexList, List<Set<Integer>> labels) {
        Path path = new Path(new ArrayList<>());
        for (int i = 0; i < labels.size(); i++) {
            for (Integer label : topology.outgoing.get(vertexList[i * 2]).keySet()) {
                if (labels.get(i).contains(label)) {
                    for (Integer next : topology.outgoing.get(vertexList[i * 2]).get(label)) {
                        if (next.equals(vertexList[i * 2 + 1])) {
                            path.append(label);
                        }
                    }
                }
            }
        }

        return path;
    }

    private long count(Query query, List<List<Set<Integer>>> labelSets, int[][] vertexLists) throws Exception {
        Path path1, path2, path3;
        List<Pair<String, Integer>> filters1, filters2, filters3;

        path1 = extractPath(query.topology, vertexLists[0], labelSets.get(0));
        filters1 = extractFilters(query.filters, vertexLists[0]);

        path2 = extractPath(query.topology, vertexLists[1], labelSets.get(1));
        filters2 = extractFilters(query.filters, vertexLists[1]);

        path3 = extractPath(query.topology, vertexLists[2], labelSets.get(2));
        filters3 = extractFilters(query.filters, vertexLists[2]);

        long sum = 0;
        for (Integer title : topVList.keySet()) {
            if (!topVList.get(title).containsKey(path1)) continue;
            if (!midVList.containsKey(title)) continue;
            if (!midVList.get(title).containsKey(path2)) continue;
            if (!bottomVList.containsKey(title)) continue;
            if (!bottomVList.get(title).containsKey(path3)) continue;

            long topNumQualified = 0;
            long midNumQualified = 0;
            long bottomNumQualified = 0;

            for (List<Integer> pathInstance : topVList.get(title).get(path1)) {
                if (applyFilter(pathInstance, filters1)) topNumQualified++;
            }

            for (List<Integer> pathInstance : midVList.get(title).get(path2)) {
                if (applyFilter(pathInstance, filters2)) midNumQualified++;
            }

            for (List<Integer> pathInstance : bottomVList.get(title).get(path3)) {
                if (applyFilter(pathInstance, filters3)) bottomNumQualified++;
            }

            sum += topNumQualified * midNumQualified * bottomNumQualified;
        }

        return sum;
    }

    private Pair<Integer, Integer> getSideIndices(int[] vertexList) {
        TreeMap<Integer, Integer> sides = new TreeMap<>();
        for (int i = 0; i < vertexList.length; ++i) {
            if (sides.containsKey(vertexList[i])) {
                sides.remove(vertexList[i]);
            } else {
                sides.put(vertexList[i], i);
            }
        }

        if (sides.firstKey() < sides.lastKey()) {
            return new Pair<>(sides.get(sides.firstKey()), sides.get(sides.lastKey()));
        } else {
            return new Pair<>(sides.get(sides.lastKey()), sides.get(sides.firstKey()));
        }
    }

    private long countBySides(Query query, List<List<Set<Integer>>> labelSets, int[][] vertexLists)
            throws Exception {
        Path path1, path2, path3;
        List<Pair<String, Integer>> filters1, filters2, filters3;

        path1 = extractPath(query.topology, vertexLists[0], labelSets.get(0));
        filters1 = extractFilters(query.filters, vertexLists[0]);

        path2 = extractPath(query.topology, vertexLists[1], labelSets.get(1));
        filters2 = extractFilters(query.filters, vertexLists[1]);
        Pair<Integer, Integer> sideIndices = getSideIndices(vertexLists[1]);

        path3 = extractPath(query.topology, vertexLists[2], labelSets.get(2));
        filters3 = extractFilters(query.filters, vertexLists[2]);

        Integer leftV, rightV;

        long sum = 0;
        for (Integer title : midVList.keySet()) {
            if (!midVList.get(title).containsKey(path2)) continue;

            for (List<Integer> vList : midVList.get(title).get(path2)) {
                if (!applyFilter(vList, filters2)) continue;

                leftV = vList.get(sideIndices.key);
                rightV = vList.get(sideIndices.value);

                if (!topVList.containsKey(leftV)) continue;
                if (!topVList.get(leftV).containsKey(path1)) continue;
                if (!bottomVList.containsKey(rightV)) continue;
                if (!bottomVList.get(rightV).containsKey(path3)) continue;

                long topNumQualified = 0;
                long bottomNumQualified = 0;

                for (List<Integer> pathInstance : topVList.get(leftV).get(path1)) {
                    if (applyFilter(pathInstance, filters1)) topNumQualified++;
                }

                for (List<Integer> pathInstance : bottomVList.get(rightV).get(path3)) {
                    if (applyFilter(pathInstance, filters3)) bottomNumQualified++;
                }

                sum += topNumQualified * bottomNumQualified;
            }
        }

        return sum;
    }

    private void evalStar(List<Query> queries) throws Exception {
        load("star_top.csv", "star_mid.csv", "star_bottom.csv");

        long startTime = System.currentTimeMillis();
        long endTime;

        int numQueries = queries.size();
        double progress = 0;

        List<List<Set<Integer>>> labelSets = new ArrayList<>();
        int[][] vertexLists = new int[3][];

        vertexLists[0] = new int[]{1, 2, 1, 3};

        labelSets.add(new ArrayList<>());
        labelSets.get(0).add(AKA);
        labelSets.get(0).add(LINK_TYPE_SET);

        vertexLists[1] = new int[]{0, 1, 1, 4};

        labelSets.add(new ArrayList<>());
        labelSets.get(1).add(COMP_TYPE_SET);
        labelSets.get(1).add(INFO_TYPE_SET);

        vertexLists[2] = new int[]{1, 5, 1, 6};

        labelSets.add(new ArrayList<>());
        labelSets.get(2).add(LINK_TYPE_SET);
        labelSets.get(2).add(INFO_TYPE_SET);

        BufferedWriter resultWriter = new BufferedWriter(new FileWriter("star6_no_filters.csv"));
        long result;
        for (Query query : queries) {
            progress += 100.0 / numQueries;
            System.out.print("\rEvaluating STAR: " + (int) progress + "%");

            result = count(query, labelSets, vertexLists);
            if (result > 0) {
                resultWriter.write(query.toString() + "," + result + "\n");
            }
        }
        resultWriter.close();

        topVList.clear();
        midVList.clear();
        bottomVList.clear();

        endTime = System.currentTimeMillis();
        System.out.println("\nEvaluating STAR: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void evalPath(List<Query> queries) throws Exception {
        load("path_left.csv", "path_mid.csv", "star_bottom_pivot.csv");

        long startTime = System.currentTimeMillis();
        long endTime;

        int numQueries = queries.size();
        double progress = 0;

        List<List<Set<Integer>>> labelSets = new ArrayList<>();
        int[][] vertexLists = new int[3][];

        vertexLists[0] = new int[]{0, 1, 1, 3};

        labelSets.add(new ArrayList<>());
        labelSets.get(0).add(KEYWORD);
        labelSets.get(0).add(LINK_TYPE_SET);

        vertexLists[1] = new int[]{2, 3, 2, 5};

        labelSets.add(new ArrayList<>());
        labelSets.get(1).add(ROLE_TYPE_SET);
        labelSets.get(1).add(ROLE_TYPE_SET);

        vertexLists[2] = new int[]{4, 5, 4, 6};

        labelSets.add(new ArrayList<>());
        labelSets.get(2).add(LINK_TYPE_SET);
        labelSets.get(2).add(INFO_TYPE_SET);

        BufferedWriter resultWriter = new BufferedWriter(new FileWriter("path6_no_filters.csv"));
        long result;
        for (Query query : queries) {
            progress += 100.0 / numQueries;
            System.out.print("\rEvaluating PATH: " + (int) progress + "%");

            result = countBySides(query, labelSets, vertexLists);
            if (result > 0) {
                resultWriter.write(query.toString() + "," + result + "\n");
            }
        }
        resultWriter.close();

        topVList.clear();
        midVList.clear();
        bottomVList.clear();

        endTime = System.currentTimeMillis();
        System.out.println("\nEvaluating PATH: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void evalTShape(List<Query> queries) throws Exception {
        load("t_left.csv", "star_bottom_pivot.csv", "t_right.csv");

        long startTime = System.currentTimeMillis();
        long endTime;

        int numQueries = queries.size();
        double progress = 0;

        List<List<Set<Integer>>> labelSets = new ArrayList<>();
        int[][] vertexLists = new int[3][];

        vertexLists[0] = new int[]{0, 1, 0, 5};

        labelSets.add(new ArrayList<>());
        labelSets.get(0).add(AKA);
        labelSets.get(0).add(ROLE_TYPE_SET);

        vertexLists[1] = new int[]{4, 5, 4, 6};

        labelSets.add(new ArrayList<>());
        labelSets.get(1).add(LINK_TYPE_SET);
        labelSets.get(1).add(INFO_TYPE_SET);

        vertexLists[2] = new int[]{2, 3, 3, 5};

        labelSets.add(new ArrayList<>());
        labelSets.get(2).add(COMP_TYPE_SET);
        labelSets.get(2).add(LINK_TYPE_SET);

        BufferedWriter resultWriter = new BufferedWriter(new FileWriter("t_shape6_no_filters.csv"));
        long result;
        for (Query query : queries) {
            progress += 100.0 / numQueries;
            System.out.print("\rEvaluating T_SHAPE: " + (int) progress + "%");

            result = count(query, labelSets, vertexLists);
            if (result > 0) {
                resultWriter.write(query.toString() + "," + result + "\n");
            }
        }
        resultWriter.close();

        topVList.clear();
        midVList.clear();
        bottomVList.clear();

        endTime = System.currentTimeMillis();
        System.out.println("\nEvaluating T_SHAPE: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void evalFork(List<Query> queries) throws Exception {
        load("path_left_pivot.csv", "t_left.csv", "star_mid.csv");

        long startTime = System.currentTimeMillis();
        long endTime;

        int numQueries = queries.size();
        double progress = 0;

        List<List<Set<Integer>>> labelSets = new ArrayList<>();
        int[][] vertexLists = new int[3][];

        vertexLists[0] = new int[]{3, 4, 4, 5};

        labelSets.add(new ArrayList<>());
        labelSets.get(0).add(KEYWORD);
        labelSets.get(0).add(LINK_TYPE_SET);

        vertexLists[1] = new int[]{0, 1, 0, 4};

        labelSets.add(new ArrayList<>());
        labelSets.get(1).add(AKA);
        labelSets.get(1).add(ROLE_TYPE_SET);

        vertexLists[2] = new int[]{2, 4, 4, 6};

        labelSets.add(new ArrayList<>());
        labelSets.get(2).add(COMP_TYPE_SET);
        labelSets.get(2).add(INFO_TYPE_SET);

        BufferedWriter resultWriter = new BufferedWriter(new FileWriter("fork6_no_filters.csv"));
        long result;
        for (Query query : queries) {
            progress += 100.0 / numQueries;
            System.out.print("\rEvaluating FORK: " + (int) progress + "%");

            result = count(query, labelSets, vertexLists);
            if (result > 0) {
                resultWriter.write(query.toString() + "," + result + "\n");
            }
        }
        resultWriter.close();

        topVList.clear();
        midVList.clear();
        bottomVList.clear();

        endTime = System.currentTimeMillis();
        System.out.println("\nEvaluating FORK: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void evalPi(List<Query> queries) throws Exception {
        load("pi_left.csv", "pi_mid.csv", "pi_right.csv");

        long startTime = System.currentTimeMillis();
        long endTime;

        int numQueries = queries.size();
        double progress = 0;

        List<List<Set<Integer>>> labelSets = new ArrayList<>();
        int[][] vertexLists = new int[3][];

        vertexLists[0] = new int[]{0, 1, 0, 2};

        labelSets.add(new ArrayList<>());
        labelSets.get(0).add(AKA);
        labelSets.get(0).add(INFO_TYPE_SET);

        vertexLists[1] = new int[]{0, 3, 3, 6};

        labelSets.add(new ArrayList<>());
        labelSets.get(1).add(LINK_TYPE_SET);
        labelSets.get(1).add(LINK_TYPE_SET);

        vertexLists[2] = new int[]{4, 6, 5, 6};

        labelSets.add(new ArrayList<>());
        labelSets.get(2).add(ROLE_TYPE_SET);
        labelSets.get(2).add(KEYWORD);

        BufferedWriter resultWriter = new BufferedWriter(new FileWriter("pi6_no_filters.csv"));

        long result;
        for (Query query : queries) {
            progress += 100.0 / numQueries;
            System.out.print("\rEvaluating PI: " + (int) progress + "%");

            result = countBySides(query, labelSets, vertexLists);
            if (result > 0) {
                resultWriter.write(query.toString() + "," + result + "\n");
            }
        }
        resultWriter.close();

        topVList.clear();
        midVList.clear();
        bottomVList.clear();

        endTime = System.currentTimeMillis();
        System.out.println("\nEvaluating PI: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void evalFork33(List<Query> queries) throws Exception {
        load("fork33_left.csv", "fork33_middle.csv", "fork33_right.csv");

        long startTime = System.currentTimeMillis();
        long endTime;

        int numQueries = queries.size();
        double progress = 0;

        List<List<Set<Integer>>> labelSets = new ArrayList<>();
        int[][] vertexLists = new int[3][];

        vertexLists[0] = new int[]{0, 1};

        labelSets.add(new ArrayList<>());
        labelSets.get(0).add(KEYWORD);

        vertexLists[1] = new int[]{1, 2, 2, 5};

        labelSets.add(new ArrayList<>());
        labelSets.get(1).add(LINK_TYPE_SET);
        labelSets.get(1).add(LINK_TYPE_SET);

        vertexLists[2] = new int[]{3, 5, 4, 5, 5, 6};

        labelSets.add(new ArrayList<>());
        labelSets.get(2).add(COMP_TYPE_SET);
        labelSets.get(2).add(ROLE_TYPE_SET);
        labelSets.get(2).add(AKA);

        BufferedWriter resultWriter = new BufferedWriter(new FileWriter("fork336.csv"));

        long result;
        for (Query query : queries) {
            progress += 100.0 / numQueries;
            System.out.print("\rEvaluating FORK33: " + (int) progress + "%");

            result = countBySides(query, labelSets, vertexLists);
            if (result > 0) {
                resultWriter.write(query.toString() + "," + result + "\n");
            }
        }
        resultWriter.close();

        topVList.clear();
        midVList.clear();
        bottomVList.clear();

        endTime = System.currentTimeMillis();
        System.out.println("\nEvaluating FORK33: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public void evaluate() throws Exception {
        AcyclicQueryEvaluation evaluation = new AcyclicQueryEvaluation(6);

        System.out.println("------- EVALUATION -------");

        evalStar(evaluation.queries.subList(0, evaluation.queryTypeBoundaries.get(0)));

        evalPath(evaluation.queries.subList(
            evaluation.queryTypeBoundaries.get(0), evaluation.queryTypeBoundaries.get(1)
        ));

        evalTShape(evaluation.queries.subList(
            evaluation.queryTypeBoundaries.get(1), evaluation.queryTypeBoundaries.get(2)
        ));

        evalFork(evaluation.queries.subList(
            evaluation.queryTypeBoundaries.get(2), evaluation.queryTypeBoundaries.get(3)
        ));

        evalPi(evaluation.queries.subList(
            evaluation.queryTypeBoundaries.get(3), evaluation.queryTypeBoundaries.get(4)
        ));

        evalFork33(evaluation.queries.subList(
            evaluation.queryTypeBoundaries.get(4), evaluation.queryTypeBoundaries.get(5)
        ));
    }

    public TrueCardinality6(String graphFile) throws Exception {
        AKA.add(Labels.ALSO_KNOWN_AS);
        EPI.add(Labels.IS_EPISODE_OF);
        KEYWORD.add(Labels.IS_KEYWORD_OF);

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

            tripleString = csvReader.readLine();
        }

        endTime = System.currentTimeMillis();
        System.out.println("Graph Loading: " + ((endTime - startTime) / 1000.0) + " sec");

        csvReader.close();
    }

    public TrueCardinality6() {
        AKA.add(Labels.ALSO_KNOWN_AS);
        EPI.add(Labels.IS_EPISODE_OF);
        KEYWORD.add(Labels.IS_KEYWORD_OF);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println("load or save: " + args[1]);
        if (args[1].contains("load")) {
            System.out.println("propFile: " + args[2]);
        }
        System.out.println();

        if (args[1].contains("save")) {
            TrueCardinality6 trueCardinality6 = new TrueCardinality6(args[0]);
            trueCardinality6.traverseAndSave();
        } else if (args[1].contains("load")) {
            TrueCardinality6 trueCardinality6 = new TrueCardinality6();
            trueCardinality6.loadProperties(args[2]);
            trueCardinality6.evaluate();
        }
    }
}
