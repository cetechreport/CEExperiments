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

public class TrueCardinality5 {
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

    private Map<Integer, Map<Path, List<List<Integer>>>> side1vList = new HashMap<>();
    private Map<Integer, Map<Path, List<List<Integer>>>> side2vList = new HashMap<>();

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

    private void starLeft() {
        Set<Integer> intersection = new HashSet<>();

        String vertexList, path;

        int numV = dest2label2src.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer title : dest2label2src.keySet()) {
            progress += 100.0 / numV;
            System.out.print("\rProcessing STAR_LEFT: " + (int) progress + "%");

            if (!src2label2dest.containsKey(title)) continue;
            if (!src2label2dest.get(title).containsKey(Labels.ALSO_KNOWN_AS)) continue;

            intersection.clear();
            for (Integer compType : COMP_TYPE_SET) {
                if (dest2label2src.get(title).containsKey(compType)) {
                    intersection.add(compType);
                }
            }
            if (intersection.isEmpty()) continue;

            for (Integer compType : intersection) {
                for (Integer comp : dest2label2src.get(title).get(compType)) {
                    for (Integer aka : src2label2dest.get(title).get(Labels.ALSO_KNOWN_AS)) {
                        path = compType + "->" + Labels.ALSO_KNOWN_AS;
                        vertexList = comp + "-" + title + ";" + title + "-" + aka;

                        v2path2vList.putIfAbsent(title, new HashMap<>());
                        v2path2vList.get(title).putIfAbsent(path, new ArrayList<>());
                        v2path2vList.get(title).get(path).add(vertexList);
                    }
                }
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nProcessing STAR_LEFT: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void starRight() {
        Set<Integer> intersection1 = new HashSet<>();
        Set<Integer> intersection2 = new HashSet<>();

        String vertexList, path;

        int numV = src2label2dest.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer title1 : src2label2dest.keySet()) {
            progress += 100.0 / numV;
            System.out.print("\rProcessing STAR_RIGHT: " + (int) progress + "%");

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
                for (Integer infoType1 : intersection2) {
                    for (Integer infoType2 : intersection2) {
                        for (Integer title2 : src2label2dest.get(title1).get(linkType)) {
                            for (Integer info1 : src2label2dest.get(title1).get(infoType1)) {
                                for (Integer info2 : src2label2dest.get(title1).get(infoType2)) {
                                    if (infoType1.equals(infoType2) && info1.equals(info2)) continue;

                                    path = linkType + "->" + infoType1 + "->" + infoType2;
                                    vertexList = title1 + "-" + title2 + ";" + title1 + "-" +
                                        info1 + ";" + title1 + "-" + info2;

                                    v2path2vList.putIfAbsent(title1, new HashMap<>());
                                    v2path2vList.get(title1).putIfAbsent(path, new ArrayList<>());
                                    v2path2vList.get(title1).get(path).add(vertexList);
                                }
                            }
                        }
                    }
                }
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nProcessing STAR_RIGHT: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void pathLeft() {
        Set<Integer> intersection1 = new HashSet<>();
        Set<Integer> intersection2 = new HashSet<>();

        String vertexList, path;

        int numV = dest2label2src.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer title1 : dest2label2src.keySet()) {
            progress += 100.0 / numV;
            System.out.print("\rProcessing PATH_LEFT: " + (int) progress + "%");

            intersection1.clear();
            for (Integer roleType : ROLE_TYPE_SET) {
                if (dest2label2src.get(title1).containsKey(roleType)) {
                    intersection1.add(roleType);
                }
            }
            if (intersection1.isEmpty()) continue;

            intersection2.clear();
            for (Integer linkType : LINK_TYPE_SET) {
                if (dest2label2src.get(title1).containsKey(linkType)) {
                    intersection2.add(linkType);
                }
            }
            if (intersection2.isEmpty()) continue;

            for (Integer roleType : intersection1) {
                for (Integer linkType1 : intersection2) {
                    for (Integer title2 : dest2label2src.get(title1).get(linkType1)) {
                        if (!src2label2dest.containsKey(title2)) continue;
                        for (Integer linkType2 : intersection2) {
                            if (!src2label2dest.get(title2).containsKey(linkType2)) continue;

                            for (Integer title3 : src2label2dest.get(title2).get(linkType2)) {
                                for (Integer name : dest2label2src.get(title1).get(roleType)) {
                                    path = roleType + "->" + linkType1 + "->" + linkType2;
                                    vertexList = name + "-" + title1 + ";" + title2 + "-" + title1 +
                                        ";" + title2 + "-" + title3;

                                    v2path2vList.putIfAbsent(title3, new HashMap<>());
                                    v2path2vList.get(title3).putIfAbsent(path, new ArrayList<>());
                                    v2path2vList.get(title3).get(path).add(vertexList);
                                }
                            }
                        }
                    }
                }
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nProcessing PATH_LEFT: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void pathRight() {
        Set<Integer> intersection1 = new HashSet<>();
        Set<Integer> intersection2 = new HashSet<>();

        String vertexList, path;

        int numV = src2label2dest.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer title1 : src2label2dest.keySet()) {
            progress += 100.0 / numV;
            System.out.print("\rProcessing PATH_RIGHT: " + (int) progress + "%");

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

                            v2path2vListPivot.putIfAbsent(title1, new HashMap<>());
                            v2path2vListPivot.get(title1).putIfAbsent(path, new ArrayList<>());
                            v2path2vListPivot.get(title1).get(path).add(vertexList);
                        }
                    }
                }
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nProcessing PATH_RIGHT: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void tShapeLeft() {
        Set<Integer> intersection1 = new HashSet<>();
        Set<Integer> intersection2 = new HashSet<>();

        String vertexList, path;

        int numV = src2label2dest.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer name : src2label2dest.keySet()) {
            progress += 100.0 / numV;
            System.out.print("\rProcessing T_LEFT: " + (int) progress + "%");

            if (!src2label2dest.get(name).containsKey(Labels.ALSO_KNOWN_AS)) continue;

            intersection1.clear();
            for (Integer infoType : INFO_TYPE_SET) {
                if (src2label2dest.get(name).containsKey(infoType)) {
                    intersection1.add(infoType);
                }
            }
            if (intersection1.isEmpty()) continue;

            intersection2.clear();
            for (Integer roleType : ROLE_TYPE_SET) {
                if (src2label2dest.get(name).containsKey(roleType)) {
                    intersection2.add(roleType);
                }
            }
            if (intersection2.isEmpty()) continue;

            for (Integer roleType : intersection2) {
                for (Integer title : src2label2dest.get(name).get(roleType)) {
                    if (!src2label2dest.containsKey(title)) continue;
                    for (Integer infoType : intersection1) {
                        if (!src2label2dest.get(title).containsKey(infoType)) continue;

                        for (Integer info : src2label2dest.get(title).get(infoType)) {
                            for (Integer aka : src2label2dest.get(name).get(Labels.ALSO_KNOWN_AS)) {
                                path = Labels.ALSO_KNOWN_AS + "->" + roleType + "->" + infoType;
                                vertexList = name + "-" + aka + ";" + name + "-" + title + ";" +
                                    title + "-" + info;

                                v2path2vList.putIfAbsent(title, new HashMap<>());
                                v2path2vList.get(title).putIfAbsent(path, new ArrayList<>());
                                v2path2vList.get(title).get(path).add(vertexList);
                            }
                        }
                    }
                }
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nProcessing T_LEFT: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void piLeft() {
        Set<Integer> intersection1 = new HashSet<>();
        Set<Integer> intersection2 = new HashSet<>();

        String vertexList, path;

        int numV = src2label2dest.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer title1 : src2label2dest.keySet()) {
            progress += 100.0 / numV;
            System.out.print("\rProcessing PI_LEFT: " + (int) progress + "%");

            if (!src2label2dest.get(title1).containsKey(Labels.ALSO_KNOWN_AS)) continue;

            intersection1.clear();
            for (Integer infoType : INFO_TYPE_SET) {
                if (src2label2dest.get(title1).containsKey(infoType)) {
                    intersection1.add(infoType);
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

            for (Integer infoType : intersection1) {
                for (Integer linkType : intersection2) {
                    for (Integer info : src2label2dest.get(title1).get(infoType)) {
                        for (Integer title2 : src2label2dest.get(title1).get(linkType)) {
                            for (Integer aka : src2label2dest.get(title1).get(Labels.ALSO_KNOWN_AS)) {
                                path = Labels.ALSO_KNOWN_AS + "->" + infoType + "->" + linkType;
                                vertexList = title1 + "-" + aka + ";" + title1 + "-" + info + ";" +
                                    title1 + "-" + title2;

                                v2path2vList.putIfAbsent(title2, new HashMap<>());
                                v2path2vList.get(title2).putIfAbsent(path, new ArrayList<>());
                                v2path2vList.get(title2).get(path).add(vertexList);
                            }
                        }
                    }
                }
            }
        }
        endTime = System.currentTimeMillis();
        System.out.println("\nProcessing PI_LEFT: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void piRight() {
        Set<Integer> intersection1 = new HashSet<>();

        String vertexList, path;

        int numV = dest2label2src.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Integer title : dest2label2src.keySet()) {
            progress += 100.0 / numV;
            System.out.print("\rProcessing PI_RIGHT: " + (int) progress + "%");

            if (!dest2label2src.get(title).containsKey(Labels.IS_KEYWORD_OF)) continue;

            intersection1.clear();
            for (Integer roleType : ROLE_TYPE_SET) {
                if (dest2label2src.get(title).containsKey(roleType)) {
                    intersection1.add(roleType);
                }
            }
            if (intersection1.isEmpty()) continue;

            for (Integer roleType : intersection1) {
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

    public void traverseAndSave() throws Exception {
        starLeft();
        flush("star_left.csv", false);

        starRight();
        flush("star_right.csv", false);

        pathLeft();
        flush("path_left.csv", false);

        pathRight();
        flush("path_right.csv", false);
        flush("path_right_pivot.csv", true);

        tShapeLeft();
        flush("t_left.csv", false);

        piLeft();
        flush("pi_left.csv", false);

        piRight();
        flush("pi_right.csv", false);
    }

    private void load(String fileName1, String fileName2) throws Exception {
        String[] info, labelList, vertexList;
        int centralVertex, numVertexLists;
        Path path;
        List<Integer> vertexPath;
        String fileName = fileName1;
        Map<Integer, Map<Path, List<List<Integer>>>> vList = side1vList;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (int h = 0; h < 2; ++h) {
            if (h == 1) {
                fileName = fileName2;
                vList = side2vList;
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
        System.out.println("Loading " + fileName1 + " & " + fileName2 + ": " +
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

    private int count(Query query, List<Set<Integer>> labelSets1,
            List<Set<Integer>> labelSets2, int[] vertexList1, int[] vertexList2) throws Exception {

        Path path1, path2;
        List<Pair<String, Integer>> filters1, filters2;

        path1 = extractPath(query.topology, vertexList1, labelSets1);
        filters1 = extractFilters(query.filters, vertexList1);

        path2 = extractPath(query.topology, vertexList2, labelSets2);
        filters2 = extractFilters(query.filters, vertexList2);

        int sum = 0;
        for (Integer title : side1vList.keySet()) {
            if (!side1vList.get(title).containsKey(path1)) continue;
            if (!side2vList.containsKey(title)) continue;
            if (!side2vList.get(title).containsKey(path2)) continue;

            int side1NumQualified = 0;
            int side2NumQualified = 0;

            for (List<Integer> pathInstance : side1vList.get(title).get(path1)) {
                if (applyFilter(pathInstance, filters1)) side1NumQualified++;
            }

            for (List<Integer> pathInstance : side2vList.get(title).get(path2)) {
                if (applyFilter(pathInstance, filters2)) side2NumQualified++;
            }

            sum += side1NumQualified * side2NumQualified;
        }

        return sum;
    }

    private void evalStar4(List<Query> queries) throws Exception {
        load("star_top.csv", "star_bottom.csv");

        long startTime = System.currentTimeMillis();
        long endTime;

        int numQueries = queries.size();
        double progress = 0;

        int[] vertexList1 = new int[]{0, 1, 0, 2};

        List<Set<Integer>> labelSets1 = new ArrayList<>();
        labelSets1.add(AKA);
        labelSets1.add(LINK_TYPE_SET);

        int[] vertexList2 = new int[]{0, 3, 0, 4};

        List<Set<Integer>> labelSets2 = new ArrayList<>();
        labelSets2.add(LINK_TYPE_SET);
        labelSets2.add(INFO_TYPE_SET);

        BufferedWriter resultWriter = new BufferedWriter(new FileWriter("star4.csv"));
        int result;
        for (Query query : queries) {
            progress += 100.0 / numQueries;
            System.out.print("\rEvaluating STAR: " + (int) progress + "%");

            result = count(query, labelSets1, labelSets2, vertexList1, vertexList2);
            if (result > 0) {
                resultWriter.write(query.toString() + "," + result + "\n");
            }
        }
        resultWriter.close();

        side1vList.clear();
        side2vList.clear();

        endTime = System.currentTimeMillis();
        System.out.println("\nEvaluating STAR: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void evalStar(List<Query> queries) throws Exception {
        load("star_left.csv", "star_right.csv");

        long startTime = System.currentTimeMillis();
        long endTime;

        int numQueries = queries.size();
        double progress = 0;

        int[] vertexList1 = new int[]{0, 1, 1, 2};

        List<Set<Integer>> labelSets1 = new ArrayList<>();
        labelSets1.add(COMP_TYPE_SET);
        labelSets1.add(AKA);

        int[] vertexList2 = new int[]{1, 3, 1, 4, 1, 5};

        List<Set<Integer>> labelSets2 = new ArrayList<>();
        labelSets2.add(LINK_TYPE_SET);
        labelSets2.add(INFO_TYPE_SET);
        labelSets2.add(INFO_TYPE_SET);

        BufferedWriter resultWriter = new BufferedWriter(new FileWriter("star.csv"));
        int result;
        for (Query query : queries) {
            progress += 100.0 / numQueries;
            System.out.print("\rEvaluating STAR: " + (int) progress + "%");

            result = count(query, labelSets1, labelSets2, vertexList1, vertexList2);
            if (result > 0) {
                resultWriter.write(query.toString() + "," + result + "\n");
            }
        }
        resultWriter.close();

        side1vList.clear();
        side2vList.clear();

        endTime = System.currentTimeMillis();
        System.out.println("\nEvaluating STAR: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void evalPath(List<Query> queries) throws Exception {
        load("path_left.csv", "path_right.csv");

        long startTime = System.currentTimeMillis();
        long endTime;

        int numQueries = queries.size();
        double progress = 0;

        int[] vertexList1 = new int[]{0, 2, 1, 2, 1, 5};

        List<Set<Integer>> labelSets1 = new ArrayList<>();
        labelSets1.add(ROLE_TYPE_SET);
        labelSets1.add(LINK_TYPE_SET);
        labelSets1.add(LINK_TYPE_SET);

        int[] vertexList2 = new int[]{3, 4, 4, 5};

        List<Set<Integer>> labelSets2 = new ArrayList<>();
        labelSets2.add(COMP_TYPE_SET);
        labelSets2.add(LINK_TYPE_SET);

        BufferedWriter resultWriter = new BufferedWriter(new FileWriter("path.csv"));
        int result;
        for (Query query : queries) {
            progress += 100.0 / numQueries;
            System.out.print("\rEvaluating PATH: " + (int) progress + "%");

            result = count(query, labelSets1, labelSets2, vertexList1, vertexList2);
            if (result > 0) {
                resultWriter.write(query.toString() + "," + result + "\n");
            }
        }
        resultWriter.close();

        side1vList.clear();
        side2vList.clear();

        endTime = System.currentTimeMillis();
        System.out.println("\nEvaluating PATH: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void evalTShape(List<Query> queries) throws Exception {
        load("t_left.csv", "path_right.csv");

        long startTime = System.currentTimeMillis();
        long endTime;

        int numQueries = queries.size();
        double progress = 0;

        int[] vertexList1 = new int[]{0, 1, 0, 4, 4, 5};

        List<Set<Integer>> labelSets1 = new ArrayList<>();
        labelSets1.add(AKA);
        labelSets1.add(ROLE_TYPE_SET);
        labelSets1.add(INFO_TYPE_SET);

        int[] vertexList2 = new int[]{2, 3, 3, 4};

        List<Set<Integer>> labelSets2 = new ArrayList<>();
        labelSets2.add(COMP_TYPE_SET);
        labelSets2.add(LINK_TYPE_SET);

        BufferedWriter resultWriter = new BufferedWriter(new FileWriter("t_shape.csv"));
        int result;
        for (Query query : queries) {
            progress += 100.0 / numQueries;
            System.out.print("\rEvaluating T_SHAPE: " + (int) progress + "%");

            result = count(query, labelSets1, labelSets2, vertexList1, vertexList2);
            if (result > 0) {
                resultWriter.write(query.toString() + "," + result + "\n");
            }
        }
        resultWriter.close();

        side1vList.clear();
        side2vList.clear();

        endTime = System.currentTimeMillis();
        System.out.println("\nEvaluating T_SHAPE: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void evalFork(List<Query> queries) throws Exception {
        load("t_left.csv", "path_right_pivot.csv");

        long startTime = System.currentTimeMillis();
        long endTime;

        int numQueries = queries.size();
        double progress = 0;

        int[] vertexList1 = new int[]{0, 1, 0, 3, 3, 5};

        List<Set<Integer>> labelSets1 = new ArrayList<>();
        labelSets1.add(AKA);
        labelSets1.add(ROLE_TYPE_SET);
        labelSets1.add(INFO_TYPE_SET);

        int[] vertexList2 = new int[]{2, 3, 3, 4};

        List<Set<Integer>> labelSets2 = new ArrayList<>();
        labelSets2.add(COMP_TYPE_SET);
        labelSets2.add(LINK_TYPE_SET);

        BufferedWriter resultWriter = new BufferedWriter(new FileWriter("fork.csv"));
        int result;
        for (Query query : queries) {
            progress += 100.0 / numQueries;
            System.out.print("\rEvaluating FORK: " + (int) progress + "%");

            result = count(query, labelSets1, labelSets2, vertexList1, vertexList2);
            if (result > 0) {
                resultWriter.write(query.toString() + "," + result + "\n");
            }
        }
        resultWriter.close();

        side1vList.clear();
        side2vList.clear();

        endTime = System.currentTimeMillis();
        System.out.println("\nEvaluating FORK: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void evalPi(List<Query> queries) throws Exception {
        load("pi_left.csv", "pi_right.csv");

        long startTime = System.currentTimeMillis();
        long endTime;

        int numQueries = queries.size();
        double progress = 0;

        int[] vertexList1 = new int[]{0, 1, 0, 2, 0, 5};

        List<Set<Integer>> labelSets1 = new ArrayList<>();
        labelSets1.add(AKA);
        labelSets1.add(INFO_TYPE_SET);
        labelSets1.add(LINK_TYPE_SET);

        int[] vertexList2 = new int[]{3, 5, 4, 5};

        List<Set<Integer>> labelSets2 = new ArrayList<>();
        labelSets2.add(ROLE_TYPE_SET);
        labelSets2.add(KEYWORD);

        BufferedWriter resultWriter = new BufferedWriter(new FileWriter("pi.csv"));

        int result;
        for (Query query : queries) {
            progress += 100.0 / numQueries;
            System.out.print("\rEvaluating PI: " + (int) progress + "%");

            result = count(query, labelSets1, labelSets2, vertexList1, vertexList2);
            if (result > 0) {
                resultWriter.write(query.toString() + "," + result + "\n");
            }
        }
        resultWriter.close();

        side1vList.clear();
        side2vList.clear();

        endTime = System.currentTimeMillis();
        System.out.println("\nEvaluating PI: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public void evaluate() throws Exception {
        AcyclicQueryEvaluation evaluation = new AcyclicQueryEvaluation(4);

        System.out.println("------- EVALUATION -------");

        evalStar4(evaluation.queries.subList(0, evaluation.queryTypeBoundaries.get(0)));
//        evalStar(evaluation.queries.subList(0, evaluation.queryTypeBoundaries.get(0)));
//
//        evalPath(evaluation.queries.subList(
//            evaluation.queryTypeBoundaries.get(0), evaluation.queryTypeBoundaries.get(1)
//        ));
//
//        evalTShape(evaluation.queries.subList(
//            evaluation.queryTypeBoundaries.get(1), evaluation.queryTypeBoundaries.get(2)
//        ));
//
//        evalFork(evaluation.queries.subList(
//            evaluation.queryTypeBoundaries.get(2), evaluation.queryTypeBoundaries.get(3)
//        ));
//
//        evalPi(evaluation.queries.subList(
//            evaluation.queryTypeBoundaries.get(3), evaluation.queryTypeBoundaries.get(4)
//        ));
    }

    public TrueCardinality5(String graphFile) throws Exception {
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

    public TrueCardinality5() {
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
            TrueCardinality5 trueCardinality5 = new TrueCardinality5(args[0]);
            trueCardinality5.traverseAndSave();
        } else if (args[1].contains("load")) {
            TrueCardinality5 trueCardinality5 = new TrueCardinality5();
            trueCardinality5.loadProperties(args[2]);
            trueCardinality5.evaluate();
        }
    }
}
