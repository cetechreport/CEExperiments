package MarkovTable.PropertyFilter;

import Common.Pair;
import Common.Path;
import Common.Query;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class TrueCardinality {
    // centralVertex -> (half_path -> list of vertex paths)
    private Map<Integer, Map<Path, List<List<Integer>>>> backwardPart = new HashMap<>();
    private Map<Integer, Map<Path, List<List<Integer>>>> forwardPart = new HashMap<>();

    private Map<Integer, Integer> vid2prodYear;

    private List<Query> queries = new ArrayList<>();

    private final int NO_FILTER = -1;
    private final int PROD_YEAR_INDEX = 4;

    private void readBackwardForwardParts(String backwardFile, String forwardFile) throws Exception {
        String[] info, labelList, vertexList;
        int centralVertex, numVertexLists;
        Path path;
        List<Integer> vertexPath;

        BufferedReader backwardReader = new BufferedReader(new FileReader(backwardFile));
        String line = backwardReader.readLine();
        while (null != line) {
            info = line.split(",");
            centralVertex = Integer.parseInt(info[0]);
            backwardPart.putIfAbsent(centralVertex, new HashMap<>());

            int k = 1;
            while (k < info.length) {
                labelList = info[k].split("->");
                path = new Path(new ArrayList<>());
                for (int i = 0; i < labelList.length; ++i) {
                    path.append(Integer.parseInt(labelList[i]));
                }
                backwardPart.get(centralVertex).putIfAbsent(path, new ArrayList<>());

                numVertexLists = Integer.parseInt(info[k + 1]);
                k++;
                for (int i = 1; i <= numVertexLists; ++i) {
                    vertexList = info[k + i].split("->");
                    vertexPath = new ArrayList<>();
                    for (int j = 0; j < vertexList.length; ++j) {
                        vertexPath.add(Integer.parseInt(vertexList[j]));
                    }
                    backwardPart.get(centralVertex).get(path).add(vertexPath);
                }

                k += numVertexLists + 1;
            }

            line = backwardReader.readLine();
        }
        backwardReader.close();

        BufferedReader forwardReader = new BufferedReader(new FileReader(forwardFile));
        line = forwardReader.readLine();
        while (null != line) {
            info = line.split(",");
            centralVertex = Integer.parseInt(info[0]);
            forwardPart.putIfAbsent(centralVertex, new HashMap<>());

            int k = 1;
            while (k < info.length) {
                labelList = info[k].split("->");
                path = new Path(new ArrayList<>());
                for (int i = 0; i < labelList.length; ++i) {
                    path.append(Integer.parseInt(labelList[i]));
                }
                forwardPart.get(centralVertex).putIfAbsent(path, new ArrayList<>());

                numVertexLists = Integer.parseInt(info[k + 1]);
                k++;
                for (int i = 1; i <= numVertexLists; ++i) {
                    vertexList = info[k + i].split("->");
                    vertexPath = new ArrayList<>();
                    for (int j = 0; j < vertexList.length; ++j) {
                        vertexPath.add(Integer.parseInt(vertexList[j]));
                    }
                    forwardPart.get(centralVertex).get(path).add(vertexPath);
                }

                k += numVertexLists + 1;
            }

            line = forwardReader.readLine();
        }
        forwardReader.close();
    }

    private void readQueries(String evalFile) throws Exception {
        String[] info, labelList;
        Path path;
        List<Pair<String, Integer>> filters;

        BufferedReader queryReader = new BufferedReader(new FileReader(evalFile));
        String line = queryReader.readLine();
        while (null != line) {
            info = line.split(",");
            labelList = info[0].split("->");
            path = new Path(new ArrayList<>());
            for (int i = 0; i < labelList.length; ++i) {
                path.append(Integer.parseInt(labelList[i]));
            }

            filters = new ArrayList<>();
            for (int i = 1; i < info.length - 1; ++i) {
                if (Integer.toString(NO_FILTER).equals(info[i])) {
                    filters.add(new Pair<>("", NO_FILTER));
                } else {
                    filters.add(new Pair<>(
                        info[i].substring(0, 1), Integer.parseInt(info[i].substring(1))
                    ));
                }
            }

            queries.add(new Query(path, filters));

            line = queryReader.readLine();
        }
        queryReader.close();
    }

    private void readProperties(String propFile) throws Exception {
        vid2prodYear = new HashMap<>();

        String[] properties;

        BufferedReader tsvReader = new BufferedReader(new FileReader(propFile));
        tsvReader.readLine(); // Header

        String line = tsvReader.readLine();
        while (null != line) {
            properties = line.split("\t");
            if (!properties[PROD_YEAR_INDEX].isEmpty()) {
                vid2prodYear.put(
                    Integer.parseInt(properties[0]), Integer.parseInt(properties[PROD_YEAR_INDEX])
                );
            }
            line = tsvReader.readLine();
        }
        tsvReader.close();
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

    private Query[] splitInHalf(Query query) {
        Query[] twoHalves = new Query[2];

        int rightHalfStartIndex = query.path.length() / 2 + query.path.length() % 2;
        Path halfPath = query.path.subPath(0, rightHalfStartIndex);
        List<Pair<String, Integer>> halfFilters = query.filters.subList(0, rightHalfStartIndex + 1);
        twoHalves[0] = new Query(halfPath, halfFilters);

        halfPath = query.path.subPath(rightHalfStartIndex, query.path.length());
        halfFilters = query.filters.subList(rightHalfStartIndex, query.filters.size());
        twoHalves[1] = new Query(halfPath, halfFilters);

        return twoHalves;
    }

    private void evaluate() throws Exception {
        Query[] halfQueries;
        List<List<Integer>> vertexPaths;

        StringJoiner result = new StringJoiner("\n");

        long startTime = System.currentTimeMillis();
        long endTime;

        double progress = 0;
        int numQueries = queries.size();

        for (Query query : queries) {
            halfQueries = splitInHalf(query);
            int total = 0;

            for (int centralVertex : backwardPart.keySet()) {
                if (!forwardPart.isEmpty() && !forwardPart.containsKey(centralVertex)) continue;
                if (!backwardPart.get(centralVertex).containsKey(halfQueries[0].path)) continue;
                if (!forwardPart.get(centralVertex).containsKey(halfQueries[1].path)) continue;

                int numBackwardQualified = 0, numForwardQualified = 0;

                vertexPaths = backwardPart.get(centralVertex).get(halfQueries[0].path);

                for (List<Integer> vertexPath : vertexPaths) {
                    if (applyFilter(vertexPath, halfQueries[0].filters)) {
                        numBackwardQualified++;
                    }
                }

                vertexPaths = forwardPart.get(centralVertex).get(halfQueries[1].path);

                for (List<Integer> vertexPath : vertexPaths) {
                    if (applyFilter(vertexPath, halfQueries[1].filters)) {
                        numForwardQualified++;
                    }
                }

                total += numBackwardQualified * numForwardQualified;
            }

            result.add(query.toString() + "," + total);

            progress += 100.0 / numQueries;
            System.out.print("\rProcessing Queries: " + (int) progress + "%");
        }

        endTime = System.currentTimeMillis();
        System.out.println("\nProcessing Queries: " + ((endTime - startTime) / 1000.0) + " sec");

        System.out.println("------- TRUE CARDINALITY -------");
        System.out.println(result.toString());
    }

    public TrueCardinality(String backwardFile, String forwardFile, String evalFile, String propFile)
            throws Exception {
        long startTime = System.currentTimeMillis();
        readBackwardForwardParts(backwardFile, forwardFile);
        long endTime = System.currentTimeMillis();
        System.out.println("Loading Backward & Forward: " + ((endTime - startTime) / 1000.0) + " sec");

        startTime = System.currentTimeMillis();
        readQueries(evalFile);
        endTime = System.currentTimeMillis();
        System.out.println("Loading Queries: " + ((endTime - startTime) / 1000.0) + " sec");

        startTime = System.currentTimeMillis();
        readProperties(propFile);
        endTime = System.currentTimeMillis();
        System.out.println("Loading Properties: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("backwardFile: " + args[0]);
        System.out.println("forwardFile: " + args[1]);
        System.out.println("evalFile: " + args[2]);
        System.out.println("propFile: " + args[3]);
        System.out.println();

        TrueCardinality trueCardinality = new TrueCardinality(args[0], args[1], args[2], args[3]);
        trueCardinality.evaluate();
    }
}
