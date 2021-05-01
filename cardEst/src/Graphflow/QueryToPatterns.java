package Graphflow;

import Common.Pair;
import Common.Query;
import Common.Topology;
import Common.Util;
import IMDB.Labels;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;

public class QueryToPatterns {
    public String extractEdgeLabels(Topology topology, Integer[] patternVList) {
        Set<Integer> intersection = new HashSet<>();
        StringJoiner sj = new StringJoiner("->");
        for (int i = 0; i < patternVList.length; i += 2) {
            intersection.clear();
            intersection.addAll(topology.outgoing.get(patternVList[i]).keySet());
            intersection.retainAll(topology.incoming.get(patternVList[i + 1]).keySet());

            // expected to have only 1 label
            for (Integer label : intersection) {
                sj.add(label.toString());
            }
        }
        return sj.toString();
    }

    private List<Pair<String, Integer>> initFilters(int numVertices) {
        List<Pair<String, Integer>> filters = new ArrayList<>();
        for (int i = 0; i < numVertices; ++i) {
            filters.add(new Pair<>("", Labels.NO_FILTER));
        }
        return filters;
    }

    public List<Query> readQueries(String queryFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        Topology topology;
        List<Pair<String, Integer>> filters;
        String operator;
        int literal;

        List<Query> queries = new ArrayList<>();

        BufferedReader csvReader = new BufferedReader(new FileReader(queryFile));
        String[] queryString, edge;
        String line = csvReader.readLine();
        while (null != line) {
            queryString = line.split(",");

            int numEdges = Integer.parseInt(queryString[0]);

            topology = new Topology();
            for (int e = 1; e <= numEdges; e++) {
                edge = queryString[e].split("(-\\[)|(]>)");
                topology.addEdge(
                    Integer.parseInt(edge[0]),
                    Integer.parseInt(edge[1]),
                    Integer.parseInt(edge[2])
                );
            }

            int numFilters = Integer.parseInt(queryString[numEdges + 1]);

            filters = initFilters(numFilters);
            for (int f = 0; f < numFilters; ++f) {
                String filter = queryString[f + numEdges + 2];
                if (!filter.equals("-1")) {
                    if (filter.charAt(1) == '=') {
                        operator = filter.substring(0, 2);
                        literal = Integer.parseInt(filter.substring(2));
                    } else {
                        operator = filter.substring(0, 1);
                        literal = Integer.parseInt(filter.substring(1));
                    }

                    filters.get(f).key = operator;
                    filters.get(f).value = literal;
                }
            }

            queries.add(new Query(topology, filters));

            line = csvReader.readLine();
        }

        endTime = System.currentTimeMillis();
        System.out.println("Query Loading: " + ((endTime - startTime) / 1000.0) + " sec");

        return queries;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("queryFile: " + args[0]);
        System.out.println("patternType: " + args[1]);
        System.out.println("patternVList: " + args[2]);
        System.out.println("destFile: " + args[3]);
        System.out.println();

        QueryToPatterns converter = new QueryToPatterns();
        List<Query> queries = converter.readQueries(args[0]);

        StringJoiner sj = new StringJoiner(",");
        sj.add(args[1]);
        sj.add(args[2]);
        for (Query query : queries) {
            String labelSeq = converter.extractEdgeLabels(query.topology, Util.toVList(args[2]));
            sj.add(labelSeq);
        }

        BufferedWriter writer = new BufferedWriter(new FileWriter(args[3]));
        writer.write(sj.toString());
        writer.close();
    }
}
