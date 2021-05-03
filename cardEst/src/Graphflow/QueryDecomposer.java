package Graphflow;

import Common.Query;
import Common.Topology;
import IMDB.AcyclicQueryEvaluation;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

public class QueryDecomposer {
    // query topology -> decomVListString (defining decom topology) -> set of edge label seq
    private List<Map<String, Set<String>>> queried = new ArrayList<>();

    private static final int NUM_QUERY_EACH_TYPE = 200;
    private static final int NUM_QUERY_TYPES = 8;

    protected String toVListString(Integer[] vList) {
        StringJoiner sj = new StringJoiner(";");
        for (int i = 0; i < vList.length; i += 2) {
            sj.add(vList[i] + "-" + vList[i + 1]);
        }
        return sj.toString();
    }

    protected String extractEdgeLabels(Topology topology, Integer[] patternVList) {
        Set<Integer> intersection = new HashSet<>();
        StringJoiner sj = new StringJoiner("->");
        for (int i = 0; i < patternVList.length; i += 2) {
            intersection.clear();
            // intersection.addAll(topology.outgoing.get(patternVList[i]).keySet());
            // intersection.retainAll(topology.incoming.get(patternVList[i + 1]).keySet());

            // expected to have only 1 label
            // for (Integer label : intersection) {
            //     sj.add(label.toString());
            // }
            Integer label = topology.src2dest2label.get(patternVList[i]).get(patternVList[i + 1]);
            sj.add(label.toString());
        }
        return sj.toString();
    }

    public void decompose(Query query, int patternType) {
        String labelSeq, vListString;
        for (Integer[] patternVList : Constants.DECOMPOSITIONS4[patternType]) {
            vListString = toVListString(patternVList);
            queried.get(patternType).putIfAbsent(vListString, new HashSet<>());
            labelSeq = extractEdgeLabels(query.topology, patternVList);
            queried.get(patternType).get(vListString).add(labelSeq);
        }
        for (Integer[] patternVList : Constants.DECOMPOSITIONS3[patternType]) {
            vListString = toVListString(patternVList);
            queried.get(patternType).putIfAbsent(vListString, new HashSet<>());
            labelSeq = extractEdgeLabels(query.topology, patternVList);
            queried.get(patternType).get(vListString).add(labelSeq);
        }
        for (Integer[] patternVList : Constants.DECOMPOSITIONS2[patternType]) {
            vListString = toVListString(patternVList);
            queried.get(patternType).putIfAbsent(vListString, new HashSet<>());
            labelSeq = extractEdgeLabels(query.topology, patternVList);
            queried.get(patternType).get(vListString).add(labelSeq);
        }
        for (Integer[] patternVList : Constants.DECOMPOSITIONS1[patternType]) {
            vListString = toVListString(patternVList);
            queried.get(patternType).putIfAbsent(vListString, new HashSet<>());
            labelSeq = extractEdgeLabels(query.topology, patternVList);
            queried.get(patternType).get(vListString).add(labelSeq);
        }
    }

    protected void saveDecompositions() throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter("decom.csv"));

        int total = 0;
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (int type = 0; type < queried.size(); ++type) {
            for (String vList : queried.get(type).keySet()) {
                total += queried.get(type).get(vList).size();
            }
        }

        for (int type = 0; type < queried.size(); ++type) {
            for (String vList : queried.get(type).keySet()) {
                progress += 100.0 / total;
                System.out.print("\rSaving: " + (int) progress + "%");

                StringJoiner sj = new StringJoiner(",");
                sj.add(Integer.toString(type));
                sj.add(vList);

                for (String labelSeq : queried.get(type).get(vList)) {
                    sj.add(labelSeq);
                }
                writer.write(sj.toString() + "\n");
            }
        }
        writer.close();

        endTime = System.currentTimeMillis();
        System.out.println("\nSaving: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public QueryDecomposer() {
        for (int i = 0; i < NUM_QUERY_TYPES; ++i) {
            queried.add(new HashMap<>());
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("queries: " + args[0]);
        System.out.println("patternType: " + args[1]);
        System.out.println();

        AcyclicQueryEvaluation queryEvaluation = new AcyclicQueryEvaluation(args[0].split(","));
//        queryEvaluation.sampleQueries(NUM_QUERY_EACH_TYPE * NUM_QUERY_TYPES);

        QueryDecomposer decomposer = new QueryDecomposer();

        int numQueries = queryEvaluation.queries.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        int startIndex = 0;
        for (Integer endIndex : queryEvaluation.queryTypeBoundaries) {
            for (int i = startIndex; i < endIndex; ++i) {
                progress += 100.0 / numQueries;
                System.out.print("\rDecomposing: " + (int) progress + "%");

                decomposer.decompose(queryEvaluation.queries.get(i), Integer.parseInt(args[1]));
            }
            startIndex = endIndex;
        }

        endTime = System.currentTimeMillis();
        System.out.println("\nDecomposing: " + ((endTime - startTime) / 1000.0) + " sec");

        decomposer.saveDecompositions();
    }
}
