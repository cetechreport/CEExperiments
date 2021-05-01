package Graphflow;

import Common.Query;
import IMDB.AcyclicQueryEvaluation;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

public class JOBQueryDecomposer extends QueryDecomposer {
    // patternType -> decomVListString (defining decom topology) -> set of edge label seq
    private Map<Integer, Map<String, Set<String>>> queried = new HashMap<>();

    @Override
    public void decompose(Query query, int patternType) {
        int patternTypeIndex = patternType % JOBDecompositions.JOB_BASE - 1;
        String labelSeq, vListString;
        for (Integer[] patternVList : JOBDecompositions.LENGTH2[patternTypeIndex]) {
            vListString = toVListString(patternVList);
            queried.get(patternType).putIfAbsent(vListString, new HashSet<>());
            labelSeq = extractEdgeLabels(query.topology, patternVList);
            queried.get(patternType).get(vListString).add(labelSeq);
        }
        for (Integer[] patternVList : JOBDecompositions.LENGTH1[patternTypeIndex]) {
            vListString = toVListString(patternVList);
            queried.get(patternType).putIfAbsent(vListString, new HashSet<>());
            labelSeq = extractEdgeLabels(query.topology, patternVList);
            queried.get(patternType).get(vListString).add(labelSeq);
        }
    }

    @Override
    protected void saveDecompositions() throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter("decom.csv"));

        int total = 0;
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Map<String, Set<String>> vList2labelSeqs : queried.values()) {
            for (String vList : vList2labelSeqs.keySet()) {
                total += vList2labelSeqs.get(vList).size();
            }
        }

        for (Integer patternType : queried.keySet()) {
            for (String vList : queried.get(patternType).keySet()) {
                StringJoiner sj = new StringJoiner(",");
                sj.add(patternType.toString());
                sj.add(vList);

                for (String labelSeq : queried.get(patternType).get(vList)) {
                    sj.add(labelSeq);

                    progress += 100.0 / total;
                    System.out.print("\rSaving: " + (int) progress + "%");
                }
                writer.write(sj.toString() + "\n");
            }
        }
        writer.close();

        endTime = System.currentTimeMillis();
        System.out.println("\nSaving: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public JOBQueryDecomposer(int patternType) {
        queried.put(patternType, new HashMap<>());
    }

    public static void main(String[] args) throws Exception {
        System.out.println("queries: " + args[0]);
        System.out.println("patternType: " + args[1]);
        System.out.println();

        AcyclicQueryEvaluation queryEvaluation = new AcyclicQueryEvaluation(args[0].split(","));

        Integer patternType = Integer.parseInt(args[1]);
        QueryDecomposer decomposer = new JOBQueryDecomposer(patternType);

        int numQueries = queryEvaluation.queries.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        int startIndex = 0;
        for (Integer endIndex : queryEvaluation.queryTypeBoundaries) {
            for (int i = startIndex; i < endIndex; ++i) {
                progress += 100.0 / numQueries;
                System.out.print("\rDecomposing: " + (int) progress + "%");

                decomposer.decompose(queryEvaluation.queries.get(i), patternType);
            }
            startIndex = endIndex;
        }

        endTime = System.currentTimeMillis();
        System.out.println("\nDecomposing: " + ((endTime - startTime) / 1000.0) + " sec");

        decomposer.saveDecompositions();
    }
}
