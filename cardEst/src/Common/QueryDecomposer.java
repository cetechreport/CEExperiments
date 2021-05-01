package Common;

import org.apache.commons.lang3.time.StopWatch;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

public class QueryDecomposer {
    private Integer patternType;
    // vList -> set of labelSeq
    public Map<String, Set<String>> decompositions = new HashMap<>();

    private void getDecom(String[] vListEdges, Set<String> result, int targetDepth, int depth, String current) {
        if (depth == targetDepth) {
            result.add(Util.sort(current));
            return;
        }

        for (int i = 0; i < vListEdges.length; i++) {
            String next = vListEdges[i];
            String updated = current.isEmpty() ? next : current + ";" + next;
            if (depth > 0 && !Util.isAcyclicConnected(updated)) continue;
            getDecom(vListEdges, result, targetDepth, depth + 1, updated);
        }
    }

    public void decompose(Query query, int decomLen) {
        String vListAndLabelSeq = query.toString();
        String vListString = vListAndLabelSeq.split(",")[0];
        String labelString = vListAndLabelSeq.split(",")[1];
        String[] vListEdges = vListString.split(";");
        for (int len = decomLen; len > 0; --len) {
            Set<String> decoms = new HashSet<>();
            getDecom(vListEdges, decoms, len, 0, "");
            for (String decom : decoms) {
                String decomLabelSeq = Util.extractLabelSeq(decom, vListString, labelString);
                decompositions.putIfAbsent(decom, new HashSet<>());
                decompositions.get(decom).add(decomLabelSeq);
            }
        }
    }

    public void saveDecompositions() throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter("decom.csv"));

        int total = 0;
        double progress = 0;

        StopWatch watch = new StopWatch();
        watch.start();

        for (Set<String> labelSeqs : decompositions.values()) {
            total += labelSeqs.size();
        }

        for (String vList : decompositions.keySet()) {
            StringJoiner sj = new StringJoiner(",");
            sj.add(patternType.toString());
            sj.add(vList);

            for (String labelSeq : decompositions.get(vList)) {
                sj.add(labelSeq);

                progress += 100.0 / total;
                System.out.print("\rSaving: " + (int) progress + "%");
            }
            writer.write(sj.toString() + "\n");
        }
        writer.close();

        watch.stop();
        System.out.println("\nSaving: " + (watch.getTime() / 1000.0) + " sec");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("queryFile: " + args[0]);
        System.out.println("decomLen: " + args[1]);
        System.out.println("patternType: " + args[2]);
        System.out.println();

        List<Query> queries = Query.readQueries(args[0]);

        QueryDecomposer decomposer = new QueryDecomposer();
        decomposer.patternType = Integer.parseInt(args[2]);

        int numQueries = queries.size();
        double progress = 0;

        StopWatch watch = new StopWatch();
        watch.start();

        for (int i = 0; i < queries.size(); ++i) {
            progress += 100.0 / numQueries;
            System.out.print("\rDecomposing: " + (int) progress + "%");

            decomposer.decompose(queries.get(i), Integer.parseInt(args[1]));
        }

        watch.stop();
        System.out.println("\nDecomposing: " + (watch.getTime() / 1000.0) + " sec");

        decomposer.saveDecompositions();
    }
}
