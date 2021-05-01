package Graphflow;

import Common.Query;
import org.apache.commons.lang3.time.StopWatch;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.List;
import java.util.StringJoiner;

public class CyclicQueryEvaluation {
    public static void main(String[] args) throws Exception {
        String method = args[0];
        System.out.println("estimationMethod: " + method);

        BufferedWriter resultWriter = new BufferedWriter(new FileWriter("estimation.csv"));
        List<Query> queries = Query.readQueries(args[args.length - 1]);

        int numQueries = queries.size();
        double progress = 0;

        StopWatch watch = new StopWatch();
        watch.start();

        if (method.contains("triangle")) {
            System.out.println("catalogueFile: " + args[1]);
            System.out.println("hopMode: " + args[2]);
            System.out.println("queries: " + args[3]);
            System.out.println();

            Double[] estimations;
            TriangleCatalogue catalogue = new TriangleCatalogue(args[1]);
            for (int i = 0; i < queries.size(); ++i) {
                Query query = queries.get(i);
                estimations = catalogue.estimateWithTriangle(query);

                StringJoiner sj = new StringJoiner(",");
                sj.add(query.toString());
                for (int k = 0; k < estimations.length; ++k) {
                    if (!Boolean.parseBoolean(args[2]) && k > 2) break;
                    sj.add(estimations[k].toString());
                }
                resultWriter.write(sj.toString() + "\n");

                progress += 100.0 / numQueries;
                System.out.print("\rEstimating: " + (int) progress + "%");
            }
        }
        resultWriter.close();

        watch.stop();
        System.out.println("\nEstimating: " + (watch.getTime() / 1000.0) + " sec");
    }
}
