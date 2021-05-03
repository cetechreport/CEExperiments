package Graphflow;

import Common.Query;
import org.apache.commons.lang3.time.StopWatch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

public class CyclicQueryEvaluation {
    public static void main(String[] args) throws Exception {
        String method = args[0];
        System.out.println("estimationMethod: " + method);

        BufferedWriter resultWriter = new BufferedWriter(new FileWriter("estimation.csv"));
        List<Query> queries = Query.readQueries(args[args.length - 3]);

        int numQueries = queries.size();
        double progress = 0;

        StopWatch watch = new StopWatch();
        watch.start();

        if (method.contains("acyclic")
                || method.contains("baseline")
                || method.contains("triangleClosing")
                || method.contains("avgSampledExtensionRate")
                || method.contains("onlyExtensionRate")
                || method.contains("midEdgeClosing")) {
            System.out.println("catalogueFile: " + args[1]);
            System.out.println("hopMode: " + args[2]);
            System.out.println("queries: " + args[3]);
            System.out.println("randomSamplingMode: " + args[4]);
            System.out.println("allEst: " + args[5]);
            System.out.println();

            boolean randomSampling = Boolean.parseBoolean(args[4]);
            boolean allEst = Boolean.parseBoolean(args[5]);
            BufferedReader reader = new BufferedReader(new FileReader(args[3]));

            Double[] estimations;
            TriangleCatalogue catalogue = new TriangleCatalogue(args[1]);
            for (int i = 0; i < queries.size(); ++i) {
                String line = reader.readLine();
                String[] queries_info = line.split(",");
                double trueCard;
                if (queries_info.length != 3) trueCard = 1;
                else trueCard = Double.parseDouble(queries_info[2]);
                Query query = queries.get(i);

                estimations = catalogue.cyclicEstimation(query, method, randomSampling, allEst, trueCard);
                if (estimations[0] < 0) continue;
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
