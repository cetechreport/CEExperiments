package Graphflow;

import Common.Query;
import Common.Util;
import org.apache.commons.lang3.time.StopWatch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.List;
import java.util.StringJoiner;

public class QErrorComparison {
    public static void main(String[] args) throws Exception {

        String method = args[0];
        System.out.println("estimationMethod: " + method);

        List<Query> queries = Query.readQueries(args[args.length - 1]);
        int numQueries = queries.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedWriter resultWriter = new BufferedWriter(new FileWriter("QErrors.csv"));

        System.out.println("catalogueFile: " + args[1]);
        System.out.println("debug: " + args[2]);
        System.out.println("maxLen: " + args[3]);
        System.out.println("formulaType: " + args[4]);
        System.out.println("random: " + args[5]);
        System.out.println("queries: " + args[6]);
        System.out.println();

        boolean debug = Boolean.parseBoolean(args[2]);
        boolean random = Boolean.parseBoolean(args[5]);
        int formulaType = Util.getFormulaType(args[4]);
        if (formulaType == -1) return;

        int catLen = Integer.parseInt(args[3]);

        //get the patternType
        BufferedReader reader = new BufferedReader(new FileReader(args[1]));
        String line = reader.readLine();
        String[] info = line.split(",");
        reader.close();

        //get vlist
        reader = new BufferedReader(new FileReader(args[6]));

        Double[] qErrors;
        Catalogue catalogue = new Catalogue(args[1], catLen);

        System.out.println("------- EVALUATION RESULT -------");
        for (int i = 0; i < queries.size(); ++i) {
            Query query = queries.get(i);
//          estimations = catalogue.estimate(query, i / NUM_QUERY_EACH_TYPE, debug);

            line = reader.readLine();
            String[] queries_info = line.split(",");
            Double trueCard = Double.parseDouble(queries_info[2]);
            System.out.println("trueCard: " + trueCard);

            qErrors = catalogue.getQErrors(query, Integer.parseInt(info[0]), formulaType, catLen, queries_info[0], random, trueCard);


            StringJoiner sj = new StringJoiner(",");
            sj.add(query.toString());
            for (Double est : qErrors) {
                sj.add(est.toString());
            }
            resultWriter.write(sj.toString() + "\n");

            progress += 100.0 / numQueries;
            if (!debug) {
                System.out.print("\rEstimating: " + (int) progress + "%");
                System.out.println();
            }
        }

        reader.close();

        resultWriter.close();

        endTime = System.currentTimeMillis();
        System.out.println("\nEstimating: " + ((endTime - startTime) / 1000.0) + " sec");

    }
}
