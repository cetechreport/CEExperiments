package Graphflow;
import Common.Pair;
import Common.Query;
import Common.Triple;
import Common.Util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class EntropyComp {
    static int index;

    private static double min(List<Double []> measures, int i, double trueCard) {
        double minsofar = Math.abs(measures.get(0)[i]);
        double result = measures.get(0)[0];
        index = 0;
        for (int j = 1; j < measures.size(); ++j) {
            if (minsofar > Math.abs(measures.get(j)[i])) {
                minsofar = Math.abs(measures.get(j)[i]);
                result = measures.get(j)[0];
                index = i;
            }
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        String method = args[0];
        System.out.println("estimationMethod: " + method);
        List<Query> queries = Query.readQueries(args[args.length - 1]);
        int numQueries = queries.size();
        double progress = 0;
        long startTime = System.currentTimeMillis();
        long endTime;
        BufferedWriter resultWriter = new BufferedWriter(new FileWriter(args[2]));
        System.out.println("catalogueFile: " + args[1]);
        System.out.println("destFile: " + args[2]);
        System.out.println("maxLen: " + args[3]);
        System.out.println("formulaType: " + args[4]);
        System.out.println("measurementFile: " + args[5]);
        System.out.println("queries: " + args[6]);
        System.out.println();
        boolean debug = Boolean.parseBoolean(args[2]);
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
        List<Triple<List<Double>, String, Double>> entropy;
        Catalogue catalogue = new Catalogue(args[1], catLen);

//        resultWriter.write("p*,mean,sum,max,max-max" + '\n');


        System.out.println("------- EVALUATION RESULT -------");
        for (int i = 0; i < queries.size(); ++i) {
            Query query = queries.get(i);
//          estimations = catalogue.estimate(query, i / NUM_QUERY_EACH_TYPE, debug);
            line = reader.readLine();
            String[] queries_info = line.split(",");
            double trueCard = Double.parseDouble(queries_info[2]);
            entropy = catalogue.getAllEntropy(query, Integer.parseInt(info[0]), formulaType, catLen, queries_info[0], args[5]);

            StringJoiner sj = new StringJoiner(",");
            sj.add(query.toString());
            List<Double []> measures = new ArrayList<>();
            int hops = 0;
            for (Triple<List<Double>, String, Double> point : entropy) {
                double est = point.v3;
                double result = est;
                if(result >= trueCard) result = Math.log10(result / trueCard);
                else result = - Math.log10(trueCard / result);

                double max, mean;
                double sum = 0.0;
                for (double entro : point.v1) {
                    sum += entro;
                }
                mean = sum / point.v1.size();
                max = point.v1.get(0);
                for (int j = 1; j < point.v1.size(); ++j) {
                    if (point.v1.get(j) > max) max = point.v1.get(j);
                }
                if (hops < point.v1.size()) {
                    measures.clear();
                    hops = point.v1.size();
                }
                if (hops == point.v1.size()) measures.add(new Double[]{result, mean, sum, max});
            }
            resultWriter.write(min(measures, 0, trueCard) + "," + min(measures, 1, trueCard) + "," + min(measures, 2, trueCard) + "," +
                    min(measures, 3, trueCard) + ",");
            double maxmax = catalogue.estimateByHops(query, Integer.parseInt(info[0]), formulaType, catLen, queries_info[0])[7];
            if(maxmax >= trueCard) maxmax = Math.log10(maxmax / trueCard);
            else maxmax = - Math.log10(trueCard / maxmax);
            resultWriter.write(maxmax + "\n");

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

