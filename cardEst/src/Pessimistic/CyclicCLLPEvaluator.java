package Pessimistic;

import Common.Query;
import org.apache.commons.lang3.time.StopWatch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class CyclicCLLPEvaluator extends CLLPEvaluator {
    @Override
    public void estimate() throws Exception {
        StopWatch watch = new StopWatch();
        watch.start();

        int total = trueCardFiles.size();
        double progress = 0;

        for (int i = 0; i < trueCardFiles.size(); ++i) {
            Map<Query, StringJoiner> query2results = new HashMap<>();
            List<Query> queries = Query.readQueries(trueCardFiles.get(i));

            readCatalogue(catFiles.get(i), 3);
            readCatalogueMaxDeg(catMaxDegFiles.get(i), 3);
            CyclicCLLP cllp = new CyclicCLLP(null, null, null, catalogue, catalogueMaxDeg);
            for (Query query : queries) {
                Double estWithoutSubmod = cllp.estimate(query, false, true);
                Double estWithSubmod = cllp.estimate(query, true, true);

                query2results.putIfAbsent(query, new StringJoiner(","));
                query2results.get(query).add(estWithoutSubmod.toString()).add(estWithSubmod.toString());
            }

            readCatalogue(catFiles.get(i), 2);
            readCatalogueMaxDeg(catMaxDegFiles.get(i), 2);
            cllp = new CyclicCLLP(null, null, null, catalogue, catalogueMaxDeg);
            for (Query query : queries) {
                Double noTriEstWithoutSubmod = cllp.estimate(query, false, false);
                Double noTriEstWithSubmod = cllp.estimate(query, true, false);
                query2results.get(query).add(noTriEstWithoutSubmod.toString()).add(noTriEstWithSubmod.toString());
            }

            BufferedWriter resultWriter = new BufferedWriter(new FileWriter("estimation" + (i+1) + ".csv"));
            for (Query query : queries) {
                resultWriter.write(query.toString() + "," + query2results.get(query).toString() + "\n");
            }
            resultWriter.close();

            progress += 100.0 / total;
            System.out.print("\rEstimating: " + (int) progress + "%");
        }

        watch.stop();
        System.out.println("\rEstimating: " + (watch.getTime() / 1000.0) + " sec");
    }

    @Override
    public void readCatalogue(String catalogueFile, int maxLen) throws Exception {
        catalogue = new HashMap<>();

        BufferedReader catalogueReader = new BufferedReader(new FileReader(catalogueFile));
        String[] info;
        String vList, labelSeq;
        String line = catalogueReader.readLine();
        while (null != line) {
            info = line.split(",");

            vList = info[1];
            labelSeq = info[2];

            if (labelSeq.split("->").length <= maxLen) {
                Long count = Long.parseLong(info[3]);
                catalogue.putIfAbsent(vList, new HashMap<>());
                catalogue.get(vList).put(labelSeq, count);
            }

            line = catalogueReader.readLine();
        }
        catalogueReader.close();
    }

    protected void readCatalogueMaxDeg(String catalogueMaxDegFile, int maxLen) throws Exception {
        catalogueMaxDeg = new HashMap<>();
        BufferedReader reader = new BufferedReader(new FileReader(catalogueMaxDegFile));
        String line = reader.readLine();
        while (line != null) {
            String[] info = line.split(",");
            if (info[3].split(";").length <= maxLen - 1) {
                catalogueMaxDeg.putIfAbsent(info[1], new HashMap<>());
                catalogueMaxDeg.get(info[1]).putIfAbsent(info[2], new HashMap<>());
                catalogueMaxDeg.get(info[1]).get(info[2]).putIfAbsent(info[3], new HashMap<>());
                catalogueMaxDeg.get(info[1]).get(info[2]).get(info[3])
                    .putIfAbsent(info[4], Integer.parseInt(info[5]));
            }
            line = reader.readLine();
        }
        reader.close();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("catDir: " + args[0]);
        System.out.println("catMaxDegDir: " + args[1]);
        System.out.println("trueCardDir: " + args[2]);
        System.out.println();

        CLLPEvaluator evaluator = new CyclicCLLPEvaluator();
        evaluator.prepare(args[0], args[1], args[2]);
        evaluator.estimate();
    }
}
