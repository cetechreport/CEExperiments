package TrueCardinality;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CountAggregator {
    Map<Query, Long> query2card = new HashMap<>();

    private void aggregate(String cardinalityFile) throws Exception {
        Topology topology;
        Query query;

        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader csvReader = new BufferedReader(new FileReader(cardinalityFile));
        String[] info;
        String line = csvReader.readLine();
        while (null != line) {
            info = line.split(",");
            Integer[] vList = Util.toVList(info[0]);
            Integer[] labelSeq = Util.toLabelSeq(info[1]);

            topology = new Topology();
            for (int i = 0; i < vList.length; i += 2) {
                topology.addEdge(vList[i], labelSeq[i / 2], vList[i + 1]);
            }
            query = new Query(topology);

            query2card.put(
                query,
                query2card.getOrDefault(query, 0L) +
                    Long.parseLong(info[info.length - 1])
            );

            line = csvReader.readLine();
        }
        csvReader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Cardinality Loading: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void persist(String destFile) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(destFile));
        for (Query query : query2card.keySet()) {
            writer.write(query.toString() + "," + query2card.get(query).toString() + "\n");
        }
        writer.close();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("cardinalityFile: " + args[0]);
        System.out.println("destFile: " + args[1]);
        System.out.println();

        CountAggregator aggregator = new CountAggregator();
        aggregator.aggregate(args[0]);
        aggregator.persist(args[1]);
    }
}
