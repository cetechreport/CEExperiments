package Common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class QueryOutputFormatter {
    public static List<Pair<Query, String>> readQueries(String queryFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        Topology topology;
        List<Pair<Query, String>> queriesAndCards = new ArrayList<>();

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

            int i = numEdges + 2;
            while (queryString[i].equals("-1")) {
                i++;
            }
            String[] cards = Arrays.copyOfRange(queryString, i, queryString.length);
            String cardsString = String.join(",", cards);

            queriesAndCards.add(new Pair<>(new Query(topology), cardsString));

            line = csvReader.readLine();
        }

        endTime = System.currentTimeMillis();
        System.out.println("Query Loading: " + ((endTime - startTime) / 1000.0) + " sec");

        return queriesAndCards;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("originalQueryFile: " + args[0]);
        System.out.println("destNewQueryFile: " + args[1]);
        System.out.println();

        List<Pair<Query, String>> queriesAndCards = readQueries(args[0]);
        BufferedWriter writer = new BufferedWriter(new FileWriter(args[1]));
        for (Pair<Query, String> queryAndCard : queriesAndCards) {
            writer.write(queryAndCard.key.toString() + "," + queryAndCard.value + "\n");
        }
        writer.close();
    }
}
