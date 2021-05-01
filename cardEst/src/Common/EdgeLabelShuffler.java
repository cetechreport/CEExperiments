package Common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class EdgeLabelShuffler {
    private void shuffle(String graphFile, int numLabels, String destFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        List<String> edges = new ArrayList<>();
        int maxVid = Integer.MIN_VALUE;

        BufferedReader reader = new BufferedReader(new FileReader(graphFile));
        String line = reader.readLine();
        while (line != null) {
            edges.add(line);

            String[] edge = line.split(",");
            maxVid = Integer.max(maxVid, Integer.parseInt(edge[0]));
            maxVid = Integer.max(maxVid, Integer.parseInt(edge[2]));

            line = reader.readLine();
        }
        reader.close();

        Random random = new Random(0);
        BufferedWriter writer = new BufferedWriter(new FileWriter(destFile));
        for (String edgeString : edges) {
            String[] edge = edgeString.split(",");
            edge[1] = Integer.toString(random.nextInt(numLabels) + maxVid + 1);
            writer.write(String.join(",", edge) + "\n");
        }
        writer.close();

        endTime = System.currentTimeMillis();
        System.out.println("Label Shuffling: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("origGraph: " + args[0]);
        System.out.println("#labels: " + args[1]);
        System.out.println("destFile: " + args[2]);
        System.out.println();

        EdgeLabelShuffler shuffler = new EdgeLabelShuffler();
        shuffler.shuffle(args[0], Integer.parseInt(args[1]), args[2]);
    }
}
