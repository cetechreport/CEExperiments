package Partitioning;

import Common.Edge;
import Common.Pair;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;
import java.util.Set;

public class Partitioning {
    public static void printPartitions(Map<Integer, Set<Edge>> partitions) {
        for (Set<Edge> partition : partitions.values()) {
            for (Edge edge : partition) {
                edge.print();
            }
            System.out.println();
        }
    }

    public static void savePartitions(String dirName, String prefix, Map<Integer, Set<Edge>> partitions,
        Set<Integer> labels) throws Exception {
        new File(dirName).mkdirs();
        int pNum = 0;
        for (Set<Edge> partition : partitions.values()) {
            BufferedWriter partitionWriter = new BufferedWriter(
                new FileWriter(dirName + prefix + "_part_" + pNum + ".csv")
            );

            String labelString = "";
            boolean first = true;
            for (int label : labels) {
                if (!first) {
                    labelString += ",";
                }
                labelString += label;
                first = false;
            }
            partitionWriter.write(labelString + "\n");

            for (Edge edge : partition) {
                partitionWriter.write(edge.toString() + "\n");
            }
            partitionWriter.close();
            pNum++;
        }
    }

    public static void savePartitions(String dirName, Map<Integer, String> prefixes,
        Map<Integer, Set<Edge>> partitions, Map<Integer, Set<Integer>> labels) throws Exception {
        new File(dirName).mkdirs();
        int pNum = 0;
        for (int partitionKey: partitions.keySet()) {
            Set<Edge> partition = partitions.get(partitionKey);
            BufferedWriter partitionWriter = new BufferedWriter(
                new FileWriter(dirName + prefixes.get(partitionKey) + "_part_" + pNum + ".csv")
            );

            String labelString = "";
            boolean first = true;
            for (int label : labels.get(partitionKey)) {
                if (!first) {
                    labelString += ",";
                }
                labelString += label;
                first = false;
            }
            partitionWriter.write(labelString + "\n");

            for (Edge edge : partition) {
                partitionWriter.write(edge.toString() + "\n");
            }
            partitionWriter.close();
            pNum++;
        }
    }
}
