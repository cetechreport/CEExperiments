package Common;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class RandomGraphGenerator {
    public static void main(String[] args) {
        int numVertices = Integer.parseInt(args[0]);
        int numEdges = Integer.parseInt(args[1]);
        int numLabels = Integer.parseInt(args[2]);
        boolean directed = Boolean.parseBoolean(args[3]);

        int[] labels = new int[numLabels];
        for (int i = 0; i < labels.length; ++i) {
            labels[i] = numVertices + i;
        }

        Random random = new Random();

        int src, dest, label;
        Map<Integer, Map<Integer, Set<Integer>>> edges = new HashMap<>();

        for (int i = 0; i < numEdges; ++i) {
            do {
                src = random.nextInt(numVertices);
                dest = random.nextInt(numVertices);
                while (dest == src) {
                    dest = random.nextInt(numVertices);
                }

                label = labels[random.nextInt(numLabels)];
            } while (edges.containsKey(src) &&
                     edges.get(src).containsKey(label) &&
                     edges.get(src).get(label).contains(dest));

            edges.putIfAbsent(src, new HashMap<>());
            edges.get(src).putIfAbsent(label, new HashSet<>());
            edges.get(src).get(label).add(dest);

            if (directed) {
                System.out.println(src + "," + label + "," + dest);
            } else {
                System.out.println(src + "," + label + "," + dest);
                System.out.println(dest + "," + label + "," + src);
            }
        }
    }
}
