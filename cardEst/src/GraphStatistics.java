import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class GraphStatistics {
    public static void main(String[] args) throws Exception {
        Set<Integer> vertices = new HashSet<>();
        Set<Integer> edges = new HashSet<>();
        long numTriples = 0;

        BufferedReader csvReader = new BufferedReader(new FileReader(args[0]));
        int[] line;
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            line = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt).toArray();
            vertices.add(line[0]);
            vertices.add(line[2]);
            edges.add(line[1]);

            numTriples++;
            tripleString = csvReader.readLine();
        }

        System.out.println(args[0]);
        System.out.println("# Triples: " + numTriples);
        System.out.println("# Vertices: " + vertices.size());
        System.out.println("# Edges Labels: " + edges.size());
    }
}
