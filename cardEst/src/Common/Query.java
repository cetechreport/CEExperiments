package Common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

public class Query {
    public Path path;

    // (operation, literal) of the i-th vertex in the query path
    public List<Pair<String, Integer>> filters;

    public Topology topology;

    public Query(Path path, List<Pair<String, Integer>> filters) {
        this.path = path;
        this.filters = filters;
    }

    public Query(Topology topology, List<Pair<String, Integer>> filters) {
        this.topology = topology;
        this.filters = filters;
    }

    public Query(Topology topology) {
        this.topology = topology;
    }

    public static List<Query> readQueries(String queryFile) throws Exception {
        List<Query> queries = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new FileReader(queryFile));
        String line = reader.readLine();
        while (line != null) {
            String[] info = line.split(",");
            Integer[] vList = Util.toVList(info[0]);
            Integer[] labelSeq = Util.toLabelSeq(info[1]);

            Topology topology = new Topology();
            Integer src, dest, label;
            for (int i = 0; i < vList.length; i += 2) {
                src = vList[i];
                dest = vList[i + 1];
                label = labelSeq[i / 2];
                topology.addEdge(src, label, dest);
            }
            Query query = new Query(topology);
            queries.add(query);

            line = reader.readLine();
        }
        reader.close();

        return queries;
    }

    public String extractLabelSeq(Integer[] vList) {
        Set<Integer> intersection = new HashSet<>();
        StringJoiner sj = new StringJoiner("->");
        for (int i = 0; i < vList.length; i += 2) {
            intersection.clear();
            intersection.addAll(topology.outgoing.get(vList[i]).keySet());
            intersection.retainAll(topology.incoming.get(vList[i + 1]).keySet());

            // expected to have only 1 label
            for (Integer label : intersection) {
                sj.add(label.toString());
            }
        }
        return sj.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Query query = (Query) o;

        return topology.equals(query.topology);
    }

    @Override
    public int hashCode() {
        return topology.hashCode();
    }

    @Override
    public String toString() {
        return topology.toString();
    }
}
