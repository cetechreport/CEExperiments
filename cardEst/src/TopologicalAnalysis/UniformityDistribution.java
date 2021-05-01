package TopologicalAnalysis;

import Common.Pair;
import Common.Query;
import Common.Topology;
import IMDB.Labels;
import TopologicalAnalysis.Parallel.UniformityDistributionPerSources;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

public class UniformityDistribution {
    // path: 0-1;1-2;2-5;5-6 / 0-1;1-2;2-5
    //    * calculate #5's and #6's for numerator, distribution of #paths for each 5
    //    * for each 5, also calc the number of instances of 0-1;1-2;2-5 attached on it
    // star: 2-5;3-5;4-5;5-6 / 2-5;3-5;4-5
    //    * calculate #5's and #6's for numerator, distribution of #stars for each 5
    //    * for each 5, also calc the number of instances of 2-5;3-5;4-5 attached on it

    Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();

    private void execute(String graphFile, String queryFile, String vListString) throws Exception {
        readGraph(graphFile);
        List<Query> queries = readQueries(queryFile);
        Set<String> labelSeqs = getLabelSeqs(queries, vListString);
        computeDistribution(vListString, labelSeqs);
    }

    private void computeDistribution(String vListString, Set<String> labelSeqs) {
        long startTime = System.currentTimeMillis();
        long endTime;

        List<Thread> threads = new ArrayList<>();
        Runnable worker;
        Thread thread;

        List<Integer> starters = new ArrayList<>(dest2label2src.keySet());

        final int NUM_STARTER_WORKERS = 15;
        int threadId = 0;

        final int targetBaseV = 5;
        final int targetExtV = 6;

        for (String labelSeq : labelSeqs) {
            for (int j = 0; j < starters.size(); j += starters.size() / NUM_STARTER_WORKERS) {
                threadId++;

                worker = new UniformityDistributionPerSources(
                    threadId, src2label2dest, dest2label2src, vListString, labelSeq,
                    targetBaseV, targetExtV,
                    starters.subList(
                        j, Math.min(starters.size(), j + starters.size() / NUM_STARTER_WORKERS)));

                thread = new Thread(worker);
                threads.add(thread);
                thread.start();
            }
        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        endTime = System.currentTimeMillis();
        System.out.println("Distribution Computing: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void readGraph(String graphFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader csvReader = new BufferedReader(new FileReader(graphFile));
        int[] line;
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            line = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt).toArray();

            src2label2dest.putIfAbsent(line[0], new HashMap<>());
            src2label2dest.get(line[0]).putIfAbsent(line[1], new ArrayList<>());
            src2label2dest.get(line[0]).get(line[1]).add(line[2]);

            dest2label2src.putIfAbsent(line[2], new HashMap<>());
            dest2label2src.get(line[2]).putIfAbsent(line[1], new ArrayList<>());
            dest2label2src.get(line[2]).get(line[1]).add(line[0]);

            tripleString = csvReader.readLine();
        }

        endTime = System.currentTimeMillis();
        System.out.println("Graph Loading: " + ((endTime - startTime) / 1000.0) + " sec");

        csvReader.close();
    }

    private List<Query> readQueries(String queryFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        Topology topology;
        List<Pair<String, Integer>> filters;
        String operator;
        int literal;

        List<Query> queries = new ArrayList<>();

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

            int numFilters = Integer.parseInt(queryString[numEdges + 1]);

            filters = initFilters(numFilters);
            for (int f = 0; f < numFilters; ++f) {
                String filter = queryString[f + numEdges + 2];
                if (!filter.equals("-1")) {
                    if (filter.charAt(1) == '=') {
                        operator = filter.substring(0, 2);
                        literal = Integer.parseInt(filter.substring(2));
                    } else {
                        operator = filter.substring(0, 1);
                        literal = Integer.parseInt(filter.substring(1));
                    }

                    filters.get(f).key = operator;
                    filters.get(f).value = literal;
                }
            }

            queries.add(new Query(topology, filters));

            line = csvReader.readLine();
        }

        endTime = System.currentTimeMillis();
        System.out.println("Query Loading: " + ((endTime - startTime) / 1000.0) + " sec");

        return queries;
    }

    private List<Pair<String, Integer>> initFilters(int numVertices) {
        List<Pair<String, Integer>> filters = new ArrayList<>();
        for (int i = 0; i < numVertices; ++i) {
            filters.add(new Pair<>("", Labels.NO_FILTER));
        }
        return filters;
    }

    private Integer[] toVList(String vListString) {
        String[] splitted = vListString.split(";");
        Integer[] vList = new Integer[splitted.length * 2];
        for (int i = 0; i < splitted.length; i++) {
            String[] srcDest = splitted[i].split("-");
            vList[i * 2] = Integer.parseInt(srcDest[0]);
            vList[i * 2 + 1] = Integer.parseInt(srcDest[1]);
        }
        return vList;
    }

    private Set<String> getLabelSeqs(List<Query> queries, String vListString) {
        Integer[] vList = toVList(vListString);

        Set<String> labelSeqs = new HashSet<>();
        Integer label;
        for (Query query : queries) {
            StringJoiner sj = new StringJoiner("->");
            for (int i = 0; i < vList.length; i += 2) {
                label = query.topology.src2dest2label.get(vList[i]).get(vList[i + 1]);
                sj.add(label.toString());
            }
            labelSeqs.add(sj.toString());
        }

        return labelSeqs;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println("queryFile: " + args[1]);
        System.out.println("vListString: " + args[2]);
        System.out.println();

        UniformityDistribution distribution = new UniformityDistribution();
        distribution.execute(args[0], args[1], args[2]);
    }
}
