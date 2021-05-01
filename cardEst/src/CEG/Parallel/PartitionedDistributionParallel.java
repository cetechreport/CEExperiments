package CEG.Parallel;

import CEG.CEG;
import CEG.Path;
import Common.Pair;
import Graphflow.CatalogueEntropy;
import PartitionedEstimation.Distribution;
import PartitionedEstimation.HashPartitioner;
import PartitionedEstimation.Partitioner;
import PartitionedEstimation.Subgraph;
import org.apache.commons.lang3.time.StopWatch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PartitionedDistributionParallel extends CatalogueEntropy {
    // patternType -> partitionScheme -> hashIdComb -> distribution
    public volatile Map<Integer, Map<String, Map<String, Distribution>>> distributions = new HashMap<>();

    // partitionScheme -> vList -> hashIdComb -> subgraph
    private volatile Map<String, Map<String, Map<String, Subgraph>>> partitions = new HashMap<>();

    // patternType -> vList -> (labelSeq, card)
    private Map<String, Map<String, List<Pair<String, Long>>>> catalogue = new HashMap<>();

    public Map<String, Map<String, Map<String, Subgraph>>> getPartitions() {
        return partitions;
    }

    public void construct(String graphFile, String queryVList, List<String> queryLabelSeqs,
        int budget, String catalogueFile, int catLength) throws Exception {

        CEG ceg = new CEG(queryVList, 2);
        List<Path> allPaths = ceg.getAllPaths();
        readCatalogue(catalogueFile, catLength);

        // get all the schemes
        Set<Map<Integer, Integer>> setOfSchemes = allPaths.stream()
            .map(path -> path.getPartitionScheme(budget))
            .collect(Collectors.toSet());
        List<Map<Integer, Integer>> schemes = new ArrayList<>(setOfSchemes);

        System.out.println(schemes.size() + ": " + schemes);

        List<Thread> threads = new ArrayList<>();
        List<PartitionedDistributionForScheme> runnables = new ArrayList<>();
        PartitionedDistributionForScheme runnable;
        Thread thread;

        // prepare the construction
        Partitioner partitioner = new HashPartitioner(graphFile, queryVList, budget);

        // compute distribution
        StopWatch watch = new StopWatch();
        watch.start();
        Map<Integer, Integer> scheme;
        for (int s = 0; s < schemes.size(); ++s) {
            scheme = schemes.get(s);

            runnable = new PartitionedDistributionForScheme(
                s,
                partitioner.partition(queryVList, queryLabelSeqs, scheme),
                scheme,
                catalogue,
                queryVList,
                queryLabelSeqs
            );
            runnables.add(runnable);

            thread = new Thread(runnable);
            threads.add(thread);
            thread.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        collect(runnables);

        watch.stop();
        System.out.println("\rComputing Distribution: " + (watch.getTime() / 1000.0) + " sec");
    }

    private void collect(List<PartitionedDistributionForScheme> runnables) {
        for (PartitionedDistributionForScheme runnable : runnables) {
            String scheme = runnable.getScheme();
            Map<String, Map<String, Subgraph>> part = runnable.getPartitions();
            for (String vList : part.keySet()) {
                for (String hashIdComb : part.get(vList).keySet()) {
                    partitions.putIfAbsent(scheme, new HashMap<>());
                    partitions.get(scheme).putIfAbsent(vList, new HashMap<>());
                    partitions.get(scheme).get(vList)
                        .put(hashIdComb, part.get(vList).get(hashIdComb));
                }
            }

            Map<Integer, Map<String, Distribution>> runnableDist = runnable.getDistributions();
            for (Integer patternType : runnableDist.keySet()) {
                for (String hashIdComb : runnableDist.get(patternType).keySet()) {
                    Distribution dist = new Distribution();
                    dist.add(runnableDist.get(patternType).get(hashIdComb));
                    distributions.putIfAbsent(patternType, new HashMap<>());
                    distributions.get(patternType).putIfAbsent(scheme, new HashMap<>());
                    distributions.get(patternType).get(scheme).put(hashIdComb, dist);
                }
            }
        }
    }

    protected void readCatalogue(String catalogueFile, int length) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader reader = new BufferedReader(new FileReader(catalogueFile));

        String patternType, vList, labelSeq;
        Long cardinality;

        String line = reader.readLine();
        String[] info;
        while (line != null) {
            info = line.split(",");
            patternType = info[0];
            vList = info[1];
            labelSeq = info[2];
            cardinality = Long.parseLong(info[3]);

            if (labelSeq.split("->").length == length) {
                catalogue.putIfAbsent(patternType, new HashMap<>());
                catalogue.get(patternType).putIfAbsent(vList, new ArrayList<>());
                catalogue.get(patternType).get(vList).add(new Pair<>(labelSeq, cardinality));
            }

            line = reader.readLine();
        }
        reader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Loading Catalogue: " + ((endTime - startTime) / 1000.0) + " sec");
    }
}
