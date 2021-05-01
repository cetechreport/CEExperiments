package CEG;

import Common.Pair;
import Common.Query;
import Common.Util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Pessimistic extends CEG {
    // partitionScheme -> hashIdComb -> baseVList -> baseLabelSeq -> extVList -> extLabel -> maxDeg
    private Map<String, Map<String, Map<String, Map<String, Map<String, Map<String, Integer>>>>>>
        catalogueMaxDegs = new HashMap<>();

    private void readPartitionedCatalogueMaxdeg(String catalogueMaxDegFile) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(catalogueMaxDegFile));
        String[] info;
        String line = reader.readLine();
        while (line != null) {
            info = line.split(",");
            String scheme = info[1];
            String hashIdComb = info[2];
            String baseVList = info[3];
            String baseEdgeLabel = info[4];
            String extVList = info[5];
            String extLabel = info[6];
            Integer maxDeg = Integer.parseInt(info[7]);

            catalogueMaxDegs.putIfAbsent(scheme, new HashMap<>());
            catalogueMaxDegs.get(scheme).putIfAbsent(hashIdComb, new HashMap<>());
            catalogueMaxDegs.get(scheme).get(hashIdComb).putIfAbsent(baseVList, new HashMap<>());
            catalogueMaxDegs.get(scheme).get(hashIdComb).get(baseVList)
                .putIfAbsent(baseEdgeLabel, new HashMap<>());
            catalogueMaxDegs.get(scheme).get(hashIdComb).get(baseVList).get(baseEdgeLabel)
                .putIfAbsent(extVList, new HashMap<>());
            catalogueMaxDegs.get(scheme).get(hashIdComb).get(baseVList).get(baseEdgeLabel).get(extVList)
                .put(extLabel, maxDeg);

            line = reader.readLine();
        }
        reader.close();
    }

    public BigInteger estimate(Query query, int budget) {
        Pair<String, String> vListAndLabelSeq = Util.topologyToVListAndLabelSeq(query.topology);
        vListAndLabelSeq = Util.sort(vListAndLabelSeq);
        List<Path> allPaths = getAllPaths();
        Map<Integer, Integer> numPartitions;
        List<String> hashIdCombs;
        Map<String, Map<String, Map<String, Long>>> catalogue;
        Map<String, Map<String, Map<String, Map<String, Map<String, Integer>>>>> catalogueMaxDeg;

        BigInteger estimation = new BigInteger("-1");
        BigInteger NEG_ONE = new BigInteger("-1");
        for (Path path : allPaths) {
            numPartitions = path.getPartitionScheme(budget);
            String partitionScheme =
                Util.partitionSchemeToString(vListAndLabelSeq.key, numPartitions);
            prepareHashIdCombs(query, partitionScheme);
            hashIdCombs = scheme2hashIdCombs.get(partitionScheme);
            catalogue = catalogues.get(partitionScheme);
            catalogueMaxDeg = catalogueMaxDegs.get(partitionScheme);

            BigInteger estimationOfPath = new BigInteger("0");
            for (String hashIdString : hashIdCombs) {
                BigInteger estimationOfPartition = new BigInteger("1");
                for (int i = 0; i < path.size() - 1; ++i) {
                    String extString = path.getExt(i + 1);
                    Integer[] extEdge = Util.toVList(extString);

                    Set<Integer> baseVertices =
                        new HashSet<>(Arrays.asList(Util.toVList(path.get(i))));
                    Integer baseV;
                    if (baseVertices.contains(extEdge[0])) {
                        baseV = extEdge[0];
                    } else if (baseVertices.contains(extEdge[1])) {
                        baseV = extEdge[1];
                    } else {
                        System.err.println("ERROR: not an extension edge");
                        System.err.println("  base: " + path.get(i));
                        System.err.println("  next: " + path.get(i + 1));
                        return null;
                    }

                    Set<String> baseEdges = new HashSet<>();
                    Integer[] baseVList = Util.toVList(path.get(i));
                    for (int j = 0; j < baseVList.length - 1; j += 2) {
                        if (baseVList[j].equals(baseV) || baseVList[j + 1].equals(baseV)) {
                            baseEdges.add(baseVList[j] + "-" + baseVList[j + 1]);
                        }
                    }

                    Integer tightestMaxDeg = Integer.MAX_VALUE;
                    for (String baseEdge : baseEdges) {
                        List<String> catEntryEdges = new ArrayList<>();
                        catEntryEdges.add(extString);
                        catEntryEdges.add(baseEdge);
                        Collections.sort(catEntryEdges);
                        String catEntry = String.join(";", catEntryEdges);
                        String catEntryHashId =
                            Util.extractHashIdComb(catEntry, vListAndLabelSeq.key, hashIdString);

                        String baseLabelSeq =
                            Util.extractLabelSeq(baseEdge, vListAndLabelSeq.key, vListAndLabelSeq.value);
                        String extLabelSeq =
                            Util.extractLabelSeq(extString, vListAndLabelSeq.key, vListAndLabelSeq.value);

                        Integer maxdeg = Integer.MAX_VALUE;
                        if (catalogueMaxDeg.containsKey(catEntryHashId) &&
                            catalogueMaxDeg.get(catEntryHashId).containsKey(baseEdge) &&
                            catalogueMaxDeg.get(catEntryHashId).get(baseEdge).containsKey(baseLabelSeq) &&
                            catalogueMaxDeg.get(catEntryHashId).get(baseEdge).get(baseLabelSeq).containsKey(extString) &&
                            catalogueMaxDeg.get(catEntryHashId).get(baseEdge).get(baseLabelSeq).get(extString)
                                .containsKey(extLabelSeq)) {

                            maxdeg = catalogueMaxDeg.get(catEntryHashId).get(baseEdge)
                                .get(baseLabelSeq).get(extString).get(extLabelSeq);
                        }
                        tightestMaxDeg = Math.min(tightestMaxDeg, maxdeg);
                    }

                    // "estimationOfPartition *= tightestMaxDeg;"
                    estimationOfPartition = estimationOfPartition.multiply(BigInteger.valueOf(tightestMaxDeg));
                }

                // sum up estimation from each partition
                String startEntryHashId =
                    Util.extractHashIdComb(path.get(0), vListAndLabelSeq.key, hashIdString);
                String startEntryLabelSeq =
                    Util.extractLabelSeq(path.get(0), vListAndLabelSeq.key, vListAndLabelSeq.value);
                long startEntryCard = 0;
                if (catalogue.get(path.get(0)).containsKey(startEntryHashId) &&
                    catalogue.get(path.get(0)).get(startEntryHashId).containsKey(startEntryLabelSeq)) {
                    startEntryCard = catalogue.get(path.get(0)).get(startEntryHashId).get(startEntryLabelSeq);
                }
                // "estimationOfPath += estimationOfPartition * startEntryCard"
                estimationOfPath = estimationOfPath.add(
                    estimationOfPartition.multiply(BigInteger.valueOf(startEntryCard)));
            }

            if (estimation.equals(NEG_ONE)) {
                // if estimation has never been updated, then just assign
                estimation = estimationOfPath;
            } else {
                // if estimationOfPath is less than estimation, update
                if (estimation.compareTo(estimationOfPath) > 0) {
                    estimation = estimationOfPath;
                }
            }
        }

        return estimation;
    }

    public Pessimistic(String queryVList, int minLen, String pcatFile, String pcatMaxdegFile) {
        super(queryVList, minLen);
        try {
            readPartitionedCatalogue(pcatFile);
            readPartitionedCatalogueMaxdeg(pcatMaxdegFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
