package PartitionedEstimation;

import Common.Pair;
import Common.Query;
import Common.Util;
import Graphflow.Constants;
import Pessimistic.CLLP;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PartitionedCLLP {
    // vListEdge -> hashIdComb -> label -> deg/count
    private Map<String, Map<String, Map<Integer, Long>>> maxOutDeg = new HashMap<>();
    private Map<String, Map<String, Map<Integer, Long>>> maxInDeg = new HashMap<>();
    private Map<String, Map<String, Map<Integer, Long>>> labelCount = new HashMap<>();

    // vList -> hashIdComb -> edge label seq -> count
    private Map<String, Map<String, Map<String, Long>>> catalogue = new HashMap<>();

    // hashIdComb -> baseVList -> baseLabelSeq -> extVList -> extLabel -> maxDeg
    private Map<String, Map<String, Map<String, Map<String, Map<String, Integer>>>>>
        catalogueMaxDeg = new HashMap<>();

    private List<String> hashIdCombs = null;

    private void readPartitionedMaxDeg(String file) throws Exception {
        Map<String, Map<String, Map<Integer, Long>>> maxDeg;

        BufferedReader reader = new BufferedReader(new FileReader(file));
        String[] info;
        String line = reader.readLine();
        while (line != null) {
            info = line.split(",");
            Integer direction = Integer.parseInt(info[0]);
            if (direction.equals(Constants.FORWARD)) {
                maxDeg = maxOutDeg;
            } else if (direction.equals(Constants.BACKWARD)) {
                maxDeg = maxInDeg;
            } else {
                System.out.println("ERROR: unsupported direction");
                return;
            }

            String vListEdge = info[1];
            String hashIdComb = info[2];
            Integer label = Integer.parseInt(info[3]);
            Long deg = Long.parseLong(info[4]);

            maxDeg.putIfAbsent(vListEdge, new HashMap<>());
            maxDeg.get(vListEdge).putIfAbsent(hashIdComb, new HashMap<>());
            maxDeg.get(vListEdge).get(hashIdComb).put(label, deg);

            line = reader.readLine();
        }
        reader.close();
    }

    private void readPartitionedLabelCount(String file) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String[] info;
        String line = reader.readLine();
        while (line != null) {
            info = line.split(",");
            String vListEdge = info[0];
            String hashIdComb = info[1];
            Integer label = Integer.parseInt(info[2]);
            Long count = Long.parseLong(info[3]);

            labelCount.putIfAbsent(vListEdge, new HashMap<>());
            labelCount.get(vListEdge).putIfAbsent(hashIdComb, new HashMap<>());
            labelCount.get(vListEdge).get(hashIdComb).put(label, count);

            line = reader.readLine();
        }
        reader.close();
    }

    private void readPartitionedCatalogue(String file) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String[] info;
        String line = reader.readLine();
        while (line != null) {
            info = line.split(",");
            String hashIdComb = info[1];
            String vList = info[2];
            String edgeLabel = info[3];
            Long count = Long.parseLong(info[4]);

            catalogue.putIfAbsent(vList, new HashMap<>());
            catalogue.get(vList).putIfAbsent(hashIdComb, new HashMap<>());
            catalogue.get(vList).get(hashIdComb).put(edgeLabel, count);

            line = reader.readLine();
        }
        reader.close();
    }

    private void readPartitionedCatMaxDeg(String file) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String[] info;
        String line = reader.readLine();
        while (line != null) {
            info = line.split(",");
            String hashIdComb = info[1];
            String baseVList = info[2];
            String baseEdgeLabel = info[3];
            String extVList = info[4];
            String extLabel = info[5];
            Integer maxDeg = Integer.parseInt(info[6]);

            catalogueMaxDeg.putIfAbsent(hashIdComb, new HashMap<>());
            catalogueMaxDeg.get(hashIdComb).putIfAbsent(baseVList, new HashMap<>());
            catalogueMaxDeg.get(hashIdComb).get(baseVList)
                .putIfAbsent(baseEdgeLabel, new HashMap<>());
            catalogueMaxDeg.get(hashIdComb).get(baseVList).get(baseEdgeLabel)
                .putIfAbsent(extVList, new HashMap<>());
            catalogueMaxDeg.get(hashIdComb).get(baseVList).get(baseEdgeLabel).get(extVList)
                .put(extLabel, maxDeg);

            line = reader.readLine();
        }
        reader.close();
    }

    private void mergeCatMaxDeg(
        Map<String, Map<String, Map<String, Map<String, Integer>>>> catMaxDeg1,
        Map<String, Map<String, Map<String, Map<String, Integer>>>> catMaxDeg2,
        String catVList, String catLabelSeq) {

        List<Pair<Pair<String, String>, Pair<String, String>>> listOfBaseAndExt =
            Util.splitToBaseAndExt(catVList, catLabelSeq, Util.getLeaves(Util.toVList(catVList)));

        for (Pair<Pair<String, String>, Pair<String, String>> baseAndExt : listOfBaseAndExt) {
            String baseVList = baseAndExt.key.key;
            String baseLabelSeq = baseAndExt.value.key;
            String extVList = baseAndExt.key.value;
            String extLabel = baseAndExt.value.value;

            int maxDeg = catMaxDeg2.get(baseVList).get(baseLabelSeq).get(extVList).get(extLabel);

            catMaxDeg1.putIfAbsent(baseVList, new HashMap<>());
            catMaxDeg1.get(baseVList).putIfAbsent(baseLabelSeq, new HashMap<>());
            catMaxDeg1.get(baseVList).get(baseLabelSeq).putIfAbsent(extVList, new HashMap<>());
            catMaxDeg1.get(baseVList).get(baseLabelSeq).get(extVList).put(extLabel, maxDeg);
        }
    }

    private Map<String, Set<String>> getVListEdge2HashIdCombs() {
        Map<String, Set<String>> vListEdge2HashIdCombs = new HashMap<>();

        for (String vListEdge : maxOutDeg.keySet()) {
            vListEdge2HashIdCombs.put(vListEdge, maxOutDeg.get(vListEdge).keySet());
        }

        return vListEdge2HashIdCombs;
    }

    public void prepareEstimate(Query query, int partType) {
        Pair<String, String> vListAndLabelSeq = Util.topologyToVListAndLabelSeq(query.topology);
        vListAndLabelSeq = Util.sort(vListAndLabelSeq);
        if (hashIdCombs == null) {
            hashIdCombs = Util.getQueryHashIdCombs(
                vListAndLabelSeq.key, getVListEdge2HashIdCombs(), partType);
        }
    }

    public double estimate(Query query, boolean submod) {
        Pair<String, String> vListAndLabelSeq = Util.topologyToVListAndLabelSeq(query.topology);
        vListAndLabelSeq = Util.sort(vListAndLabelSeq);

        double estimation = 0;
        String[] vListEdges = vListAndLabelSeq.key.split(";");
        String[] labelSeq = vListAndLabelSeq.value.split("->");
        for (String hashIdString : hashIdCombs) {
            Map<Integer, Long> partitionedMaxOutDeg = new HashMap<>();
            Map<Integer, Long> partitionedMaxInDeg = new HashMap<>();
            Map<Integer, Long> partitionedLabelCount = new HashMap<>();
            Map<String, Map<String, Long>> partitionedCatalogue = new HashMap<>();
            Map<String, Map<String, Map<String, Map<String, Integer>>>> partitionedCatalogueMaxDeg
                = new HashMap<>();

            boolean hasZeroCard = false;

            String[] hashIds = hashIdString.split(";");
            for (int i = 0; i < vListEdges.length; ++i) {
                Integer label = Integer.parseInt(labelSeq[i]);
                long maxOutDegOfLabel = maxOutDeg.get(vListEdges[i]).get(hashIds[i]).get(label);
                long maxInDegOfLabel = maxInDeg.get(vListEdges[i]).get(hashIds[i]).get(label);
                long labelCountOfLabel = labelCount.get(vListEdges[i]).get(hashIds[i]).get(label);

                if (labelCountOfLabel == 0) {
                    hasZeroCard = true;
                    break;
                }

                partitionedMaxOutDeg.put(label, maxOutDegOfLabel);
                partitionedMaxInDeg.put(label, maxInDegOfLabel);
                partitionedLabelCount.put(label, labelCountOfLabel);
            }

            if (hasZeroCard) continue;

            for (String catVList : catalogue.keySet()) {
                String catHashId =
                    Util.extractHashIdComb(catVList, vListAndLabelSeq.key, hashIdString);
                String catLabelSeq =
                    Util.extractLabelSeq(catVList, vListAndLabelSeq.key, vListAndLabelSeq.value);

                long catCard = catalogue.get(catVList).get(catHashId).get(catLabelSeq);
                if (catCard == 0) {
                    hasZeroCard = true;
                    break;
                }

                partitionedCatalogue.putIfAbsent(catVList, new HashMap<>());
                partitionedCatalogue.get(catVList).put(
                    catLabelSeq,
                    catalogue.get(catVList).get(catHashId).get(catLabelSeq));

                if (catVList.split(";").length > 1 && !catalogueMaxDeg.isEmpty()) {
                    mergeCatMaxDeg(
                        partitionedCatalogueMaxDeg, catalogueMaxDeg.get(catHashId),
                        catVList, catLabelSeq);
                }
            }

            if (hasZeroCard) continue;

            CLLP cllp = new CLLP(
                partitionedMaxOutDeg, partitionedMaxInDeg, partitionedLabelCount,
                partitionedCatalogue, partitionedCatalogueMaxDeg);
            double partialEst = cllp.estimate(query, submod);
            estimation += partialEst;
        }

        return estimation;
    }

    public PartitionedCLLP(
        String maxDegFile,
        String labelCountFile,
        String catalogueFile,
        String catMaxDegFile) throws Exception {
        readPartitionedMaxDeg(maxDegFile);
        readPartitionedLabelCount(labelCountFile);
        if (catalogueFile != null) {
            readPartitionedCatalogue(catalogueFile);
        }
        if (catMaxDegFile != null) {
            readPartitionedCatMaxDeg(catMaxDegFile);
        }
    }
}
