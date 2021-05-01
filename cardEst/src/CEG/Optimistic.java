package CEG;

import Common.Pair;
import Common.Query;
import Common.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Optimistic extends CEG {
    public enum Mode { MIN_MAX, MIN_MAX_AVG }

    public Double[] estimate(Query query, int budget, Mode mode) {
        Pair<String, String> vListAndLabelSeq = Util.topologyToVListAndLabelSeq(query.topology);
        vListAndLabelSeq = Util.sort(vListAndLabelSeq);
        List<Path> allPaths = getAllPaths();
        Map<Integer, Integer> numPartitions;
        List<String> hashIdCombs;
        Map<String, Map<String, Map<String, Long>>> catalogue;

        Map<String, Double> fullFormula2est = new HashMap<>();

        Double[] estimation = new Double[] { Double.MAX_VALUE, Double.MIN_VALUE };
        for (Path path : allPaths) {
            numPartitions = path.getPartitionScheme(budget);
            String partitionScheme =
                Util.partitionSchemeToString(vListAndLabelSeq.key, numPartitions);
            prepareHashIdCombs(query, partitionScheme);
            hashIdCombs = scheme2hashIdCombs.get(partitionScheme);
            catalogue = catalogues.get(partitionScheme);

            Double[] estimationOfPath = new Double[] { 0.0, 0.0 };
            for (String hashIdString : hashIdCombs) {
                // (formula, est)
                Set<Pair<String, Double>> formulaeEst = new HashSet<>();

                Double[] estimationOfPartition = new Double[] { 1.0, 1.0 };
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
                    for (int j = 0; j < baseVList.length; j += 2) {
                        if (baseVList[j].equals(baseV) || baseVList[j + 1].equals(baseV)) {
                            baseEdges.add(baseVList[j] + "-" + baseVList[j + 1]);
                        }
                    }

                    // update formula with the current fraction term and estimation
                    // note that each denominator creates a different branch of formula
                    Set<Pair<String, Double>> updatedFormula2est = new HashSet<>();
                    Double[] estimationOfLevel = new Double[] { Double.MAX_VALUE, Double.MIN_VALUE };
                    for (String baseEdge : baseEdges) {
                        List<String> numeratorEdges = new ArrayList<>();
                        numeratorEdges.add(extString);
                        numeratorEdges.add(baseEdge);
                        Collections.sort(numeratorEdges);
                        String numerator = String.join(";", numeratorEdges);

                        String numerHashId =
                            Util.extractHashIdComb(numerator, vListAndLabelSeq.key, hashIdString);
                        String numerLabelSeq =
                            Util.extractLabelSeq(numerator, vListAndLabelSeq.key, vListAndLabelSeq.value);
                        String denomHashId =
                            Util.extractHashIdComb(baseEdge, vListAndLabelSeq.key, hashIdString);
                        String denomLabelSeq =
                            Util.extractLabelSeq(baseEdge, vListAndLabelSeq.key, vListAndLabelSeq.value);

                        double numeratorValue = 0;
                        if (catalogue.get(numerator).containsKey(numerHashId) &&
                            catalogue.get(numerator).get(numerHashId).containsKey(numerLabelSeq)) {
                            numeratorValue = catalogue.get(numerator).get(numerHashId).get(numerLabelSeq);
                        }
                        double denominatorValue = 0;
                        if (catalogue.get(baseEdge).containsKey(denomHashId) &&
                            catalogue.get(baseEdge).get(denomHashId).containsKey(denomLabelSeq)) {
                            denominatorValue = catalogue.get(baseEdge).get(denomHashId).get(denomLabelSeq);
                        }

                        double est = 0;
                        if (numeratorValue > 0 && denominatorValue > 0) {
                            est = numeratorValue / denominatorValue;
                        }

                        if (mode == Mode.MIN_MAX_AVG) {
                            computeAllPaths(i, formulaeEst, updatedFormula2est, numerator, baseEdge, est);
                        } else if (mode == Mode.MIN_MAX) {
                            estimationOfLevel[0] = Math.min(estimationOfLevel[0], est);
                            estimationOfLevel[1] = Math.max(estimationOfLevel[1], est);
                        }
                    }

                    formulaeEst = updatedFormula2est;
                    estimationOfPartition[0] *= estimationOfLevel[0];
                    estimationOfPartition[1] *= estimationOfLevel[1];
                }

                // sum up estimation from each partition
                String startEntry = path.get(0);
                String startHashId =
                    Util.extractHashIdComb(startEntry, vListAndLabelSeq.key, hashIdString);
                String startLabelSeq =
                    Util.extractLabelSeq(startEntry, vListAndLabelSeq.key, vListAndLabelSeq.value);
                double startEst = 0;
                if (catalogue.get(startEntry).containsKey(startHashId) &&
                    catalogue.get(startEntry).get(startHashId).containsKey(startLabelSeq)) {
                    startEst = catalogue.get(startEntry).get(startHashId).get(startLabelSeq);
                }
                if (mode == Mode.MIN_MAX_AVG) {
                    for (Pair<String, Double> formulaAndEst : formulaeEst) {
                        String fullFormula = startEntry + formulaAndEst.key;
                        Double fullEst = formulaAndEst.value * startEst;
                        fullFormula2est.put(
                            fullFormula,
                            fullFormula2est.getOrDefault(fullFormula, 0.0) + fullEst
                        );
                    }
                } else if (mode == Mode.MIN_MAX) {
                    estimationOfPath[0] += startEst * estimationOfPartition[0];
                    estimationOfPath[1] += startEst * estimationOfPartition[1];
                }
            }

            estimation[0] = Math.min(estimation[0], estimationOfPath[0]);
            estimation[1] = Math.max(estimation[1], estimationOfPath[1]);
        }

        if (mode == Mode.MIN_MAX_AVG) {
            Double sum = 0.0;
            Double min = Double.MAX_VALUE;
            Double max = Double.MIN_VALUE;
            for (Double est : fullFormula2est.values()) {
                sum += est;
                min = Math.min(min, est);
                max = Math.max(max, est);
            }

            return new Double[] {min, max, sum / fullFormula2est.size()};
        } else if (mode == Mode.MIN_MAX) {
            return estimation;
        } else {
            System.err.println("ERROR: unrecognized mode");
            System.err.println("   mode: " + mode);
            return null;
        }
    }

    private void computeAllPaths(
        int level,
        Set<Pair<String, Double>> formulaeEst,
        Set<Pair<String, Double>> updatedFormula2est,
        String numerator,
        String baseEdge,
        double est) {

        if (level == 0) {
            updatedFormula2est.add(new Pair<>("," + numerator + "," + baseEdge, est));
        } else {
            // create branches of formulae for different denominator
            for (Pair<String, Double> formulaAndEst : formulaeEst) {
                Pair<String, Double> updated = new Pair<>(formulaAndEst);
                updated.key += "," + numerator + "," + baseEdge;
                updated.value *= est;
                updatedFormula2est.add(updated);
            }
        }
    }

    public Optimistic(String queryVList, int minLen, String pcatFile) {
        super(queryVList, minLen);
        try {
            readPartitionedCatalogue(pcatFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
