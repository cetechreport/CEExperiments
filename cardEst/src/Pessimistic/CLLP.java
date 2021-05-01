package Pessimistic;

import Common.Pair;
import Common.Query;
import Common.Topology;
import Common.Util;
import Graphflow.Constants;
import com.google.ortools.linearsolver.MPConstraint;
import com.google.ortools.linearsolver.MPObjective;
import com.google.ortools.linearsolver.MPSolver;
import com.google.ortools.linearsolver.MPSolver.ResultStatus;
import com.google.ortools.linearsolver.MPVariable;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class CLLP {
    static { System.loadLibrary("jniortools");}

    final double INFINITY = MPSolver.infinity();
    final double NEG_INFINITY = -MPSolver.infinity();

    protected Map<Integer, Long> maxOutDeg = new HashMap<>();
    protected Map<Integer, Long> maxInDeg = new HashMap<>();
    protected Map<Integer, Long> labelCount = new HashMap<>();

    // decomVListString (defining decom topology) -> edge label seq -> count
    protected Map<String, Map<String, Long>> catalogue = new HashMap<>();

    // baseVList -> baseLabelSeq -> extVList -> extLabel -> maxDeg
    protected Map<String, Map<String, Map<String, Map<String, Integer>>>> catalogueMaxDeg =
        new HashMap<>();

    protected boolean isZero = false;

    protected Set<Set<String>> getPowerSet(String vListString) {
        Set<String> vListSet = Util.toVListSet(vListString);

        Set<Set<String>> powerSet = new HashSet<>();
        Set<Set<String>> vListSizeCurrent = new HashSet<>();
        vListSizeCurrent.add(vListSet);
        Set<Set<String>> vListSizeNext = new HashSet<>();
        for (int i = 0; i < vListSet.size(); ++i) {
            for (Set<String> vListSize : vListSizeCurrent) {
                for (String edge : vListSize) {
                    Set<String> subSet = new HashSet<>(vListSize);
                    subSet.remove(edge);
                    vListSizeNext.add(subSet);
                }
            }

            powerSet.addAll(vListSizeCurrent);
            vListSizeCurrent = vListSizeNext;
            vListSizeNext = new HashSet<>();
        }

        // add empty set
        powerSet.add(new HashSet<>());

        return powerSet;
    }

    protected String getId(Set<String> set) {
        if (set.isEmpty()) return "h_";

        List<String> vList = new ArrayList<>(set);
        Collections.sort(vList);
        return "h_" + String.join(";", vList);
    }

    protected double getLogMaxDegOrLabelCount(Set<String> set1, Set<String> set2, Topology topology) {
        Set<String> extSet = new HashSet<>(set2);
        extSet.removeAll(set1);
        Set<Integer> base = new HashSet<>(Arrays.asList(Util.toVList(Util.toVListString(set1))));

        for (String extVList : extSet) {
            String[] srcDest = extVList.split("-");
            Integer src = Integer.parseInt(srcDest[0]);
            Integer dest = Integer.parseInt(srcDest[1]);
            for (Integer label : topology.outgoing.get(src).keySet()) {
                for (Integer labelDest : topology.outgoing.get(src).get(label)) {
                    if (labelDest.equals(dest)) {
                        if (base.isEmpty()) {
                            return Math.log(labelCount.get(label));
                        } else if (base.contains(src)) {
                            if (maxOutDeg.get(label).equals(0L)) {
                                isZero = true;
                                return 0;
                            } else {
                                return Math.log(maxOutDeg.get(label));
                            }
                        } else if (base.contains(dest)) {
                            if (maxInDeg.get(label).equals(0L)) {
                                isZero = true;
                                return 0;
                            } else {
                                return Math.log(maxInDeg.get(label));
                            }
                        } else {
                            return Math.log(Math.max(maxOutDeg.get(label), maxInDeg.get(label)));
                        }
                    }
                }
            }
        }

        // should not reach here
        return -1;
    }

    protected void readMaxDeg(String maxDegFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader reader = new BufferedReader(new FileReader(maxDegFile));
        String line = reader.readLine();
        while (line != null) {
            String[] info = line.split(",");
            Integer dir = Integer.parseInt(info[0]);
            if (dir.equals(Constants.FORWARD)) {
                maxOutDeg.put(Integer.parseInt(info[1]), Long.parseLong(info[2]));
            } else {
                maxInDeg.put(Integer.parseInt(info[1]), Long.parseLong(info[2]));
            }
            line = reader.readLine();
        }
        reader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Loading MaxDeg: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    protected void readLabelCount(String labelCountFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader reader = new BufferedReader(new FileReader(labelCountFile));
        String line = reader.readLine();
        while (line != null) {
            String[] info = line.split(",");
            labelCount.put(Integer.parseInt(info[0]), Long.parseLong(info[1]));
            line = reader.readLine();
        }
        reader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Loading LabelCount: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public void readCatalogue(String catalogueFile, int maxLen) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader catalogueReader = new BufferedReader(new FileReader(catalogueFile));
        String[] info;
        String vList, labelSeq;
        String line = catalogueReader.readLine();
        while (null != line) {
            info = line.split(",");

            vList = info[1];
            labelSeq = info[2];

            if (labelSeq.split("->").length <= maxLen) {
                Long count = Long.parseLong(info[3]);
                catalogue.putIfAbsent(vList, new HashMap<>());
                catalogue.get(vList).put(labelSeq, count);
            }

            line = catalogueReader.readLine();
        }
        catalogueReader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Loading Catalogue: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    protected void readCatalogueMaxDeg(String catalogueMaxDegFile) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(catalogueMaxDegFile));
        String line = reader.readLine();
        while (line != null) {
            String[] info = line.split(",");
            catalogueMaxDeg.putIfAbsent(info[1], new HashMap<>());
            catalogueMaxDeg.get(info[1]).putIfAbsent(info[2], new HashMap<>());
            catalogueMaxDeg.get(info[1]).get(info[2]).putIfAbsent(info[3], new HashMap<>());
            catalogueMaxDeg.get(info[1]).get(info[2]).get(info[3])
                .putIfAbsent(info[4], Integer.parseInt(info[5]));
            line = reader.readLine();
        }
        reader.close();
    }

    protected String getMaxDegCTName(String id1, String id2, double logMaxDeg) {
        return "MaxDeg: " + id2 + " - " + id1 + " <= " + logMaxDeg;
    }

    protected String getGrowCTName(String id1, String id2) {
        return "Grow: " + id1 + " - " + id2 + " <= 0";
    }

    protected String getSubmodCTName(
        String id1, String id2, String union, String intersection) {
        return "Submod: " + union + " + " + intersection + " - " + id1 + " - " + id2 + " <= 0";
    }

    protected String getCardEquCTName(String id, double cardinality) {
        return "Card: " + id + " <= " + cardinality;
    }

    protected double getLogCat2MaxDeg(Set<String> set1, Set<String> set2, String vListString, String labelSeqString) {
        Set<String> extSet = new HashSet<>(set2);
        extSet.removeAll(set1);
        Set<Integer> base = new HashSet<>(Arrays.asList(Util.toVList(Util.toVListString(set1))));

        for (String extVList : extSet) {
            String extLabel = Util.extractLabelSeq(extVList, vListString, labelSeqString);
            String[] srcDest = extVList.split("-");
            Integer src = Integer.parseInt(srcDest[0]);
            Integer dest = Integer.parseInt(srcDest[1]);

            Integer baseV;

            if (base.contains(src)) {
                baseV = src;
            } else if (base.contains(dest)) {
                baseV = dest;
            } else {
                System.err.println("ERROR: not connected");
                System.err.println("   " + set1 + "," + set2);
                return -1;
            }

            Set<String> baseEdges = new HashSet<>();
            for (String edge : set1) {
                Integer[] edgeVList = Util.toVList(edge);
                if (edgeVList[0].equals(baseV) || edgeVList[1].equals(baseV)) {
                    baseEdges.add(edgeVList[0] + "-" + edgeVList[1]);
                }
            }

            Integer minExtMaxDeg = Integer.MAX_VALUE;
            for (String baseEdge : baseEdges) {
                String baseLabel = Util.extractLabelSeq(baseEdge, vListString, labelSeqString);
                Integer maxDeg = catalogueMaxDeg.get(baseEdge).get(baseLabel).get(extVList).get(extLabel);
                minExtMaxDeg = Math.min(minExtMaxDeg, maxDeg);
            }

            return Math.log(minExtMaxDeg);
        }

        System.err.println("ERROR: should not reach here");
        return -1;
    }

    protected MPSolver createBasicLP(String vListString, String labelSeqString, boolean subMod) {
        MPSolver solver =
            new MPSolver("CLLP", MPSolver.OptimizationProblemType.GLOP_LINEAR_PROGRAMMING);
        MPVariable empty = solver.makeNumVar(0.0, 0.0, getId(new HashSet<>()));

        Set<Set<String>> allSubSets = getPowerSet(vListString);
        allSubSets = allSubSets.stream().filter(Util::isConnected).collect(Collectors.toSet());

        Set<Set<String>> generatedSubmod = new HashSet<>();

        for (Set<String> set1 : allSubSets) {
            String id1 = getId(set1);
            for (Set<String> set2 : allSubSets) {
                if (set1.equals(set2)) continue;
                String id2 = getId(set2);

                MPVariable h1 = solver.lookupVariableOrNull(id1);
                if (h1 == null) {
                    h1 = solver.makeNumVar(0.0, INFINITY, id1);
                }
                MPVariable h2 = solver.lookupVariableOrNull(id2);
                if (h2 == null) {
                    h2 = solver.makeNumVar(0.0, INFINITY, id2);
                }

                if (set1.size() < 2 || set2.size() < 2) continue;

                Set<String> ext = new HashSet<>(set2);
                ext.removeAll(set1);

                // if set2 is one edge extended from set1
                if (set2.containsAll(set1) && ext.size() == 1) {
                    // add the constraint of max deg or card
//                    double logMaxDegOrCard = getLogMaxDegOrLabelCount(set1, set2, topology);
                    double logMaxDegOrCard = getLogCat2MaxDeg(set1, set2, vListString, labelSeqString);
                    MPConstraint ct1 = solver.makeConstraint(
                        NEG_INFINITY, logMaxDegOrCard, getMaxDegCTName(id1, id2, logMaxDegOrCard));
                    ct1.setCoefficient(h1, -1);
                    ct1.setCoefficient(h2, 1);

                    // add the constraint of h(X) - h(Y) <= 0
                    MPConstraint ct2 = solver.makeConstraint(
                        NEG_INFINITY, 0.0, getGrowCTName(id1, id2));
                    ct2.setCoefficient(h1, 1);
                    ct2.setCoefficient(h2, -1);
                }

                // add sub-modularity constraint, if specified
                if (subMod && !set1.isEmpty() && !set2.isEmpty()) {
                    Set<String> submodPair = new HashSet<>();
                    submodPair.add(id1);
                    submodPair.add(id2);
                    if (generatedSubmod.contains(submodPair)) continue;
                    generatedSubmod.add(submodPair);

                    Set<String> union = new HashSet<>(set1);
                    union.addAll(set2);
                    if (!Util.isConnected(union)) continue;
                    String idUnion = getId(union);

                    MPVariable hUnion = solver.lookupVariableOrNull(idUnion);
                    if (hUnion == null) {
                        hUnion = solver.makeNumVar(0.0, INFINITY, idUnion);
                    }

                    Set<String> intersection = new HashSet<>(set1);
                    intersection.retainAll(set2);
                    if (!Util.isConnected(intersection)) continue;
                    String idIntersection = getId(intersection);

                    if (!idIntersection.equals("")) {
                        MPVariable hIntersection = solver.lookupVariableOrNull(idIntersection);
                        if (hIntersection == null) {
                            hIntersection = solver.makeNumVar(0.0, INFINITY, idIntersection);
                        }

                        MPConstraint ct = solver.makeConstraint(
                            NEG_INFINITY, 0.0, getSubmodCTName(id1, id2, idUnion, idIntersection));
                        ct.setCoefficient(hUnion, 1);
                        ct.setCoefficient(hIntersection, 1);
                        ct.setCoefficient(h1, -1);
                        ct.setCoefficient(h2, -1);
                    } else {
                        MPConstraint ct = solver.makeConstraint(
                            NEG_INFINITY, 0.0, getSubmodCTName(id1, id2, idUnion, idIntersection));
                        ct.setCoefficient(hUnion, 1);
                        ct.setCoefficient(h1, -1);
                        ct.setCoefficient(h2, -1);
                    }
                }
            }
        }

        String objId = getId(Util.toVListSet(vListString));
        MPVariable obj = solver.lookupVariableOrNull(objId);
        if (obj == null) {
            System.out.println("ERROR: cannot find objective variable");
        }

        // create objective function
        MPObjective objective = solver.objective();
        objective.setCoefficient(obj, 1);
        objective.setMaximization();

        return solver;
    }

    protected void addCardinalityConstraints(
        MPSolver solver, String vListString, String labelSeqString) {

        for (String subVList : catalogue.keySet()) {
            if (!Util.isSubquery(subVList, vListString)) continue;
            String subLabelSeq = Util.extractLabelSeq(subVList, vListString, labelSeqString);
            double logCardinality = Math.log(catalogue.get(subVList).get(subLabelSeq));

            Set<String> subVListSet = Util.toVListSet(subVList);
            String id = getId(subVListSet);
            MPVariable variable = solver.lookupVariableOrNull(id);
            if (variable == null) {
                System.out.println("ERROR: " + subVListSet + " cannot be found");
            }
            MPConstraint ct = solver.makeConstraint(
                NEG_INFINITY, logCardinality, getCardEquCTName(id, logCardinality));
            ct.setCoefficient(variable, 1);
        }
    }

    protected void addTighterMaxDeg(MPSolver solver, String vListString, String labelSeqString) {
        for (String baseVList : catalogueMaxDeg.keySet()) {
            String baseLabelSeq = Util.extractLabelSeq(baseVList, vListString, labelSeqString);
            Set<String> baseVListSet = Util.toVListSet(baseVList);
            String baseId = getId(baseVListSet);
            MPVariable baseVar = solver.lookupVariableOrNull(baseId);
            if (baseVar == null) {
                System.out.println("ERROR: " + baseVListSet + " cannot be found");
            }

            Map<String, Map<String, Integer>> ext2maxDeg = catalogueMaxDeg.get(baseVList).get(baseLabelSeq);
            for (String extVList : ext2maxDeg.keySet()) {
                String extLabel = Util.extractLabelSeq(extVList, vListString, labelSeqString);
                Set<String> extendedVListSet = Util.toVListSet(baseVList + ";" + extVList);
                String extendedId = getId(extendedVListSet);
                MPVariable extendedVar = solver.lookupVariableOrNull(extendedId);
                if (extendedVar == null) {
                    System.out.println("ERROR: " + extendedVListSet + " cannot be found");
                }

                double logMaxDeg = Math.log(ext2maxDeg.get(extVList).get(extLabel));

                MPConstraint ct = solver.makeConstraint(
                    NEG_INFINITY, logMaxDeg, "i" + getMaxDegCTName(baseId, extendedId, logMaxDeg));
                ct.setCoefficient(baseVar, -1);
                ct.setCoefficient(extendedVar, 1);
            }
        }
    }

    protected void injectVarValue(MPSolver solver) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader("data/injection.csv"));
        String line = reader.readLine();
        while (line != null) {
            String[] vListAndContribution = line.split(",");
            double logCardinality = Math.log(Integer.parseInt(vListAndContribution[1]));
            String id = getId(Util.toVListSet(vListAndContribution[0]));
            MPVariable variable = solver.lookupVariableOrNull(id);
            variable.setBounds(logCardinality, logCardinality);
            System.out.println(id + ": " + Math.exp(logCardinality));
            MPSolver.ResultStatus status = solver.solve();
            if (status != ResultStatus.OPTIMAL) break;

            line = reader.readLine();
        }
        reader.close();
    }

    protected double solveLP(MPSolver solver) {
        MPSolver.ResultStatus status = solver.solve();
        if (status != ResultStatus.OPTIMAL) {
            System.out.println("ERROR: not optimal - " + status);
        }
        solver.verifySolution(Math.pow(10, -6), true);
        return solver.objective().value();
    }

    public double estimate(Query query, boolean subMod) {
        Pair<String, String> vListAndLabelSeq = Util.topologyToVListAndLabelSeq(query.topology);
        MPSolver solver = createBasicLP(vListAndLabelSeq.key, vListAndLabelSeq.value, subMod);
        addCardinalityConstraints(solver, vListAndLabelSeq.key, vListAndLabelSeq.value);
//        addTighterMaxDeg(solver, vListAndLabelSeq.key, vListAndLabelSeq.value);
        if (isZero) return 0;
        return Math.exp(solveLP(solver));
    }

    public CLLP(
        String maxDegFile,
        String labelCountFile,
        String catalogueFile,
        int catMaxLen,
        String catMaxDegFile) throws Exception {

        readMaxDeg(maxDegFile);
        readLabelCount(labelCountFile);
        if (catalogueFile != null) {
            readCatalogue(catalogueFile, catMaxLen);
        }
        if (catMaxDegFile != null) {
            readCatalogueMaxDeg(catMaxDegFile);
        }
    }

    public CLLP(
        Map<Integer, Long> maxOutDeg,
        Map<Integer, Long> maxInDeg,
        Map<Integer, Long> labelCount,
        Map<String, Map<String, Long>> catalogue,
        Map<String, Map<String, Map<String, Map<String, Integer>>>> catalogueMaxDeg) {

        this.maxOutDeg = maxOutDeg;
        this.maxInDeg = maxInDeg;
        this.labelCount = labelCount;
        this.catalogue = catalogue;
        this.catalogueMaxDeg = catalogueMaxDeg;
    }
}
