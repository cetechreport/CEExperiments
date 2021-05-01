package Pessimistic;

import Common.Pair;
import Common.Query;
import Common.Util;
import com.google.ortools.linearsolver.MPConstraint;
import com.google.ortools.linearsolver.MPObjective;
import com.google.ortools.linearsolver.MPSolver;
import com.google.ortools.linearsolver.MPVariable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class CyclicCLLP extends CLLP {
    static { System.loadLibrary("jniortools");}

    protected MPSolver createBasicLP(String vListString, String labelSeqString, boolean subMod, boolean triangle) {
        MPSolver solver = new MPSolver("CLLP", MPSolver.OptimizationProblemType.GLOP_LINEAR_PROGRAMMING);
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

                if (triangle && set2.containsAll(set1) && ext.size() == 2) {
                    if (isTriangleExtension(set1, ext)) {
                        double logMaxDegOrCard = getLogTriMaxDeg(set1, set2, vListString, labelSeqString);
                        MPConstraint ct1 = solver.makeConstraint(
                            NEG_INFINITY, logMaxDegOrCard, getMaxDegCTName(id1, id2, logMaxDegOrCard));
                        ct1.setCoefficient(h1, -1);
                        ct1.setCoefficient(h2, 1);
                    }
                // if set2 is one edge extended from set1
                } else if (set2.containsAll(set1) && ext.size() == 1) {
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

    protected boolean isTriangleExtension(Set<String> base, Set<String> ext) {
        if (!Util.isConnected(ext)) return false;

        Integer[] extVList = new Integer[4];
        int i = 0;
        for (String extEdge : ext) {
            String[] vStrings = extEdge.split("-");
            extVList[i] = Integer.parseInt(vStrings[0]);
            extVList[i + 1] = Integer.parseInt(vStrings[1]);
            i += 2;
        }
        Set<Integer> leaves = Util.getLeaves(extVList);

        Integer[] triangleEdge = new Integer[2];
        int j = 0;
        for (Integer leaf : leaves) {
            triangleEdge[j] = leaf;
            ++j;
        }
        Arrays.sort(triangleEdge);
        return base.contains(Util.toVListString(triangleEdge));
    }

    protected double getLogTriMaxDeg(Set<String> set1, Set<String> set2, String vListString, String labelSeqString) {
        Set<String> extSet = new HashSet<>(set2);
        extSet.removeAll(set1);
        StringJoiner extVListString = new StringJoiner(";");
        for (String ext : extSet) {
            extVListString.add(ext);
        }
        String sortedExtVList = Util.sort(extVListString.toString());
        Set<Integer> leaves = Util.getLeaves(Util.toVList(sortedExtVList));
        Integer src = -1, dest = -1;
        for (Integer leaf : leaves) {
            if (src.equals(-1)) {
                src = leaf;
            } else if (leaf < src) {
                dest = src;
                src = leaf;
            } else {
                dest = leaf;
            }
        }

        String baseVList = src + "-" + dest;
        String baseLabelSeq = Util.extractLabelSeq(baseVList, vListString, labelSeqString);
        String extLabelSeq = Util.extractLabelSeq(sortedExtVList, vListString, labelSeqString);
        return Math.log(catalogueMaxDeg.get(baseVList).get(baseLabelSeq).get(sortedExtVList).get(extLabelSeq));
    }

    @Override
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

    protected void addCardinalityConstraints(
        MPSolver solver, String vListString, String labelSeqString, boolean triangle) {

        for (String subVList : catalogue.keySet()) {
            if (!triangle && subVList.split(";").length > 2) continue;

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

    public double estimate(Query query, boolean subMod, boolean triangle) {
        Pair<String, String> vListAndLabelSeq = Util.topologyToVListAndLabelSeq(query.topology);
        MPSolver solver = createBasicLP(vListAndLabelSeq.key, vListAndLabelSeq.value, subMod, triangle);
        addCardinalityConstraints(solver, vListAndLabelSeq.key, vListAndLabelSeq.value, triangle);
        if (isZero) return 0;
//        for (MPConstraint ct : solver.constraints()) {
//            System.out.println(ct.name());
//        }
        return Math.exp(solveLP(solver));
    }

    public CyclicCLLP(
        Map<Integer, Long> maxOutDeg,
        Map<Integer, Long> maxInDeg,
        Map<Integer, Long> labelCount,
        Map<String, Map<String, Long>> catalogue,
        Map<String, Map<String, Map<String, Map<String, Integer>>>> catalogueMaxDeg) {

        super(maxOutDeg, maxInDeg, labelCount, catalogue, catalogueMaxDeg);
    }
}
