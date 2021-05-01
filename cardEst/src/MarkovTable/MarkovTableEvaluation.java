package MarkovTable;

import Common.Evaluation;
import Common.Pair;
import Common.Path;
import Common.TrueSimplePathCardinality;

import java.util.*;

public class MarkovTableEvaluation extends Evaluation {
    // path -> (actual, estimation)
    private static Map<Path, Pair<Double, Double>> existingPaths2actAndEst;
    private static int tableMaxLength;

    private static void allSameEdgeType(MarkovTable markovTable, TrueSimplePathCardinality
        trueSimplePathCardinality) {
        System.out.println("allSameEdgeType");

        List<Integer> edgeLabelList;
        Path path;
        double est, actual;
        double re, qe;
        double totalRE = 0, totalQE = 0;
        int numValidExp = 0;
        for (Integer edgeLabel : trueSimplePathCardinality.pred2objs.keySet()) {
            edgeLabelList = new ArrayList<>();
            edgeLabelList.add(edgeLabel);
            edgeLabelList.add(edgeLabel);
            edgeLabelList.add(edgeLabel);
            path = new Path(edgeLabelList);

            est = Math.ceil(markovTable.estimate(path));
            actual = trueSimplePathCardinality.compute(path);

            if (0 != actual) {
                re = computeRelativeError(est, actual);
                qe = computeQError(est, actual);
                System.out.println(edgeLabel + " (RE): " + actual + ", " + est + ", " + re);
                System.out.println(edgeLabel + " (QE): " + actual + ", " + est + ", " + qe);
                totalRE += re;
                totalQE += qe;
                numValidExp++;

                existingPaths2actAndEst.put(path, new Pair<>(actual, est));
            }
        }

        System.out.println("MRE: " + (totalRE / numValidExp));
        System.out.println("MQE: " + (totalQE / numValidExp));
    }

    private static void diffEdgeTypes(MarkovTable markovTable, TrueSimplePathCardinality
        trueSimplePathCardinality) {
        System.out.println("diffEdgeTypes");

        List<Integer> edgeLabelList;
        Path path;
        double est, actual;
        double re, qe;
        double totalRE = 0, totalQE = 0;
        int numValidExp = 0;
        for (Integer edgeLabel1 : trueSimplePathCardinality.pred2objs.keySet()) {
            for (Integer edgeLabel2 : trueSimplePathCardinality.pred2objs.keySet()) {
                if (edgeLabel1.equals(edgeLabel2)) continue;
                for (Integer edgeLabel3 : trueSimplePathCardinality.pred2objs.keySet()) {
                    if (edgeLabel3.equals(edgeLabel1) || edgeLabel3.equals(edgeLabel2)) continue;
                    edgeLabelList = new ArrayList<>();
                    edgeLabelList.add(edgeLabel1);
                    edgeLabelList.add(edgeLabel2);
                    edgeLabelList.add(edgeLabel3);
                    path = new Path(edgeLabelList);

                    est = Math.ceil(markovTable.estimate(path));
                    actual = trueSimplePathCardinality.compute(path);

                    if (0 != actual) {
                        re = computeRelativeError(est, actual);
                        qe = computeQError(est, actual);
                        System.out.println(edgeLabel1 + "->" + edgeLabel2 + "->" + edgeLabel3 +
                            " (RE): " + actual + "," + " " + est + ", " + re);
                        System.out.println(edgeLabel1 + "->" + edgeLabel2 + "->" + edgeLabel3 +
                            " (QE): " + actual + "," + " " + est + ", " + qe);
                        totalRE += re;
                        totalQE += qe;
                        numValidExp++;

                        existingPaths2actAndEst.put(path, new Pair<>(actual, est));
                    }
                }
            }
        }

        System.out.println("MRE: " + (totalRE / numValidExp));
        System.out.println("MQE: " + (totalQE / numValidExp));
    }

    private static void longPath(MarkovTable markovTable, TrueSimplePathCardinality trueSimplePathCardinality,
        int pathLen) {
        System.out.println("longPath: " + pathLen);

        List<Integer> edgeLabelList;
        Path path;
        double est, actual;
        double re, qe;
        double totalRE = 0, totalQE = 0;
        int numValidExp = 0;

        Random random = new Random(0);
        int TOTAL_NUM_EXP = 1000;
        int MAX_NUM_EXP = (int) Math.pow(10, 5);

        int i = 0;
        while (numValidExp < TOTAL_NUM_EXP && i < MAX_NUM_EXP) {
            StringBuilder stringBuilder = new StringBuilder();
            edgeLabelList = new ArrayList<>();

            for (int j = 0; j < pathLen; ++j) {
                edgeLabelList.add(random.nextInt(trueSimplePathCardinality.pred2objs.size()) + 1);

                stringBuilder.append(edgeLabelList.get(j));
                if (j < pathLen - 1) {
                    stringBuilder.append("->");
                }
            }

            path = new Path(edgeLabelList);
            est = Math.ceil(markovTable.estimate(path));
            actual = trueSimplePathCardinality.compute(path);

            if (0 != actual) {
                re = computeRelativeError(est, actual);
                qe = computeQError(est, actual);
                System.out.println(stringBuilder.toString() + " (RE): " + actual + ", " + est + ", " + re);
                System.out.println(stringBuilder.toString() + " (QE): " + actual + ", " + est + ", " + qe);
                totalRE += re;
                totalQE += qe;
                numValidExp++;

                existingPaths2actAndEst.put(path, new Pair<>(actual, est));
            }

            i++;
        }

        System.out.println("MRE: " + (totalRE / numValidExp));
        System.out.println("MQE: " + (totalQE / numValidExp));
    }

    public static void boundVertex(MarkovTable markovTable, TrueSimplePathCardinality trueSimplePathCardinality) {
        System.out.println("Bound Vertex");

        Path[] existingPaths =
            existingPaths2actAndEst.keySet().toArray(new Path[existingPaths2actAndEst.size()]);

        Random random = new Random(0);
        int TOTAL_NUM_EXP = 1000;

        int i = 0;
        double est, actual;
        double re, qe;
        double totalRE = 0, totalQE = 0;
        int numValidExp = 0;
        Integer[] firstVertices;
        Path path;
        List<Integer> srcVertexList;
        int MAX_NUM_EXP = (int) Math.pow(10, 5);
        while (numValidExp < TOTAL_NUM_EXP && i < MAX_NUM_EXP) {
            path = new Path(existingPaths[random.nextInt(existingPaths.length)]);
            srcVertexList = path.getSrcVertexList();
            firstVertices = markovTable.path2src2occ.get(path.getPrefix(tableMaxLength)).keySet().toArray(
                    new Integer[markovTable.path2src2occ.get(path.getPrefix(tableMaxLength)).size()]
                );
            srcVertexList.set(0, firstVertices[random.nextInt(firstVertices.length)]);
            path.setSrcVertexList(srcVertexList);

            est = Math.ceil(markovTable.estimate(path));
            actual = trueSimplePathCardinality.compute(path);

            if (0 != actual) {
                re = computeRelativeError(est, actual);
                qe = computeQError(est, actual);
                System.out.println(path.toString() + " (RE): " + actual + ", " + est + ", " + re);
                System.out.println(path.toString() + " (QE): " + actual + ", " + est + ", " + qe);
                totalRE += re;
                totalQE += qe;
                numValidExp++;

                existingPaths2actAndEst.put(path, new Pair<>(actual, est));
            }

            i++;
        }

        System.out.println("MRE: " + (totalRE / numValidExp));
        System.out.println("MQE: " + (totalQE / numValidExp));
    }

    private static void inversionEval() {
        System.out.println("Inversion Count Evaluation");

        Path[] existingPaths =
            existingPaths2actAndEst.keySet().toArray(new Path[existingPaths2actAndEst.size()]);

        Random random = new Random(0);
        int TOTAL_NUM_EXP = 1000;

        int i = 0;
        List<Pair<Double, Double>> actAndEstList;
        Pair<Double, Double> actEst;
        Path path;
        while (i < TOTAL_NUM_EXP) {
            actAndEstList = new ArrayList<>();

            for (int j = 0; j < 10; ++j) {
                path = existingPaths[random.nextInt(existingPaths.length)];
                System.out.println(path.toString());
                actEst = existingPaths2actAndEst.get(path);
                actAndEstList.add(actEst);
            }

            System.out.println("#inv: " + numInversions(actAndEstList));

            i++;
        }
    }

    public static void main(String[] args) throws Exception {
        existingPaths2actAndEst = new HashMap<>();
        tableMaxLength = Integer.parseInt(args[1]);

        System.out.println("Building Markov table...: " + args[1]);
        MarkovTable markovTable = new MarkovTable(args[0], tableMaxLength, false, false);
        System.out.println("Building stats for actual cardinalities...");
        TrueSimplePathCardinality trueSimplePathCardinality = new TrueSimplePathCardinality(args[0]);

        allSameEdgeType(markovTable, trueSimplePathCardinality);
        diffEdgeTypes(markovTable, trueSimplePathCardinality);
//        longPath(markovTable, trueSimplePathCardinality, 6);
//        boundVertex(markovTable, trueSimplePathCardinality);

//        inversionEval();
    }
}
