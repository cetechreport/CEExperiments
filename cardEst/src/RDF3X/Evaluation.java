package RDF3X;

import Common.TrueSimplePathCardinality;

import java.util.Random;

public class Evaluation {

    private static int TOTAL_NUM_DISTINCT_PREDICATES = 64;
    private static int TOTAL_NUM_DISTINCT_SUBJECTS = 647215;

    /*
     * (1) path with all same edge types: (?a, 1, ?b), (?b, 1, ?c), (?c, 1, ?d)
     * (2) path with different edge types: (?a, 1, ?b), (?b, 4, ?c), (?c, 5, ?d)
     * (3) path with specified starting node: (1977, 1, ?b), (?b, 4, ?c), (?c, 5, ?d)
     * (4) long path:
     *     (?a, 1, ?b), (?b, 4, ?c), (?c, 5, ?d), (?d, 25, ?e), (?e, 56, ?f), (?f, 40, ?g),
     *     (?g, 64, ?h), (?h, 62, ?i), (?i, 27, ?j), (?j, 32, ?k)
     */

    private static double computeRelativeError(double estimation, double actual) {
        return Math.abs(estimation - actual) / actual;
    }

    private static void allSameEdgeType(Estimator estimator, TrueSimplePathCardinality
        trueSimplePathCardinality) {
        System.out.println("allSameEdgeType");
        RdfTriple[] path = {
            new RdfTriple(-1, 0, -1),
            new RdfTriple(-1, 0, -1),
            new RdfTriple(-1, 0, -1),
        };

        double est;
        double actual;
        double re;
        double totalRE = 0;
        int numValidExp = 0;
        for (int i = 1; i <= TOTAL_NUM_DISTINCT_PREDICATES; ++i) {
            path[0].second = i;
            path[1].second = i;
            path[2].second = i;
            est = estimator.estimateUsingJoinStats(path);
            actual = trueSimplePathCardinality.compute(path);

            if (0 != actual) {
                re = computeRelativeError(est, actual);
                System.out.println(i + ": " + actual + ", " + est + ", " + re);
                totalRE += re;
                numValidExp++;
            }
        }

        System.out.println("MRE: " + (totalRE / numValidExp));
    }

    private static void diffEdgeTypes(Estimator estimator, TrueSimplePathCardinality trueSimplePathCardinality) {
        System.out.println("diffEdgeTypes");
        RdfTriple[] path = {
            new RdfTriple(-1, 0, -1),
            new RdfTriple(-1, 0, -1),
            new RdfTriple(-1, 0, -1),
        };

        double est;
        double actual;
        double re;
        double totalRE = 0;
        int numValidExp = 0;
        for (int i = 1; i <= TOTAL_NUM_DISTINCT_PREDICATES; ++i) {
            for (int j = 1; j <= TOTAL_NUM_DISTINCT_PREDICATES; ++j) {
                if (j == i) continue;
                for (int k = 1; k <= TOTAL_NUM_DISTINCT_PREDICATES; ++k) {
                    if (k == j || k == i) continue;

                    path[0].second = i;
                    path[1].second = j;
                    path[2].second = k;
                    est = estimator.estimateUsingJoinStats(path);
                    actual = trueSimplePathCardinality.compute(path);

                    if (0 != actual) {
                        re = computeRelativeError(est, actual);
                        System.out.println(i + "->" + j + "->" + k + ": " + actual + ", " + est +
                            ", " + re);
                        totalRE += re;
                        numValidExp++;
                    }
                }
            }
        }

        System.out.println("MRE: " + (totalRE / numValidExp));
    }

    private static void specifiedStartingNode(Estimator estimator, TrueSimplePathCardinality
        trueSimplePathCardinality) {
        System.out.println("specifiedStartingNode");
        RdfTriple[] path = {
            new RdfTriple(-1, 0, -1),
            new RdfTriple(-1, 0, -1),
            new RdfTriple(-1, 0, -1),
        };

        double est;
        double actual;
        double re;
        double totalRE = 0;
        int numValidExp = 0;
        for (int s = 1; s <= TOTAL_NUM_DISTINCT_SUBJECTS; ++s) {
            for (int i = 1; i <= TOTAL_NUM_DISTINCT_PREDICATES; ++i) {
                path[0].first = s;
                path[0].second = i;
                if (!trueSimplePathCardinality.doesTripleExist(path[0])) continue;

                for (int j = 1; j <= TOTAL_NUM_DISTINCT_PREDICATES; ++j) {
                    if (j == i) continue;
                    for (int k = 1; k <= TOTAL_NUM_DISTINCT_PREDICATES; ++k) {
                        if (k == j || k == i) continue;

                        path[1].second = j;
                        path[2].second = k;

                        est = estimator.estimateUsingJoinStats(path);
                        actual = trueSimplePathCardinality.compute(path);

                        if (0 != actual) {
                            re = computeRelativeError(est, actual);
                            System.out.println("(" + s + ") " + i + "->" + j + "->" + k + ": " +
                                actual + ", " + est + ", " + re);
                            totalRE += re;
                            numValidExp++;
                        }
                    }
                }
            }
        }

        System.out.println("MRE: " + (totalRE / numValidExp));
    }

    private static void longPath(Estimator estimator, TrueSimplePathCardinality trueSimplePathCardinality) {
        System.out.println("longPath");
        RdfTriple[] path = {
            new RdfTriple(-1, 0, -1),
            new RdfTriple(-1, 0, -1),
            new RdfTriple(-1, 0, -1),
            new RdfTriple(-1, 0, -1),
            new RdfTriple(-1, 0, -1),
            new RdfTriple(-1, 0, -1),
            new RdfTriple(-1, 0, -1),
            new RdfTriple(-1, 0, -1),
            new RdfTriple(-1, 0, -1),
            new RdfTriple(-1, 0, -1),
        };

        double est;
        double actual;
        double re;
        double totalRE = 0;
        int numValidExp = 0;

        Random random = new Random(0);
        int TOTAL_NUM_EXP = 1000;

        for (int i = 0; i < TOTAL_NUM_EXP; ++i) {
            StringBuilder stringBuilder = new StringBuilder();

            for (int j = 0; j < path.length; ++j) {
                path[j].second = random.nextInt(64) + 1;

                stringBuilder.append(path[j].second);
                if (j < path.length - 1) {
                    stringBuilder.append("->");
                }
            }
            est = estimator.estimateUsingJoinStats(path);
            // TODO: figure out how to make this into selectivity
            actual = trueSimplePathCardinality.compute(path);

            if (0 != actual) {
                re = computeRelativeError(est, actual);
                System.out.println(stringBuilder.toString() + ": " + actual + ", " + est + ", " +
                    re);
                totalRE += re;
                numValidExp++;
            }
        }

        System.out.println("MRE: " + (totalRE / numValidExp));
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Building histograms...");
        Estimator estimator = new Estimator(args);
        System.out.println("Building stats for actual cardinalities...");
        TrueSimplePathCardinality trueSimplePathCardinality = new TrueSimplePathCardinality(args[0]);

        allSameEdgeType(estimator, trueSimplePathCardinality);
        diffEdgeTypes(estimator, trueSimplePathCardinality);
        specifiedStartingNode(estimator, trueSimplePathCardinality);
        longPath(estimator, trueSimplePathCardinality);
    }
}
