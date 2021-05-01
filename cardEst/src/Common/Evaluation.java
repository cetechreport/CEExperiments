package Common;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Evaluation {
    protected static double computeRelativeError(double estimation, double actual) {
        return Math.abs(estimation - actual) / actual;
    }

    public static double computeQError(double estimation, double actual) {
        return Math.max(estimation / actual, actual / estimation);
    }

    protected static int numInversions(List<Pair<Double, Double>> estActual) {
        Collections.sort(estActual, new Comparator<Pair<Double, Double>>() {
            public int compare(Pair<Double, Double> pair1, Pair<Double, Double> pair2) {
                return pair1.getKey().compareTo(pair2.getKey());
            }
        });

        int invCount = 0;
        for (int i = 0; i < estActual.size() - 1; i++) {
            for (int j = i + 1; j < estActual.size(); j++) {
                if (estActual.get(i).getValue() > estActual.get(j).getValue()) {
                    invCount++;
                }
            }
        }

        return invCount;
    }
}
