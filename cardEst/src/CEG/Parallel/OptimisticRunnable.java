package CEG.Parallel;

import CEG.Optimistic;
import CEG.Optimistic.Mode;
import Common.Query;

public class OptimisticRunnable implements Runnable {
    private Optimistic estimator;
    private volatile Query query;
    private int budget;
    private Mode mode;

    private volatile Double[] results;

    public Double[] getResults() {
        return results;
    }

    public Query getQuery() {
        return query;
    }

    public void run() {
        results = estimator.estimate(query, budget, mode);
    }

    public OptimisticRunnable(Optimistic estimator, Query query, int budget, Mode mode) {
        this.estimator = estimator;
        this.query = query;
        this.budget = budget;
        this.mode = mode;
    }
}
