package CEG.Parallel;

import CEG.Optimistic;
import CEG.Pessimistic;
import Common.Query;

import java.math.BigInteger;

public class PessimisticRunnable implements Runnable {
    private Pessimistic estimator;
    private volatile Query query;
    private int budget;

    private volatile BigInteger results;

    public BigInteger getResults() {
        return results;
    }

    public Query getQuery() {
        return query;
    }

    public void run() {
        results = estimator.estimate(query, budget);
    }

    public PessimisticRunnable(Pessimistic estimator, Query query, int budget) {
        this.estimator = estimator;
        this.query = query;
        this.budget = budget;
    }
}
