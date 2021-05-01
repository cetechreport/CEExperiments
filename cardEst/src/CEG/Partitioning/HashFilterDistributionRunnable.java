package CEG.Partitioning;

import Common.Triple;

public class HashFilterDistributionRunnable implements Runnable {
    volatile HashFilterDistribution distribution;
    volatile String queryScheme;
    String entryBudget;
    volatile Triple<String, String, Long> entry;

    public HashFilterDistribution getDistribution() {
        return distribution;
    }

    public String getQueryScheme() {
        return queryScheme;
    }

    public Triple<String, String, Long> getEntry() {
        return entry;
    }

    public void run() {
        if (entry.v1.split(";").length > 1) {
            distribution.compute2(entry.v1, entry.v2, entryBudget);
        } else {
            distribution.compute1(entry.v1, entry.v2, entryBudget);
        }
    }

    public HashFilterDistributionRunnable(
        HashFilterDistribution distribution,
        String queryScheme,
        String entryBudget,
        Triple<String, String, Long> entry) {
        this.distribution = distribution;
        this.queryScheme = queryScheme;
        this.entryBudget = entryBudget;
        this.entry = entry;
    }
}
