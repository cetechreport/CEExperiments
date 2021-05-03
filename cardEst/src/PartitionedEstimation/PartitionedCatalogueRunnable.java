package PartitionedEstimation;

import Common.Query;

public class PartitionedCatalogueRunnable implements Runnable {
    private int threadId;
    private PartitionedCatalogue estimator;
    private Query query;
    private Integer patternType;
    private int formulaType;
    private int catLen;

    private Double[] estimates;

    public void run() {
        try {
            estimates = estimator.estimate(query, patternType, formulaType, catLen);
        } catch (Exception e) {
            return;
        }
    }

    public Double[] getEstimates() {
        return estimates;
    }

    public PartitionedCatalogueRunnable(
        int threadId,
        PartitionedCatalogue estimator,
        Query query,
        Integer patternType,
        int formulaType,
        int catLen) {

        this.threadId = threadId;
        this.estimator = estimator;
        this.query = query;
        this.patternType = patternType;
        this.formulaType = formulaType;
        this.catLen = catLen;
    }
}
