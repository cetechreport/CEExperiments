package RDF3X;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Histograms {
    public Map<HistogramType, Histogram> type2hist;
    private String[] dataFilePaths;

    public Histograms(String[] dataFilePaths) throws Exception {
        this.dataFilePaths = dataFilePaths;

        type2hist = new HashMap<>();

        System.out.println("--SP histogram");
        type2hist.put(HistogramType.SP, new Histogram(dataFilePaths[0], HistogramType.SP));
        System.out.println("--SO histogram");
        type2hist.put(HistogramType.SO, new Histogram(dataFilePaths[1], HistogramType.SO));
        System.out.println("--PS histogram");
        type2hist.put(HistogramType.PS, new Histogram(dataFilePaths[2], HistogramType.PS));
        System.out.println("--PO histogram");
        type2hist.put(HistogramType.PO, new Histogram(dataFilePaths[3], HistogramType.PO));
        System.out.println("--OS histogram");
        type2hist.put(HistogramType.OS, new Histogram(dataFilePaths[4], HistogramType.OS));
        System.out.println("--OP histogram");
        type2hist.put(HistogramType.OP, new Histogram(dataFilePaths[5], HistogramType.OP));
    }

    // triplePattern needs to have subject as first field and so on
    public double estimateCardinality(RdfTriple triplePattern) {
        boolean boundSub = triplePattern.first > 0;
        boolean boundPred = triplePattern.second > 0;
        boolean boundObj = triplePattern.third > 0;

        List<HistogramBucket> buckets;
        int numBoundFields;

        if (boundSub && boundPred) {
            buckets = type2hist.get(HistogramType.SP).buckets;
            numBoundFields = 2;
        } else if (boundPred && boundObj) {
            buckets = type2hist.get(HistogramType.PO).buckets;
            numBoundFields = 2;
        } else if (boundSub && boundObj) {
            buckets = type2hist.get(HistogramType.SO).buckets;
            numBoundFields = 2;
        } else if (boundSub) {
            buckets = type2hist.get(HistogramType.SP).buckets;
            numBoundFields = 1;
        } else if (boundPred) {
            buckets = type2hist.get(HistogramType.PO).buckets;
            numBoundFields = 1;
        } else if (boundObj) {
            buckets = type2hist.get(HistogramType.OS).buckets;
            numBoundFields = 1;
        } else {
            return 1.0;
        }

        int total = 0;
        for (HistogramBucket bucket : buckets) {
            if (bucket.within(triplePattern)) {
                if (2 == numBoundFields) {
                    return ((float) bucket.numTriples) / bucket.numDistinctTwoPrefix /
                        type2hist.get(HistogramType.SP).numTriples;
                } else { // numBoundFields == 1
                    total += bucket.numTriples;
                }
            }
        }

        return ((float) total) / type2hist.get(HistogramType.SP).numTriples;
    }

    // assumes
    // (1) joinedTriple1 is object-subject joined with joinedTriple2
    // (2) predicates are bound
    public double estimateCardinality(RdfTriple joinedTriple1, RdfTriple joinedTriple2) {
        RdfTriple triple = new RdfTriple(joinedTriple1.second, joinedTriple1.first,
            joinedTriple1.third);

        int total = 0;
        for (HistogramBucket bucket : type2hist.get(HistogramType.PS).buckets) {
            if (bucket.within(triple)) {
                total += bucket.thirdEqSec;
            }
        }
        return ((float) total) / type2hist.get(HistogramType.PS).numTriples;
    }
}
