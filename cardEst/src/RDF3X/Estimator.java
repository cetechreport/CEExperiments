package RDF3X;

public class Estimator {

    private Histograms histograms;

    public Estimator(String[] dataFilePaths) throws Exception {
        this.histograms = new Histograms(dataFilePaths);
    }

    public double estimateUsingJoinStats(RdfTriple[] path) {
        double est = 1;
        for (int i = 0; i < path.length - 1; ++i) {
            est *= histograms.estimateCardinality(path[i], path[i+1]);
        }

        return est;
    }
}
