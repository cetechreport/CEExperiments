package EdgeLabelHistogram;

public class HistogramSaver {
    public static void main(String[] args) throws Exception {
        Histogram histogram = new Histogram(args[0]);
        histogram.save(args[1], args[2]);
    }
}
