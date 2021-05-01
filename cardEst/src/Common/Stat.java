package Common;

public class Stat {
    public long cardinality;

    public int numUniqueLivePrefixDests;
    public int numUniqueDests;
    public double avgDestOutDeg;
    public double avgDestInDeg;
    public int numUniquePrefixDests;

    public long maxExc0;
    public long minExc0;
    public double meanExc0;
    public double sdExc0;
    public double cvExc0;

    public long maxInc0;
    public long minInc0;
    public double meanInc0;
    public double sdInc0;
    public double cvInc0;

    public String toCsv() {
        return cardinality + "," + numUniqueDests + "," + avgDestInDeg + "," + avgDestOutDeg + "," +
            numUniquePrefixDests + "," + numUniqueLivePrefixDests + "," + maxExc0 + "," + minExc0 +
            "," + meanExc0 + "," + sdExc0 + "," + cvExc0 + "," + maxInc0 + "," + minInc0 + "," +
            meanInc0 + "," + sdInc0 + "," + cvInc0;
    }
}
