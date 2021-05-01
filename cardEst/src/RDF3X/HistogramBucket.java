package RDF3X;

import java.io.BufferedReader;
import java.io.BufferedWriter;

public class HistogramBucket {

    public RdfTriple startingTriple;
    public RdfTriple endingTriple;

    public int numTriples;
    public int numDistinctTwoPrefix;
    public int numDistinctOnePrefix;

    // join partners on subject

    // s = s
    public int firstEqFirst;
    // s = p
    public int firstEqSec;
    // s = o
    public int firstEqThird;

    // join partners on predicate

    // p = s
    public int secEqFirst;
    // p = p
    public int secEqSec;
    // p = o
    public int secEqThird;

    // join partners on object

    // o = s
    public int thirdEqFirst;
    // o = p
    public int thirdEqSec;
    // o = o
    public int thirdEqThird;

    public boolean within(RdfTriple triple) {
        return (triple.first > this.startingTriple.first &&
                triple.first < this.endingTriple.first) ||
               ((triple.first == this.startingTriple.first ||
                triple.first == this.endingTriple.first) && triple.second == -1) ||
               (triple.first == this.startingTriple.first &&
                triple.first == this.endingTriple.first &&
                triple.second >= this.startingTriple.second &&
                triple.second <= this.endingTriple.second) ||
               (triple.first == this.startingTriple.first &&
                triple.second >= this.startingTriple.second) ||
               (triple.first == this.endingTriple.first &&
                triple.second <= this.endingTriple.second);
    }

    public void save(BufferedWriter writer) throws Exception {
        StringBuilder histogramData = new StringBuilder();
        histogramData.append("----------");
        histogramData.append("\n");
        histogramData.append("starting: ");
        histogramData.append(startingTriple.toString());
        histogramData.append("\n");
        histogramData.append("ending: ");
        histogramData.append(endingTriple.toString());
        histogramData.append("\n");
        histogramData.append("numTriples: ");
        histogramData.append(numTriples);
        histogramData.append("\n");
        histogramData.append("numDistinctOnePrefix: ");
        histogramData.append(numDistinctOnePrefix);
        histogramData.append("\n");
        histogramData.append("numDistinctTwoPrefix: ");
        histogramData.append(numDistinctTwoPrefix);
        histogramData.append("\n");
        histogramData.append("first=first: ");
        histogramData.append(firstEqFirst);
        histogramData.append("\n");
        histogramData.append("first=third: ");
        histogramData.append(firstEqThird);
        histogramData.append("\n");
        histogramData.append("third=first: ");
        histogramData.append(thirdEqFirst);
        histogramData.append("\n");
        histogramData.append("third=third: ");
        histogramData.append(thirdEqThird);
        histogramData.append("\n");
        writer.write(histogramData.toString());
    }
}
