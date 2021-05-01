package RDF3X;

public class RdfTriple {
    public int first;
    public int second;
    public int third;

    public RdfTriple(int first, int second, int third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public String toString() {
        return "(" + first + "," + second + "," + third + ")";
    }
}
