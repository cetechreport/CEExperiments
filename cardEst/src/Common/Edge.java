package Common;

public class Edge {
    int src;
    int dest;
    int label;

    public Edge(int src, int label, int dest) {
        this.src = src;
        this.dest = dest;
        this.label = label;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Edge e = (Edge) o;

        return src == e.src && dest == e.dest && label == e.label;
    }

    @Override
    public int hashCode() {
        return src + dest + label;
    }

    public void print() {
        System.out.println(src + "," + label + "," + dest);
    }

    public String toString() {
        return src + "," + label + "," + dest;
    }
}
