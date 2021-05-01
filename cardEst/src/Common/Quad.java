package Common;

public class Quad<T1, T2, T3, T4> {
    public T1 v1;
    public T2 v2;
    public T3 v3;
    public T4 v4;

    public Quad(T1 v1, T2 v2, T3 v3, T4 v4) {
        this.v1 = v1;
        this.v2= v2;
        this.v3 = v3;
        this.v4 = v4;
    }

    public Quad(Quad<T1, T2, T3, T4> quad) {
        this.v1 = quad.v1;
        this.v2 = quad.v2;
        this.v3 = quad.v3;
        this.v4 = quad.v4;
    }

    public T1 getV1() {
        return this.v1;
    }

    public T2 getV2() {
        return this.v2;
    }

    public T3 getV3() {
        return this.v3;
    }

    public T4 getV4() {
        return this.v4;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Quad<?, ?, ?, ?> quad = (Quad<?, ?, ?, ?>) o;

        return v1.equals(quad.getV1()) &&
            v2.equals(quad.getV2()) &&
            v3.equals(quad.getV3()) &&
            v4.equals(quad.getV4());
    }

    @Override
    public int hashCode() {
        return v1.hashCode() + v2.hashCode() + v3.hashCode() + v4.hashCode();
    }

    @Override
    public String toString() {
        return v1 + "," + v2 + "," + v3 + "," + v4;
    }
}
