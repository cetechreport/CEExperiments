package Common;

public class Triple<T1, T2, T3> {
    public T1 v1;
    public T2 v2;
    public T3 v3;

    public Triple(T1 v1, T2 v2, T3 v3) {
        this.v1 = v1;
        this.v2= v2;
        this.v3 = v3;
    }

    public Triple(Triple<T1, T2, T3> triple) {
        this.v1 = triple.v1;
        this.v2 = triple.v2;
        this.v3 = triple.v3;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Triple<?, ?, ?> triple = (Triple<?, ?, ?>) o;

        return v1.equals(triple.getV1()) &&
            v2.equals(triple.getV2()) &&
            v3.equals(triple.getV3());
    }

    @Override
    public int hashCode() {
        return v1.hashCode() + v2.hashCode() + v3.hashCode();
    }

    @Override
    public String toString() {
        return v1 + "," + v2 + "," + v3;
    }
}
