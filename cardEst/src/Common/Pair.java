package Common;

public class Pair<T1, T2> {
    public T1 key;
    public T2 value;

    public Pair(T1 key, T2 value) {
        this.key = key;
        this.value = value;
    }

    public Pair(Pair<T1, T2> pair) {
        this.key = pair.key;
        this.value = pair.value;
    }

    public T1 getKey() {
        return this.key;
    }

    public T2 getValue() {
        return this.value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Pair<?, ?> pair = (Pair<?, ?>) o;

        return key.equals(pair.getKey()) && value.equals(pair.getValue());
    }

    @Override
    public int hashCode() {
        return key.hashCode() + value.hashCode();
    }

    @Override
    public String toString() {
        return key + "," + value;
    }
}
