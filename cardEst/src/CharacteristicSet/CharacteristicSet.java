package CharacteristicSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

public class CharacteristicSet {
    private Map<Integer, Integer> outLabel2count;
    private Map<Integer, Integer> inLabel2count;

    public CharacteristicSet(
        Map<Integer, List<Integer>> outLabel2dests, Map<Integer, List<Integer>> inLabel2dests) {

        outLabel2count = new HashMap<>();
        inLabel2count = new HashMap<>();

        if (null != outLabel2dests) {
            for (Integer label : outLabel2dests.keySet()) {
                outLabel2count.put(label, outLabel2dests.get(label).size());
            }
        }

        if (null != inLabel2dests) {
            for (Integer label : inLabel2dests.keySet()) {
                inLabel2count.put(label, inLabel2dests.get(label).size());
            }
        }
    }

    public CharacteristicSet() {
        outLabel2count = new HashMap<>();
        inLabel2count = new HashMap<>();
    }

    public void add(Integer label, Integer count, int direction) {
        if (Constants.OUT == direction) {
            outLabel2count.put(label, count);
        } else if (Constants.IN == direction) {
            inLabel2count.put(label, count);
        }
    }

    // merge cSet into this cSet
    public void merge(CharacteristicSet cSet) {
        for (Integer outLabel : cSet.outLabel2count.keySet()) {
            this.outLabel2count.put(
                outLabel, this.outLabel2count.get(outLabel) + cSet.outLabel2count.get(outLabel)
            );
        }

        for (Integer inLabel : cSet.inLabel2count.keySet()) {
            this.inLabel2count.put(
                inLabel, this.inLabel2count.get(inLabel) + cSet.inLabel2count.get(inLabel)
            );
        }
    }

    public CharacteristicSet[] breakdown(CharacteristicSet subset) {
        CharacteristicSet[] cSets = new CharacteristicSet[]{
            new CharacteristicSet(), new CharacteristicSet()
        };

        for (Integer outLabel : this.outLabel2count.keySet()) {
            if (subset.outLabel2count.containsKey(outLabel)) {
                cSets[0].add(outLabel, this.outLabel2count.get(outLabel), Constants.OUT);
            } else {
                cSets[1].add(outLabel, this.outLabel2count.get(outLabel), Constants.OUT);
            }
        }

        for (Integer inLabel : this.inLabel2count.keySet()) {
            if (subset.inLabel2count.containsKey(inLabel)) {
                cSets[0].add(inLabel, this.inLabel2count.get(inLabel), Constants.IN);
            } else {
                cSets[1].add(inLabel, this.inLabel2count.get(inLabel), Constants.IN);
            }
        }

        return cSets;
    }

    public boolean containsStar(Set<Integer> outLabels, Set<Integer> inLabels) {
        return (
            outLabel2count.keySet().containsAll(outLabels) &&
            inLabel2count.keySet().containsAll(inLabels)
        );
    }

    public boolean isSubsetOf(CharacteristicSet cSet) {
        return (
            cSet.outLabel2count.keySet().containsAll(this.outLabel2count.keySet()) &&
            cSet.inLabel2count.keySet().containsAll(this.inLabel2count.keySet())
        );
    }

    public int size() {
        return outLabel2count.keySet().size() + inLabel2count.keySet().size();
    }

    public int getCount(Integer label, int direction) {
        if (direction == Constants.OUT) {
            return outLabel2count.get(label);
        } else if (direction == Constants.IN) {
            return inLabel2count.get(label);
        } else {
            return -1;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CharacteristicSet charSet = (CharacteristicSet) o;

        return (
            outLabel2count.keySet().equals(charSet.outLabel2count.keySet()) &&
            inLabel2count.keySet().equals(charSet.inLabel2count.keySet())
        );
    }

    @Override
    public int hashCode() {
        int result = 0;
        for (Integer label : outLabel2count.keySet()) {
            result += label;
        }
        for (Integer label : inLabel2count.keySet()) {
            result += label;
        }
        return result;
    }

    @Override
    public String toString() {
        StringJoiner sj = new StringJoiner(",");
        sj.add(Integer.toString(outLabel2count.size()));
        for (Integer label : outLabel2count.keySet()) {
            sj.add(label + "->" + outLabel2count.get(label));
        }

        sj.add(Integer.toString(inLabel2count.size()));
        for (Integer label : inLabel2count.keySet()) {
            sj.add(label + "<-" + inLabel2count.get(label));
        }

        return sj.toString();
    }
}
