package CEG;

import Common.Util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Path {
    // ["0-1", "0-1;1-2", "0-1;1-2;2-3", ...]
    List<String> vLists = new ArrayList<>();

    public String getTopNode() {
        return vLists.get(vLists.size() - 1);
    }

    public void append(String node) {
        vLists.add(node);
    }

    public void appendAll(List<String> nodes) {
        vLists.addAll(nodes);
    }

    public int size() {
        return vLists.size();
    }

    public String get(int index) {
        return vLists.get(index);
    }

    public String getExt(int index) {
        Set<String> current = new HashSet<>(Arrays.asList(vLists.get(index).split(";")));
        Set<String> base = new HashSet<>(Arrays.asList(vLists.get(index - 1).split(";")));
        current.removeAll(base);
        if (current.size() != 1) {
            System.err.println("ERROR: wrong path");
            System.err.println("  current: " + vLists.get(index));
            System.err.println("  base: " + vLists.get(index - 1));
            return "";
        }

        String ext = "";
        for (String edge : current) {
            ext = edge;
        }
        return ext;
    }

    public Map<Integer, Integer> getPartitionScheme(int budget) {
        Set<Integer> initial = new HashSet<>(Arrays.asList(Util.toVList(vLists.get(0))));
        Integer[] vList = Util.toVList(vLists.get(vLists.size() - 1));
        Set<Integer> leaves = Util.getLeaves(vList);
        initial.removeAll(leaves); // this gets join attributes covered unconditionally

        Map<Integer, Integer> numPartitions = new HashMap<>();
        for (Integer v : vList) {
            numPartitions.put(v, 1);
        }

        int rootedBudget = (int) Math.floor(Math.pow(budget, 1.0 / initial.size()));
        for (Integer qualifiedJoinV : initial) {
            numPartitions.put(qualifiedJoinV, rootedBudget);
        }

        return numPartitions;
    }
}
