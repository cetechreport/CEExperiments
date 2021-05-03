package Common;

import Graphflow.Constants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

public class Util {
    public static Integer[] toLabelSeq(String labelSeqString) {
        String[] splitted = labelSeqString.split("->");
        Integer[] labelSeq = new Integer[splitted.length];
        for (int i = 0; i < splitted.length; ++i) {
            labelSeq[i] = Integer.parseInt(splitted[i]);
        }
        return labelSeq;
    }

    public static Set<Integer> getLeaves(Integer[] vList) {
        Map<Integer, Integer> occurrences = new HashMap<>();
        for (Integer v : vList) {
            occurrences.put(v, occurrences.getOrDefault(v, 0) + 1);
        }

        Set<Integer> leaves = new HashSet<>();
        for (Integer v : occurrences.keySet()) {
            if (occurrences.get(v).equals(1)) {
                leaves.add(v);
            }
        }
        return leaves;
    }

    public static Integer[] toVList(String vListString) {
        if (vListString.equals("")) return new Integer[0];

        String[] splitted = vListString.split(";");
        Integer[] vList = new Integer[splitted.length * 2];
        for (int i = 0; i < splitted.length; i++) {
            String[] srcDest = splitted[i].split("-");
            vList[i * 2] = Integer.parseInt(srcDest[0]);
            vList[i * 2 + 1] = Integer.parseInt(srcDest[1]);
        }
        return vList;
    }

    /**
     *
     * @param vListString
     * @param labelSeqString
     * @param leaves
     * @return (baseVList, extVList), (baseLabelSeq, extLabel)
     */
    public static List<Pair<Pair<String, String>, Pair<String, String>>> splitToBaseAndExt(
        String vListString, String labelSeqString, Set<Integer> leaves) {

        List<Pair<Pair<String, String>, Pair<String, String>>> splits = new ArrayList<>();

        String[] edges = vListString.split(";");
        String[] labelSeq = labelSeqString.split("->");
        for (int i = 0; i < edges.length; ++i) {
            String[] srcDest = edges[i].split("-");
//            if (leaves.contains(Integer.parseInt(srcDest[0])) ||
//                leaves.contains(Integer.parseInt(srcDest[1]))) {

            StringJoiner vListSj = new StringJoiner(";");
            StringJoiner labelSeqSj = new StringJoiner("->");
            for (int j = 0; j < edges.length; ++j) {
                if (i == j) continue;
                vListSj.add(edges[j]);
                labelSeqSj.add(labelSeq[j]);
            }
            if (leaves.contains(Integer.parseInt(srcDest[0])) ||
                    leaves.contains(Integer.parseInt(srcDest[1]))) {
                splits.add(new Pair<>(
                        new Pair<>(vListSj.toString(), edges[i]),
                        new Pair<>(labelSeqSj.toString(), labelSeq[i])
                ));
            }
            splits.add(new Pair<>(
                    new Pair<>(edges[i], vListSj.toString()),
                    new Pair<>(labelSeq[i], labelSeqSj.toString())
            ));
//            }
        }

        return splits;
    }

    public static boolean doesEntryFitFormula(int entryType, int formulaType) {
        if (formulaType == Constants.C_ALL) {
            return true;
        } else if (formulaType == Constants.C_PATH) {
            return entryType == Constants.C_PATH;
        } else if (formulaType == Constants.C_FORK) {
            return entryType == Constants.C_FORK;
        } else if (formulaType == Constants.C_STAR) {
            return entryType == Constants.C_FORK || entryType == Constants.C_STAR;
        } else {
            return false;
        }
    }

    public static int getFormulaType(String formulaTypeString) {
        if (formulaTypeString.contains("star")) {
            return Constants.C_STAR;
        } else if (formulaTypeString.contains("fork")) {
            return Constants.C_FORK;
        } else if (formulaTypeString.contains("path")) {
            return Constants.C_PATH;
        } else if (formulaTypeString.contains("all")) {
            return Constants.C_ALL;
        } else {
            System.err.println("ERROR: unrecognized formula type");
            return -1;
        }
    }

    public static String extractPath(Topology topology, Integer[] vertexList) {
        StringJoiner path = new StringJoiner("->");
        for (int i = 0; i < vertexList.length; i += 2) {
            for (Integer label : topology.outgoing.get(vertexList[i]).keySet()) {
                for (Integer next : topology.outgoing.get(vertexList[i]).get(label)) {
                    if (next.equals(vertexList[i + 1])) {
                        path.add(label.toString());
                    }
                }
            }
        }

        return path.toString();
    }

    public static String toVListString(Integer[] vList) {
        StringJoiner sj = new StringJoiner(";");
        for (int i = 0; i < vList.length; i += 2) {
            sj.add(vList[i] + "-" + vList[i + 1]);
        }
        return sj.toString();
    }

    public static Pair<String, String> topologyToVListAndLabelSeq(Topology topology) {
        StringJoiner vList = new StringJoiner(";");
        StringJoiner labelSeq = new StringJoiner("->");
        for (int i = 0; i < topology.outgoing.size(); ++i) {
            for (Integer label : topology.outgoing.get(i).keySet()) {
                for (Integer dest : topology.outgoing.get(i).get(label)) {
                    vList.add(i + "-" + dest);
                    labelSeq.add(label.toString());
                }
            }
        }

        return new Pair<>(vList.toString(), labelSeq.toString());
    }

    public static Pair<String, String> sort(Pair<String, String> vListAndLabelSeq) {
        String[] vList = vListAndLabelSeq.key.split(";");
        String[] labelSeq = vListAndLabelSeq.value.split("->");
        Map<String, String> vList2labelSeq = new HashMap<>();
        for (int i = 0; i < vList.length; ++i) {
            vList2labelSeq.put(vList[i], labelSeq[i]);
        }

        String[] vListSort = vList.clone();
        Arrays.sort(vListSort);
        String[] labelSeqSort = new String[vListSort.length];
        for (int i = 0; i < vListSort.length; ++i) {
            labelSeqSort[i] = vList2labelSeq.get(vListSort[i]);
        }

        return new Pair<>(String.join(";", vListSort), String.join("->", labelSeqSort));
    }

    public static String sort(String vListString) {
        String[] vList = vListString.split(";");
        String[] vListSort = vList.clone();
        Arrays.sort(vListSort);
        return String.join(";", vListSort);
    }

    public static Set<String> toVListSet(String vListString) {
        String[] vList = vListString.split(";");
        return new HashSet<>(Arrays.asList(vList));
    }

    public static String toVListString(Set<String> vListSet) {
        StringJoiner sj = new StringJoiner(";");
        for (String edge : vListSet) {
            sj.add(edge);
        }
        return sj.toString();
    }

    public static boolean isConnected(Set<String> vListSet) {
        if (vListSet.isEmpty()) return true;

        Set<String> unprocessed = new HashSet<>(vListSet);
        Set<String> nextUnprocess = new HashSet<>();
        Set<Integer> component = new HashSet<>();

        while (!unprocessed.isEmpty()) {
            int sizeBefore = component.size();
            for (String edge : unprocessed) {
                Integer[] srcDest = Util.toVList(edge);
                if (component.isEmpty()) {
                    component.add(srcDest[0]);
                    component.add(srcDest[1]);
                } else {
                    if (component.contains(srcDest[0]) || component.contains(srcDest[1])) {
                        component.add(srcDest[0]);
                        component.add(srcDest[1]);
                    } else {
                        nextUnprocess.add(edge);
                    }
                }
            }
            int sizeAfter = component.size();

            if (sizeBefore == sizeAfter) return false;

            unprocessed = nextUnprocess;
            nextUnprocess = new HashSet<>();
        }

        return true;
    }

    public static String extractLabelSeq(
        String subVListString, String vListString, String labelSeqString) {

        Map<String, String> edge2label = new HashMap<>();
        String[] edges = vListString.split(";");
        String[] labelSeq = labelSeqString.split("->");
        for (int i = 0; i < edges.length; ++i) {
            edge2label.put(edges[i], labelSeq[i]);
        }

        StringJoiner subLabelSeq = new StringJoiner("->");
        String[] subVList = subVListString.split(";");
        for (String edge : subVList) {
            subLabelSeq.add(edge2label.get(edge));
        }

        return subLabelSeq.toString();
    }

    public static Set<String> extractLabelSeqs(
        String subVListString, String vListString, List<String> labelSeqStrings) {
        Set<String> labelSeqs = new HashSet<>();
        for (String labelSeqString : labelSeqStrings) {
            labelSeqs.add(extractLabelSeq(subVListString, vListString, labelSeqString));
        }
        return labelSeqs;
    }

    public static String extractHashIdComb(
        String subVListString, String queryVListString, String queryHashIdString) {
        StringJoiner hashIds = new StringJoiner(";");
        Set<String> subVListSet = Util.toVListSet(subVListString);

        String[] queryVListEdges = queryVListString.split(";");
        String[] queryHashIds = queryHashIdString.split(";");
        for (int i = 0; i < queryVListEdges.length; ++i) {
            if (subVListSet.contains(queryVListEdges[i])) {
                hashIds.add(queryHashIds[i]);
            }
        }

        return hashIds.toString();
    }

    public static String extractScheme(String subVListString, String queryVListString, String queryScheme) {
        return extractHashIdComb(subVListString, queryVListString, queryScheme);
    }

    public static String hash(Integer srcV, int srcBudget, Integer destV, int destBudget) {
        return (srcV % srcBudget) + "-" + (destV % destBudget);
    }

    public static String bucketize(int srcDeg, int srcBucketBase, int destDeg, int destBucketBase) {
        int srcBucketExp = 0;
        if (srcBucketBase != 0) {
            srcBucketExp = (int) Math.floor(Math.log(srcDeg) / Math.log(srcBucketBase));
        }
        int destBucketExp = 0;
        if (destBucketBase != 0) {
            destBucketExp = (int) Math.floor(Math.log(destDeg) / Math.log(destBucketBase));
        }

        return srcBucketExp + "-" + destBucketExp;
    }

    public static boolean canConnect(String vListString, String hashIdCombString) {
        Map<Integer, Integer> v2hash = new HashMap<>();
        Integer[] vList = Util.toVList(vListString);
        Integer[] hashIdList = Util.toVList(hashIdCombString);
        for (int i = 0; i < vList.length; ++i) {
            Integer v = vList[i];
            Integer hash = hashIdList[i];
            if (v2hash.containsKey(v) && !v2hash.get(v).equals(hash)) return false;
            v2hash.put(v, hash);
        }

        return true;
    }

    public static boolean isAcyclicConnected(String vListString) {
        Set<Integer> component = new HashSet<>();
        Integer[] vList = toVList(vListString);
        for (int i = 0; i < vList.length; i += 2) {
            if (!component.isEmpty()) {
                boolean hasSrc = component.contains(vList[i]);
                boolean hasDest = component.contains(vList[i + 1]);
                if ((hasSrc && hasDest) || (!hasSrc && !hasDest)) {
                    return false;
                }
            }

            component.add(vList[i]);
            component.add(vList[i + 1]);
        }
        return true;
    }

    private static void getQueryHashIdCombs(String[] vListEdges, List<List<String>> hashIdLists,
        List<String> result, int partType, int depth, String current) {
        if (depth == hashIdLists.size()) {
            result.add(current);
            return;
        }

        for (int i = 0; i < hashIdLists.get(depth).size(); i++) {
            String next = hashIdLists.get(depth).get(i);
            String updated = current.isEmpty() ? next : current + ";" + next;
            if (depth > 0 && partType == Constants.HASH) {
                boolean isConnected =
                    Util.canConnect(
                        String.join(";", Arrays.copyOfRange(vListEdges, 0, depth+1)),
                        updated);
                if (!isConnected) continue;
            }
            getQueryHashIdCombs(vListEdges, hashIdLists, result, partType, depth + 1, updated);
        }
    }

    public static List<String> getQueryHashIdCombs(
        String queryVListString, Map<String, Set<String>> vListEdge2hashIdCombs, int partType) {
        Integer[] queryVList = Util.toVList(queryVListString);
        Set<Integer> leaves = Util.getLeaves(queryVList);
        Set<Integer> joinVertices = new HashSet<>(Arrays.asList(queryVList));
        joinVertices.removeAll(leaves);

        String[] vListEdges = queryVListString.split(";");

        List<List<String>> hashIdLists = new ArrayList<>();
        for (int i = 0; i < vListEdges.length; ++i) {
            hashIdLists.add(new ArrayList<>());
            hashIdLists.get(i).addAll(vListEdge2hashIdCombs.get(vListEdges[i]));
        }

        List<String> queryHashIdCombs = new ArrayList<>();
        getQueryHashIdCombs(
            queryVListString.split(";"), hashIdLists, queryHashIdCombs, partType, 0, "");

        return queryHashIdCombs;
    }

    public static String partitionSchemeToString(
        String queryVList, Map<Integer, Integer> numPartitions) {
        Integer[] vList = toVList(queryVList);
        StringJoiner partitionScheme = new StringJoiner(";");
        for (int i = 0; i < vList.length; i += 2) {
            partitionScheme.add(numPartitions.get(vList[i]) + "-" + numPartitions.get(vList[i + 1]));
        }
        return partitionScheme.toString();
    }

    public static boolean isSubquery(String subVList, String queryVList) {
        String[] subQEdges = subVList.split(";");
        Set<String> queryEdges = toVListSet(queryVList);
        for (String edge : subQEdges) {
            if (!queryEdges.contains(edge)) return false;
        }
        return true;
    }
}
