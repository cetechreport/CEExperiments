package CEG;

import Common.Pair;
import Common.Query;
import Common.Util;
import Graphflow.Constants;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

public class CEG {
    Map<String, List<String>> adjLists = new HashMap<>();
    protected int nodeMinLen;
    protected String queryVList;

    Integer patternType;
    // partitionScheme -> vList -> hashIdComb -> labelSeq -> Long
    Map<String, Map<String, Map<String, Map<String, Long>>>> catalogues = new HashMap<>();

    Map<String, List<String>> scheme2hashIdCombs = new HashMap<>();

    protected void readPartitionedCatalogue(String pcatFile) throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(pcatFile));
        String line = reader.readLine();
        while (line != null) {
            String[] info = line.split(",");
            patternType = Integer.parseInt(info[0]);
            String partitionScheme = info[1];
            String hashIdComb = info[2];
            String vList = info[3];
            String labelSeq = info[4];
            Long card = Long.parseLong(info[5]);

            catalogues.putIfAbsent(partitionScheme, new HashMap<>());
            catalogues.get(partitionScheme).putIfAbsent(vList, new HashMap<>());
            catalogues.get(partitionScheme).get(vList).putIfAbsent(hashIdComb, new HashMap<>());
            catalogues.get(partitionScheme).get(vList).get(hashIdComb).put(labelSeq, card);

            line = reader.readLine();
        }
        reader.close();
    }

    protected Map<String, Set<String>> getVListEdge2HashIdCombs(String scheme) {
        Map<String, Set<String>> vListEdge2HashIdCombs = new HashMap<>();

        for (String vList : catalogues.get(scheme).keySet()) {
            for (String hashIdComb : catalogues.get(scheme).get(vList).keySet()) {
                String[] hashIds = hashIdComb.split(";");
                String[] vListEdges = vList.split(";");

                if (vListEdges.length == 2) {
                    vListEdge2HashIdCombs.putIfAbsent(vListEdges[0], new HashSet<>());
                    vListEdge2HashIdCombs.putIfAbsent(vListEdges[1], new HashSet<>());
                    vListEdge2HashIdCombs.get(vListEdges[0]).add(hashIds[0]);
                    vListEdge2HashIdCombs.get(vListEdges[1]).add(hashIds[1]);
                } else if (vListEdges.length == 1) {
                    vListEdge2HashIdCombs.putIfAbsent(vListEdges[0], new HashSet<>());
                    vListEdge2HashIdCombs.get(vListEdges[0]).add(hashIds[0]);
                }
            }
        }

        return vListEdge2HashIdCombs;
    }

    protected void prepareHashIdCombs(Query query, String partitionScheme) {
        if (scheme2hashIdCombs.containsKey(partitionScheme)) return;

        Pair<String, String> vListAndLabelSeq = Util.topologyToVListAndLabelSeq(query.topology);
        vListAndLabelSeq = Util.sort(vListAndLabelSeq);
        scheme2hashIdCombs.put(
            partitionScheme,
            Util.getQueryHashIdCombs(
                vListAndLabelSeq.key, getVListEdge2HashIdCombs(partitionScheme), Constants.HASH)
        );
    }

    public List<Path> getAllPaths() {
        List<Path> allPaths = new ArrayList<>();

        List<String> startingNodes = adjLists.keySet().stream()
            .filter(node -> node.split(";").length == nodeMinLen)
            .collect(Collectors.toList());

        for (String startingNode : startingNodes) {
            List<String> start = new ArrayList<>();
            start.add(startingNode);

            Stack<List<String>> stack = new Stack<>();
            stack.add(start);

            while (!stack.empty()) {
                List<String> current = stack.pop();
                if (current.get(current.size() - 1).equals(queryVList)) {
                    Path path = new Path();
                    path.appendAll(current);
                    allPaths.add(path);
                }

                for (String neighbour : adjLists.get(current.get(current.size() - 1))) {
                    List<String> next = new ArrayList<>(current);
                    next.add(neighbour);
                    stack.add(next);
                }
            }
        }

        return allPaths;
    }

    private void getNextNodesForIndex(
        String[] vListEdges, int index, List<List<String>> result) {
        Set<List<String>> toAdd = new HashSet<>();
        for (List<String> node : result) {
            List<String> newNode = new ArrayList<>(node);
            newNode.add(vListEdges[index]);
            if (Util.isConnected(new HashSet<>(newNode))) {
                Collections.sort(newNode);
                toAdd.add(newNode);
            }
        }
        result.addAll(toAdd);

        List<String> single = new ArrayList<>();
        single.add(vListEdges[index]);
        result.add(single);
    }

    private void buildCEG(String queryVListString, int initLen) {
        String[] queryVListEdges = queryVListString.split(";");

        List<List<String>> allNodes = new ArrayList<>();
        for (int i = 0; i < queryVListEdges.length; ++i) {
            getNextNodesForIndex(queryVListEdges, i, allNodes);
        }

        List<List<String>> allQualifiedNodes = allNodes.stream()
            .filter(node -> node.size() >= initLen)
            .filter(node -> Util.isConnected(new HashSet<>(node)))
            .collect(Collectors.toList());

        for (List<String> node : allQualifiedNodes) {
            List<String> neighbours = allQualifiedNodes.stream()
                .filter(otherNode -> otherNode.size() == node.size() + 1 && isSubquery(node, otherNode))
                .map(otherNode -> String.join(";", otherNode))
                .collect(Collectors.toList());
            String current = String.join(";", node);
            adjLists.put(current, neighbours);
        }
    }

    private boolean isSubquery(List<String> shortQuery, List<String> longQuery) {
        Set<String> shortEdgeSet = new HashSet<>(shortQuery);
        Set<String> longEdgeSet = new HashSet<>(longQuery);
        return longEdgeSet.containsAll(shortEdgeSet);
    }

    public List<String> topologicalSort() {
        List<String> sorted = new ArrayList<>();

        List<String> currentLevelNodes = adjLists.keySet().stream()
            .filter(node -> node.split(";").length == nodeMinLen)
            .collect(Collectors.toList());
        List<String> nextLevelNodes = new ArrayList<>();

        while (!currentLevelNodes.isEmpty()) {
            for (String node : currentLevelNodes) {
                sorted.add(node);
                if (adjLists.containsKey(node)) {
                    nextLevelNodes.addAll(adjLists.get(node));
                }
            }

            currentLevelNodes = nextLevelNodes;
            nextLevelNodes = new ArrayList<>();
        }

        return sorted;
    }

    public CEG(String queryVList, int minLen) {
        buildCEG(queryVList, minLen);
        this.nodeMinLen = minLen;
        this.queryVList = queryVList;
    }
}
