package Pessimistic.Parallel;

import Common.Util;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

public class SubgraphExtractor implements Runnable {
    int threadId;
    Map<Integer, Map<Integer, List<Integer>>> src2label2dest;
    Map<Integer, Map<Integer, List<Integer>>> dest2label2src;
    List<List<Integer>> listOfStarters = new ArrayList<>();
    List<String> vLists = new ArrayList<>();
    List<String> labelSeqs = new ArrayList<>();

    public void run() {
        for (int i = 0; i < vLists.size(); ++i) {
            Set<String> subgraph = traverse(vLists.get(i), labelSeqs.get(i), listOfStarters.get(i));
            try {
                persist(vLists.get(i), labelSeqs.get(i), subgraph);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private Set<String> traverse(String vListString, String labelSeqString, List<Integer> starters) {
        Integer[] vList = Util.toVList(vListString);
        Integer[] labelSeq = Util.toLabelSeq(labelSeqString);

        Integer nextVirSrc, nextVirDest, nextLabel, currentV, currentPhysical;
        Set<Integer> covered;
        List<Integer> current, nextPhysicals;
        Map<Integer, Map<Integer, List<Integer>>> current2label2next;
        List<Integer> subVList;

        Set<String> allCsv = new HashSet<>();
        for (Integer startSrc : starters) {
            Stack<List<Integer>> stack = new Stack<>();
            Set<String> currentCsv = new HashSet<>();
            for (Integer startDest : src2label2dest.get(startSrc).get(labelSeq[0])) {
                subVList = new ArrayList<>();
                subVList.add(startSrc);
                subVList.add(startDest);
                stack.push(subVList);
            }

            while (!stack.isEmpty()) {
                current = stack.pop();
//                if (hasTraversed(current, labelSeq, allCsv)) {
//                    currentCsv.addAll(toCsvEdges(current, labelSeq));
//                    continue;
//                }

                nextVirSrc = vList[current.size()];
                nextVirDest = vList[current.size() + 1];
                nextLabel = labelSeq[current.size() / 2];
                covered = new HashSet<>(Arrays.asList(vList).subList(0, current.size()));

                if (covered.contains(nextVirSrc)) {
                    current2label2next = src2label2dest;
                    currentV = nextVirSrc;
                } else if (covered.contains(nextVirDest)) {
                    current2label2next = dest2label2src;
                    currentV = nextVirDest;
                } else {
                    System.out.println("UNHANDLED: both sides of edge not covered");
                    return null;
                }

                currentPhysical = findPhysical(vList, current, currentV);
                if (current2label2next.containsKey(currentPhysical)) {
                    if (current2label2next.get(currentPhysical).containsKey(nextLabel)) {
                        nextPhysicals = current2label2next.get(currentPhysical).get(nextLabel);
                        for (Integer nextPhysical : nextPhysicals) {
                            if (currentV.equals(nextVirSrc)) {
                                if (hasTraversed(currentPhysical, nextLabel, nextPhysical, allCsv)) {
                                    currentCsv.addAll(toCsvEdges(current, labelSeq));
                                    continue;
                                }
                            } else {
                                if (hasTraversed(nextPhysical, nextLabel, currentPhysical, allCsv)) {
                                    currentCsv.addAll(toCsvEdges(current, labelSeq));
                                    continue;
                                }
                            }

                            subVList = new ArrayList<>(current);
                            if (currentV.equals(nextVirSrc)) {
                                subVList.add(currentPhysical);
                                subVList.add(nextPhysical);
                            } else {
                                subVList.add(nextPhysical);
                                subVList.add(currentPhysical);
                            }

                            if (subVList.size() < vList.length) {
                                stack.push(subVList);
                            } else if (subVList.size() == vList.length) {
                                currentCsv.addAll(toCsvEdges(subVList, labelSeq));
                            }
                        }
                    }
                }
            }

            allCsv.addAll(currentCsv);
        }

        return allCsv;
    }

    private boolean hasTraversed(
        List<Integer> physicalVList, Integer[] labelSeq, Set<String> subgraph) {
        Integer src = physicalVList.get(physicalVList.size() - 2);
        Integer dest = physicalVList.get(physicalVList.size() - 1);
        Integer label = labelSeq[physicalVList.size() / 2 - 1];
        String edge = src + "," + label + "," + dest;
        return subgraph.contains(edge);
    }

    private boolean hasTraversed(
        Integer src, Integer label, Integer dest, Set<String> subgraph) {
        String edge = src + "," + label + "," + dest;
        return subgraph.contains(edge);
    }

    private Integer findPhysical(Integer[] vList, List<Integer> physicals, Integer target) {
        for (int i = 0; i < vList.length; ++i) {
            if (vList[i].equals(target)) {
                return physicals.get(i);
            }
        }

        System.out.println("ERROR: target does not exist: " + target);
        return -1;
    }

    private Set<String> toCsvEdges(List<Integer> physicalVList, Integer[] labelSeq) {
        Set<String> edges = new HashSet<>();
        for (int i = 0; i < physicalVList.size(); i += 2) {
            edges.add(physicalVList.get(i) + "," + labelSeq[i / 2] + "," + physicalVList.get(i + 1));
        }
        return edges;
    }

    private void persist(String vListString, String labelSeqString, Set<String> subgraph)
        throws Exception {
        String fileName = vListString + "_" + labelSeqString + "_" + threadId + ".d";
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
        for (String edge : subgraph) {
            writer.write(edge + "\n");
        }
        writer.close();
    }

    public void addJob(String vList, String labelSeq, List<Integer> starters) {
        vLists.add(vList);
        labelSeqs.add(labelSeq);
        listOfStarters.add(starters);
    }

    public SubgraphExtractor(
        int threadId,
        Map<Integer, Map<Integer, List<Integer>>> src2label2dest,
        Map<Integer, Map<Integer, List<Integer>>> dest2label2src) {
        this.threadId = threadId;
        this.src2label2dest = src2label2dest;
        this.dest2label2src = dest2label2src;
    }
}
