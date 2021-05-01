package Graphflow.Parallel;

import Common.Topology;
import Graphflow.Constants;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

public class CatalogueConstructionForLabelSeq implements Runnable {
    private int threadId;
    private int parentId;

    private Map<Integer, Map<Integer, List<Integer>>> src2label2dest;
    private Map<Integer, Map<Integer, List<Integer>>> dest2label2src;
    private Map<Integer, Map<Integer, List<Integer>>> label2src2dest;
    private Map<Integer, Map<Integer, List<Integer>>> label2dest2src;

    String line;

    public void run() {
        String[] info = line.split(",");
        Integer queryType = Integer.parseInt(info[0]);

        try {
            String vList = info[1];
            BufferedWriter writer = new BufferedWriter(
                new FileWriter("catalogue_part_" + parentId + "_" + threadId + ".csv"));
            for (int i = 2; i < info.length; ++i) {
                StringJoiner sj = new StringJoiner(",");
                sj.add(queryType.toString());
                sj.add(vList);
                sj.add(info[i]);
                if (vList.split(";").length < 4) {
                    sj.add(count(vList, info[i]).toString());
                } else {
                    sj.add("0");
                }
                writer.write(sj.toString() + "\n");
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Long count(String vListString, String labelSeqString) throws Exception {
        Integer[] labelSeq = toLabelSeq(labelSeqString);
        Integer[] vList = toVList(vListString);
        int type = getType(vList);
        Integer starCenter;
        if (type == Constants.C_FORK) {
            return countFork(labelSeq, vList);
        } else if (type == Constants.C_STAR || type == Constants.C_PATH) {
            starCenter = getCenter(vList);
        } else if (type == Constants.C_SINGLE_LABEL) {
            return countSingleLabel(labelSeq[0]);
        } else {
            System.err.println("ERROR: unrecognized pattern type");
            return -1L;
        }

        Set<Integer> leaves = getLeaves(vList);
        Integer virtualSrc, virtualDest, startDest;
        List<Integer> updatedPhysicals;

        Map<Integer, Map<Integer, List<Integer>>> listSrcDest;
        if (starCenter.equals(vList[1])) {
            listSrcDest = label2dest2src;
            startDest = vList[0];
        } else {
            listSrcDest = label2src2dest;
            startDest = vList[1];
        }

        Long count = 0L;
        for (Integer origin : listSrcDest.get(labelSeq[0]).keySet()) {
            Map<Integer, List<Integer>> virtual2physicals = new HashMap<>();

            virtual2physicals.putIfAbsent(starCenter, new ArrayList<>());
            virtual2physicals.get(starCenter).add(origin);
            virtual2physicals.putIfAbsent(startDest, new ArrayList<>());
            virtual2physicals.get(startDest).addAll(listSrcDest.get(labelSeq[0]).get(origin));

            boolean rerun;
            do {
                rerun = false;
                for (int i = 1; i < labelSeq.length; ++i) {
                    virtualSrc = vList[i * 2];
                    virtualDest = vList[i * 2 + 1];

                    if (virtual2physicals.containsKey(virtualSrc) &&
                        virtual2physicals.containsKey(virtualDest)) continue;

                    // determine direction and extend one step
                    if (virtual2physicals.containsKey(virtualSrc)) {
                        virtual2physicals.putIfAbsent(virtualDest, new ArrayList<>());
                        updatedPhysicals = new ArrayList<>();
                        for (Integer currentV : virtual2physicals.get(virtualSrc)) {
                            if (!src2label2dest.containsKey(currentV)) continue;
                            if (!src2label2dest.get(currentV).containsKey(labelSeq[i])) continue;

                            updatedPhysicals.add(currentV);
                            virtual2physicals.get(virtualDest).addAll(
                                src2label2dest.get(currentV).get(labelSeq[i])
                            );
                        }
                        virtual2physicals.put(virtualSrc, updatedPhysicals);
                    } else if (virtual2physicals.containsKey(virtualDest)) {
                        virtual2physicals.putIfAbsent(virtualSrc, new ArrayList<>());
                        updatedPhysicals = new ArrayList<>();
                        for (Integer currentV : virtual2physicals.get(virtualDest)) {
                            if (!dest2label2src.containsKey(currentV)) continue;
                            if (!dest2label2src.get(currentV).containsKey(labelSeq[i])) continue;

                            updatedPhysicals.add(currentV);
                            virtual2physicals.get(virtualSrc).addAll(
                                dest2label2src.get(currentV).get(labelSeq[i])
                            );
                        }
                        virtual2physicals.put(virtualDest, updatedPhysicals);
                    } else {
                        rerun = true;
                    }
                }
            } while (rerun);

            Long countForOrigin = 1L;
            for (Integer leaf : leaves) {
                if (!virtual2physicals.containsKey(leaf) || virtual2physicals.get(leaf).isEmpty()) {
                    countForOrigin = 0L;
                    break;
                }
                countForOrigin *= virtual2physicals.get(leaf).size();
            }

            count += countForOrigin;
        }

        return count;
    }

    private Long countFork(Integer[] labelSeq, Integer[] vList) {
        Topology topology = toTopology(labelSeq, vList);

        Map<Integer, Integer> occurrences = new HashMap<>();
        for (Integer v : vList) {
            occurrences.put(v, occurrences.getOrDefault(v, 0) + 1);
        }

        Integer highestDegV = -1;
        Integer secHighestDegV = -1;
        for (Integer v : occurrences.keySet()) {
            if (occurrences.get(v).equals(3)) {
                highestDegV = v;
            } else if (occurrences.get(v).equals(2)) {
                secHighestDegV = v;
            }
        }

        boolean highest2sec = true;
        Set<Integer> highestDegVCut = new HashSet<>();
        Set<Integer> secHighestDegVCut = new HashSet<>();
        highestDegVCut.addAll(topology.outgoing.get(highestDegV).keySet());
        secHighestDegVCut.addAll(topology.incoming.get(secHighestDegV).keySet());
        highestDegVCut.retainAll(secHighestDegVCut);
        if (highestDegVCut.isEmpty()) {
            highest2sec = false;
            highestDegVCut.addAll(topology.incoming.get(highestDegV).keySet());
            secHighestDegVCut.addAll(topology.outgoing.get(secHighestDegV).keySet());
            highestDegVCut.retainAll(secHighestDegVCut);
        }

        Integer midLabel = -1;
        for (Integer label : highestDegVCut) {
            midLabel = label;
        }

        Long count = 0L;
        Integer virtualSrc, virtualDest;
        for (Integer src : label2src2dest.get(midLabel).keySet()) {
            Long countPerSrc = 1L;
            if (highest2sec) {
                virtualSrc = highestDegV;
                virtualDest = secHighestDegV;
            } else {
                virtualSrc = secHighestDegV;
                virtualDest = highestDegV;
            }

            // calculate the src outgoing count
            for (Integer label : topology.outgoing.get(virtualSrc).keySet()) {
                if (label.equals(midLabel)) continue;
                if (!src2label2dest.containsKey(src) ||
                    !src2label2dest.get(src).containsKey(label)) {
                    countPerSrc *= 0;
                } else {
                    countPerSrc *= src2label2dest.get(src).get(label).size();
                }
            }

            // calculate the src incoming count
            for (Integer label : topology.incoming.get(virtualSrc).keySet()) {
                if (label.equals(midLabel)) continue;
                if (!dest2label2src.containsKey(src) ||
                    !dest2label2src.get(src).containsKey(label)) {
                    countPerSrc *= 0;
                } else {
                    countPerSrc *= dest2label2src.get(src).get(label).size();
                }
            }

            // skip if this starting vertex doesn't have any extensions
            if (countPerSrc == 0) continue;

            for (Integer dest : label2src2dest.get(midLabel).get(src)) {
                Long countPerBranch = countPerSrc;

                // calculate the dest outgoing count
                for (Integer label : topology.outgoing.get(virtualDest).keySet()) {
                    if (label.equals(midLabel)) continue;
                    if (!src2label2dest.containsKey(dest) ||
                        !src2label2dest.get(dest).containsKey(label)) {
                        countPerBranch *= 0;
                    } else {
                        countPerBranch *= src2label2dest.get(dest).get(label).size();
                    }
                }

                // calculate the dest incoming count
                for (Integer label : topology.incoming.get(virtualDest).keySet()) {
                    if (label.equals(midLabel)) continue;
                    if (!dest2label2src.containsKey(dest) ||
                        !dest2label2src.get(dest).containsKey(label)) {
                        countPerBranch *= 0;
                    } else {
                        countPerBranch *= dest2label2src.get(dest).get(label).size();
                    }
                }

                count += countPerBranch;
            }
        }

        return count;
    }

    private Long countSingleLabel(Integer label) {
        Long count = 0L;
        for (List<Integer> dests : label2src2dest.get(label).values()) {
            count += dests.size();
        }

        return count;
    }

    private Integer[] toLabelSeq(String labelSeqString) {
        String[] splitted = labelSeqString.split("->");
        Integer[] labelSeq = new Integer[splitted.length];
        for (int i = 0; i < splitted.length; ++i) {
            labelSeq[i] = Integer.parseInt(splitted[i]);
        }
        return labelSeq;
    }

    private Integer[] toVList(String vListString) {
        String[] splitted = vListString.split(";");
        Integer[] vList = new Integer[splitted.length * 2];
        for (int i = 0; i < splitted.length; i++) {
            String[] srcDest = splitted[i].split("-");
            vList[i * 2] = Integer.parseInt(srcDest[0]);
            vList[i * 2 + 1] = Integer.parseInt(srcDest[1]);
        }
        return vList;
    }

    private int getType(Integer[] vList) {
        if (vList.length == 2) return Constants.C_SINGLE_LABEL;

        if (vList.length == 6 || vList.length == 4) {
            return Constants.C_STAR;
        }

        Map<Integer, Integer> occurrences = new HashMap<>();
        for (Integer v : vList) {
            occurrences.put(v, occurrences.getOrDefault(v, 0) + 1);
        }

        int maxCount = Integer.MIN_VALUE;
        for (Integer count : occurrences.values()) {
            maxCount = Math.max(maxCount, count);
        }

        if (maxCount == 4) {
            return Constants.C_STAR;
        } else if (maxCount == 3) {
            return Constants.C_FORK;
        } else if (maxCount == 2) {
            return Constants.C_PATH;
        } else {
            return -1;
        }
    }

    private Integer getCenter(Integer[] vList) {
        if (vList[0].equals(vList[2]) || vList[0].equals(vList[3])) {
            return vList[0];
        }
        return vList[1];
    }

    private Topology toTopology(Integer[] labelSeq, Integer[] vList) {
        Topology topology = new Topology();
        Integer src, dest;
        for (int i = 0; i < labelSeq.length; ++i) {
            src = vList[i * 2];
            dest = vList[i * 2 + 1];
            topology.addEdge(src, labelSeq[i], dest);
        }
        return topology;
    }

    private Set<Integer> getLeaves(Integer[] vList) {
        Map<Integer, Integer> occurrences = new HashMap<>();
        for (Integer v : vList) {
            occurrences.put(v, occurrences.getOrDefault(v, 0) + 1);
        }

        Set<Integer> leaves = new HashSet<>();
        for (Integer v : occurrences.keySet()) {
            if (occurrences.get(v).equals(1)) leaves.add(v);
        }
        return leaves;
    }

    public CatalogueConstructionForLabelSeq(
        int threadId,
        int parentId,
        Map<Integer, Map<Integer, List<Integer>>> src2label2dest,
        Map<Integer, Map<Integer, List<Integer>>> dest2label2src,
        Map<Integer, Map<Integer, List<Integer>>> label2src2dest,
        Map<Integer, Map<Integer, List<Integer>>> label2dest2src,
        String line) {

        this.threadId = threadId;
        this.parentId = parentId;
        this.src2label2dest = src2label2dest;
        this.dest2label2src = dest2label2src;
        this.label2src2dest = label2src2dest;
        this.label2dest2src = label2dest2src;
        this.line = line;
    }
}
