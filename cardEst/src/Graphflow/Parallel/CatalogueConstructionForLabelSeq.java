package Graphflow.Parallel;

import Common.Topology;
import Graphflow.Constants;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.*;

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

    // function calculating x choose y
    public static Long choose(Long x, Long y) {
        if (y < 0 || y > x) return 0L;
        if (y > x/2) {
            // choose(n,k) == choose(n,n-k),
            // so this could save a little effort
            y = x - y;
        }

        Long answer = 1L;
        for (int i = 1; i <= y; i++) {
            answer /= i;
            answer *= (x + 1 - i);
        }
        return answer;
    }

    private Long count(String vListString, String labelSeqString) throws Exception {
        Integer[] labelSeq = toLabelSeq(labelSeqString);
        Integer[] vList = toVList(vListString);
        int type = getType(vList);
        Integer starCenter;
        if (type == Constants.C_FORK) {
            return countFork(labelSeq, vList);
        } else if (type == Constants.C_STAR) {
            starCenter = getCenter(vList);
        } else if (type == Constants.C_PATH) {
            return countPath(vListString, labelSeqString);
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

        // ---------- Account for repeated edge labels ----------
        // 0 means forward (src is the starCenter)
        // 1 means backward (dest is the starCenter)
        // edgeLabel -> direction -> vertices
        /*
        Map<Integer, Map<Integer, List<Integer>>> combinedLeaves = new HashMap<>();
        for (int i = 0; i < labelSeq.length; ++i) {
            Integer curSrc = vList[i * 2];
            Integer curDest = vList[i * 2 + 1];
            if (curSrc.intValue() == starCenter.intValue()) {
                combinedLeaves.putIfAbsent(labelSeq[i], new HashMap<>());
                combinedLeaves.get(labelSeq[i]).putIfAbsent(0, new ArrayList<>());
                combinedLeaves.get(labelSeq[i]).get(0).add(curDest);
            } else {
                combinedLeaves.putIfAbsent(labelSeq[i], new HashMap<>());
                combinedLeaves.get(labelSeq[i]).putIfAbsent(1, new ArrayList<>());
                combinedLeaves.get(labelSeq[i]).get(1).add(curSrc);
            }
        }
        */

        Long count = 0L;
        for (Integer origin : listSrcDest.get(labelSeq[0]).keySet()) {
            Map<Integer, List<Integer>> virtual2physicals = new HashMap<>();

            virtual2physicals.putIfAbsent(starCenter, new ArrayList<>());
            virtual2physicals.get(starCenter).add(origin);
            virtual2physicals.putIfAbsent(startDest, new ArrayList<>());
            virtual2physicals.get(startDest).addAll(listSrcDest.get(labelSeq[0]).get(origin));
            // ----------- debug -----------
            // if (labelSeq[0] == 2 && labelSeq[1] == 2) {
            //     for (Integer vertex : listSrcDest.get(2).keySet()) {
            //         if (listSrcDest.get(2).get(vertex).size() >= 2) System.out.println("Here3");
            //     }
            // }

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

            /* no longer need this: different vertex id in query can refer to the same vertex in graph
            boolean breakloop = false;
            for (Integer leafLabel : combinedLeaves.keySet()) {
                for (Integer direction : combinedLeaves.get(leafLabel).keySet()) {
                    Integer leaf = combinedLeaves.get(leafLabel).get(direction).get(0);
                    if (!virtual2physicals.containsKey(leaf) || virtual2physicals.get(leaf).isEmpty()) {
                        countForOrigin = 0L;
                        breakloop = true;
                        break;
                    }
                    int var1 = virtual2physicals.get(leaf).size();
                    int var2 = combinedLeaves.get(leafLabel).get(direction).size();
                    Long param1 = Long.valueOf(var1);
                    Long param2 = Long.valueOf(var2);
                    countForOrigin *= choose(param1, param2);
                }
                if (breakloop) break;
            }
            */

            count += countForOrigin;
        }

        return count;
    }

    private Long countPath(String vListString, String labelSeqString) throws Exception {
        // when the pattern type is a path (only need length = 3 here)
        // note that length = 2 path is counted as a Star
        Integer[] labelSeq = toLabelSeq(labelSeqString);
        Integer[] vList = toVList(vListString);
        // make sure vList and labelSeq are in order
        if (vList[0].intValue() == vList[2].intValue() || vList[0].intValue() == vList[3].intValue() || vList[1].intValue() == vList[2].intValue() || vList[1].intValue() == vList[3].intValue()) {
            if (vList[2].intValue() == vList[4].intValue() || vList[2].intValue() == vList[5].intValue() || vList[3].intValue() == vList[4].intValue() || vList[3].intValue() == vList[5].intValue()) {
            } else {
                // swap vertices
                Integer temp1 = vList[0];
                Integer temp2 = vList[1];
                vList[0] = vList[2];
                vList[1] = vList[3];
                vList[2] = temp1;
                vList[3] = temp2;
                // swap edges
                Integer temp3 = labelSeq[0];
                labelSeq[0] = labelSeq[1];
                labelSeq[1] = temp3;
            }
        } else {
            // swap vertices
            Integer temp1 = vList[2];
            Integer temp2 = vList[3];
            vList[2] = vList[4];
            vList[3] = vList[5];
            vList[4] = temp1;
            vList[5] = temp2;
            // swap edges
            Integer temp3 = labelSeq[1];
            labelSeq[1] = labelSeq[2];
            labelSeq[2] = temp3;
        }

        Map<Integer, Map<Integer, List<Integer>>> listSrcDest;
        Map<Integer, Map<Integer, List<Integer>>> listDestSrc;
        listSrcDest = label2src2dest;
        listDestSrc = label2dest2src;

        // different cases for 3-path

        Long count = 0L;
        // v1 <- v0 -> v2 -> v3
        if (vList[0].intValue() == vList[2].intValue() && vList[3].intValue() == vList[4].intValue()) {
            for (Integer v0 : listSrcDest.get(labelSeq[0]).keySet()) {
                for (Integer v1 : listSrcDest.get(labelSeq[0]).get(v0)) {
                    if (listSrcDest.get(labelSeq[1]).containsKey(v0)) {
                        for (Integer v2 : listSrcDest.get(labelSeq[1]).get(v0)) {
                            if (listSrcDest.get(labelSeq[2]).containsKey(v2)) {
                                count += listSrcDest.get(labelSeq[2]).get(v2).size();
                                if (count == 9223372036854775807L) return count;
                                // for (Integer v3 : listSrcDest.get(labelSeq[2]).get(v2)) {
                                //     count += 1L;
                                // }
                            }
                        }
                    }
                }
            }
        }
        // v1 <- v0 -> v2 <- v3
        else if (vList[0].intValue() == vList[2].intValue() && vList[3].intValue() == vList[5].intValue()) {
            for (Integer v0 : listSrcDest.get(labelSeq[0]).keySet()) {
                for (Integer v1 : listSrcDest.get(labelSeq[0]).get(v0)) {
                    if (listSrcDest.get(labelSeq[1]).containsKey(v0)) {
                        for (Integer v2 : listSrcDest.get(labelSeq[1]).get(v0)) {
                            if (listDestSrc.get(labelSeq[2]).containsKey(v2)) {
                                count += listDestSrc.get(labelSeq[2]).get(v2).size();
                                if (count == 9223372036854775807L) return count;
                                // for (Integer v3 : listDestSrc.get(labelSeq[2]).get(v2)) {
                                //     count += 1L;
                                // }
                            }
                        }
                    }
                }
            }
        }
        // v0 -> v1 -> v2 -> v3
        else if (vList[1].intValue() == vList[2].intValue() && vList[3].intValue() == vList[4].intValue()) {
            for (Integer v0 : listSrcDest.get(labelSeq[0]).keySet()) {
                for (Integer v1 : listSrcDest.get(labelSeq[0]).get(v0)) {
                    if (listSrcDest.get(labelSeq[1]).containsKey(v1)) {
                        for (Integer v2 : listSrcDest.get(labelSeq[1]).get(v1)) {
                            if (listSrcDest.get(labelSeq[2]).containsKey(v2)) {
                                count += listSrcDest.get(labelSeq[2]).get(v2).size();
                                if (count == 9223372036854775807L) return count;
                                // for (Integer v3 : listSrcDest.get(labelSeq[2]).get(v2)) {
                                //     count += 1L;
                                // }
                            }
                        }
                    }
                }
            }
        }
        // v0 -> v1 -> v2 <- v3
        else if (vList[1].intValue() == vList[2].intValue() && vList[3].intValue() == vList[5].intValue()) {
            for (Integer v0 : listSrcDest.get(labelSeq[0]).keySet()) {
                for (Integer v1 : listSrcDest.get(labelSeq[0]).get(v0)) {
                    if (listSrcDest.get(labelSeq[1]).containsKey(v1)) {
                        for (Integer v2 : listSrcDest.get(labelSeq[1]).get(v1)) {
                            if (listDestSrc.get(labelSeq[2]).containsKey(v2)) {
                                count += listDestSrc.get(labelSeq[2]).get(v2).size();
                                if (count == 9223372036854775807L) return count;
                                // for (Integer v3 : listDestSrc.get(labelSeq[2]).get(v2)) {
                                //     count += 1L;
                                // }
                            }
                        }
                    }
                }
            }
        }
        // v0 -> v1 <- v2 -> v3
        else if (vList[1].intValue() == vList[3].intValue() && vList[2].intValue() == vList[4].intValue()) {
            for (Integer v0 : listSrcDest.get(labelSeq[0]).keySet()) {
                for (Integer v1 : listSrcDest.get(labelSeq[0]).get(v0)) {
                    if (listDestSrc.get(labelSeq[1]).containsKey(v1)) {
                        for (Integer v2 : listDestSrc.get(labelSeq[1]).get(v1)) {
                            if (listSrcDest.get(labelSeq[2]).containsKey(v2)) {
                                count += listSrcDest.get(labelSeq[2]).get(v2).size();
                                if (count == 9223372036854775807L) return count;
                                // for (Integer v3 : listSrcDest.get(labelSeq[2]).get(v2)) {
                                //     count += 1L;
                                // }
                            }
                        }
                    }
                }
            }
        }
        // v0 -> v1 <- v2 <- v3
        else if (vList[1].intValue() == vList[3].intValue() && vList[2].intValue() == vList[5].intValue()) {
            for (Integer v0 : listSrcDest.get(labelSeq[0]).keySet()) {
                for (Integer v1 : listSrcDest.get(labelSeq[0]).get(v0)) {
                    if (listDestSrc.get(labelSeq[1]).containsKey(v1)) {
                        for (Integer v2 : listDestSrc.get(labelSeq[1]).get(v1)) {
                            if (listDestSrc.get(labelSeq[2]).containsKey(v2)) {
                                count += listDestSrc.get(labelSeq[2]).get(v2).size();
                                if (count == 9223372036854775807L) return count;
                                // for (Integer v3 : listDestSrc.get(labelSeq[2]).get(v2)) {
                                //     count += 1L;
                                // }
                            }
                        }
                    }
                }
            }
        }
        // v1 <- v0 <- v2 -> v3
        else if (vList[0].intValue() == vList[3].intValue() && vList[2].intValue() == vList[4].intValue()) {
            for (Integer v0 : listSrcDest.get(labelSeq[0]).keySet()) {
                for (Integer v1 : listSrcDest.get(labelSeq[0]).get(v0)) {
                    if (listDestSrc.get(labelSeq[1]).containsKey(v0)) {
                        for (Integer v2 : listDestSrc.get(labelSeq[1]).get(v0)) {
                            if (listSrcDest.get(labelSeq[2]).containsKey(v2)) {
                                count += listSrcDest.get(labelSeq[2]).get(v2).size();
                                if (count == 9223372036854775807L) return count;
                                // for (Integer v3 : listSrcDest.get(labelSeq[2]).get(v2)) {
                                //     count += 1L;
                                // }
                            }
                        }
                    }
                }
            }
        }
        // v1 <- v0 <- v2 <- v3
        else if (vList[0].intValue() == vList[3].intValue() && vList[2].intValue() == vList[5].intValue()) {
            for (Integer v0 : listSrcDest.get(labelSeq[0]).keySet()) {
                for (Integer v1 : listSrcDest.get(labelSeq[0]).get(v0)) {
                    if (listDestSrc.get(labelSeq[1]).containsKey(v0)) {
                        for (Integer v2 : listDestSrc.get(labelSeq[1]).get(v0)) {
                            if (listDestSrc.get(labelSeq[2]).containsKey(v2)) {
                                count += listDestSrc.get(labelSeq[2]).get(v2).size();
                                if (count == 9223372036854775807L) return count;
                                // for (Integer v3 : listDestSrc.get(labelSeq[2]).get(v2)) {
                                //     count += 1L;
                                // }
                            }
                        }
                    }
                }
            }
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

        // if (vList.length == 6 || vList.length == 4) {
        //     return Constants.C_STAR;
        // }

        Map<Integer, Integer> occurrences = new HashMap<>();
        for (Integer v : vList) {
            occurrences.put(v, occurrences.getOrDefault(v, 0) + 1);
        }

        // if all except one vertex has degree 1, this is a STAR

        Integer notOne = 0;
        for (Integer vertex : occurrences.keySet()) {
            if (occurrences.get(vertex) != 1) ++notOne;
        }

        if (notOne == 1) {
            return Constants.C_STAR;
        }

        int maxCount = Integer.MIN_VALUE;
        for (Integer count : occurrences.values()) {
            maxCount = Math.max(maxCount, count);
        }

        // if maximum degree is 2, this is a PATH/CHAIN
        if (maxCount == 2) {
            return Constants.C_PATH;
        } else {
            return Constants.C_FORK;
        }

        // if (maxCount == 4) {
        //     return Constants.C_STAR;
        // } else if (maxCount == 3) {
        //     return Constants.C_FORK;
        // } else if (maxCount == 2) {
        //     return Constants.C_PATH;
        // } else {
        //     return -1;
        // }
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