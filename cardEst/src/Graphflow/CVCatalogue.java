package Graphflow;

import Common.Pair;
import Common.Query;
import Common.Topology;
import Graphflow.Parallel.Util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;

public class CVCatalogue {
    // query topology -> decomVListString (defining decom topology) -> edge label seq -> count
    private List<Map<String, Map<String, Long>>> catalogue = new ArrayList<>();

    // base dirs -> base edge label seq -> ext dir -> ext label -> cv
    private Map<String, Map<String, Map<String, Map<String, Double>>>> starCV = new HashMap<>();
    private Map<String, Map<String, Map<String, Map<String, Double>>>> forkCV = new HashMap<>();
    private Map<String, Map<String, Map<String, Map<String, Double>>>> pathCV = new HashMap<>();

    // patternType -> base/overlap vList -> base/overlap labelSeq -> extVList -> extLabel -> cv/ent
    Map<Integer, Map<String, Map<String, Map<String, Map<String, Double>>>>> cvCache = new HashMap<>();

    public CVCatalogue(String catalogueFile, String starCVFiles, String forkCVFiles,
        String pathCVFiles) throws Exception {

        load(catalogueFile);
        if (starCVFiles != null) readCVFiles(starCVFiles, starCV);
        if (forkCVFiles != null) readCVFiles(forkCVFiles, forkCV);
        if (pathCVFiles != null) readCVFiles(pathCVFiles, pathCV);
    }

    public CVCatalogue(String catalogueFile, String entropyFile) throws Exception {
        load(catalogueFile);
        loadEntropy(entropyFile);
    }

    private void loadEntropy(String entropyFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        String[] info;

        BufferedReader reader = new BufferedReader(new FileReader(entropyFile));
        String line = reader.readLine();
        while (line != null) {
            info = line.split(",");
            Integer patternType = Integer.parseInt(info[0]);
            String baseVList = info[1];
            String baseLabelSeq = info[2];
            String extVList = info[3];
            String extLabel = info[4];
            Double entropy = Double.parseDouble(info[5]);

            cvCache.putIfAbsent(patternType, new HashMap<>());
            cvCache.get(patternType).putIfAbsent(baseVList, new HashMap<>());
            cvCache.get(patternType).get(baseVList).putIfAbsent(baseLabelSeq, new HashMap<>());
            cvCache.get(patternType).get(baseVList).get(baseLabelSeq)
                .putIfAbsent(extVList, new HashMap<>());
            cvCache.get(patternType).get(baseVList).get(baseLabelSeq).get(extVList)
                .put(extLabel, entropy);

            line = reader.readLine();
        }
        reader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Loading Entropy: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void readCVFiles(
        String cvFiles,
        Map<String, Map<String, Map<String, Map<String, Double>>>> cv) throws Exception {

        String line;
        String[] info, baseDirsAndEdges, extDirsAndEdges;

        String[] starFiles = cvFiles.split(",");
        for (String fileName : starFiles) {
            long startTime = System.currentTimeMillis();

            BufferedReader reader = new BufferedReader(new FileReader(fileName));
            line = reader.readLine();
            while (null != line) {
                info = line.split(",");
                baseDirsAndEdges = info[0].split(";");
                extDirsAndEdges = info[1].split(";");

                cv.putIfAbsent(baseDirsAndEdges[0], new HashMap<>());
                cv.get(baseDirsAndEdges[0]).putIfAbsent(baseDirsAndEdges[1], new HashMap<>());
                cv.get(baseDirsAndEdges[0])
                    .get(baseDirsAndEdges[1])
                    .putIfAbsent(extDirsAndEdges[0], new HashMap<>());
                cv.get(baseDirsAndEdges[0])
                    .get(baseDirsAndEdges[1])
                    .get(extDirsAndEdges[0])
                    .put(extDirsAndEdges[1], Double.parseDouble(info[info.length - 1]));

                line = reader.readLine();
            }

            long endTime = System.currentTimeMillis();
            System.out.println("Loading " + fileName + ": " +
                    ((endTime - startTime) / 1000.0) + " sec");
        }
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

    public void load(String catalogueFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader catalogueReader = new BufferedReader(new FileReader(catalogueFile));
        String[] info;
        String vList, labelSeq;
        String line = catalogueReader.readLine();
        while (null != line) {
            info = line.split(",");
            int queryType = Integer.parseInt(info[0]);
            while (catalogue.size() <= queryType) {
                catalogue.add(new HashMap<>());
            }

            vList = info[1];
            labelSeq = info[2];
            Long count = Long.parseLong(info[3]);
            catalogue.get(queryType).putIfAbsent(vList, new HashMap<>());
            catalogue.get(queryType).get(vList).put(labelSeq, count);

            line = catalogueReader.readLine();
        }
        catalogueReader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Loading Catalogue: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private String extractPath(Topology topology, Integer[] vertexList) {
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

    private String toVListString(Integer[] vList) {
        StringJoiner sj = new StringJoiner(";");
        for (int i = 0; i < vList.length; i += 2) {
            sj.add(vList[i] + "-" + vList[i + 1]);
        }
        return sj.toString();
    }

    private Set<Integer> toLabelSet(String labelSeq) {
        Set<Integer> labelSet = new HashSet<>();
        String[] splitted = labelSeq.split("->");
        for (String label : splitted) {
            labelSet.add(Integer.parseInt(label));
        }
        return labelSet;
    }

    private Set<Integer> getAllLabels(Topology topology) {
        Set<Integer> allLabels = new HashSet<>();
        for (Map<Integer, List<Integer>> label2dests : topology.outgoing) {
            allLabels.addAll(label2dests.keySet());
        }
        return allLabels;
    }

    private Pair<Integer, Set<String>> getOverlappingType(String vList, String extendVList) throws Exception {
        Set<String> intersection = new HashSet<>();
        String[] vListSplitted = vList.split(";");
        String[] extendVListSplitted = extendVList.split(";");

        intersection.addAll(Arrays.asList(vListSplitted));
        intersection.retainAll(Arrays.asList(extendVListSplitted));

        Map<Integer, Integer> degree = new HashMap<>();
        Integer src, dest;
        Integer maxDeg = Integer.MIN_VALUE;
        for (String srcDestString : intersection) {
            String[] srcDest = srcDestString.split("-");
            src = Integer.parseInt(srcDest[0]);
            dest = Integer.parseInt(srcDest[1]);
            degree.put(src, degree.getOrDefault(src, 0) + 1);
            degree.put(dest, degree.getOrDefault(dest, 0) + 1);
            maxDeg = Math.max(degree.get(src), maxDeg);
            maxDeg = Math.max(degree.get(dest), maxDeg);
        }

        if (maxDeg.equals(3)) {
            return new Pair<>(Constants.C_STAR, intersection);
        } else if (maxDeg.equals(2)) {
            return new Pair<>(Constants.C_PATH, intersection);
        } else {
            throw new Exception("ERROR: unrecognized overlapping pattern");
        }
    }

    private int getType(Integer[] vList) {
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

    /**
     *
     * @param covered
     * @param labelSeq
     * @param vList
     * @return (label_seq, v_list_seq)
     */
    private Pair<String, String> getOverlap(Set<Integer> covered, String labelSeq, String vList) {
        String[] labelSeqSplitted = labelSeq.split("->");
        String[] vListSplitted = vList.split(";");
        StringJoiner labelSj = new StringJoiner("->");
        StringJoiner vListSj = new StringJoiner(";");

        for (int i = 0; i < labelSeqSplitted.length; ++i) {
            if (covered.contains(Integer.parseInt(labelSeqSplitted[i]))) {
                labelSj.add(labelSeqSplitted[i]);
                vListSj.add(vListSplitted[i]);
            }
        }

        return new Pair<>(labelSj.toString(), vListSj.toString());
    }

    private Integer[][] getStartingDecoms(Integer patternType) {
        Random random = new Random(0);

        final Integer[][] INITIALS = new Integer[][] {
            // fork24 forks
//            {0, 1, 0, 4, 3, 4, 4, 5},
//            {0, 1, 0, 4, 3, 4, 4, 6},
//            {0, 1, 0, 4, 2, 4, 3, 4},
//            {0, 1, 0, 4, 4, 5, 4, 6},
//            {0, 1, 0, 4, 2, 4, 4, 5},
//            {0, 1, 0, 4, 2, 4, 4, 6}
            // pi paths
//            {0, 1, 0, 3, 3, 6, 4, 6},
//            {0, 2, 0, 3, 3, 6, 4, 6},
//            {0, 1, 0, 3, 3, 6, 5, 6},
//            {0, 2, 0, 3, 3, 6, 5, 6},
            // fork33 paths
//            {0, 1, 1, 2, 2, 5, 3, 5},
//            {0, 1, 1, 2, 2, 5, 4, 5},
//            {0, 1, 1, 2, 2, 5, 5, 6},
        };

//        return new Integer[][]{INITIALS[random.nextInt(INITIALS.length)]};
//        return INITIALS;
        return Constants.DECOMPOSITIONS4[patternType];

        // TODO: based on the patternType, return different set of patterns
    }

    private Integer[][] getExtendVLists(Integer patternType, boolean onlyLen4) {
        if (onlyLen4) return Constants.DECOMPOSITIONS4[patternType];

        int totalLen = Constants.DECOMPOSITIONS4[patternType].length +
            Constants.DECOMPOSITIONS3[patternType].length;
        Integer[][] extendVList = new Integer[totalLen][];
        int i = 0;

        for (int j = 0; j < Constants.DECOMPOSITIONS4[patternType].length; ++j) {
            extendVList[i] = Constants.DECOMPOSITIONS4[patternType][j];
            i++;
        }
        for (int j = 0; j < Constants.DECOMPOSITIONS3[patternType].length; ++j) {
            extendVList[i] = Constants.DECOMPOSITIONS3[patternType][j];
            i++;
        }

        return extendVList;
    }

    /**
     *
     * @param overlapLabels
     * @param overlapVList
     * @return ext dir -> ext label -> cv
     */
    private Map<String, Map<String, Double>> getExt2CVMappingFromOverlapVList(
        String overlapLabels, String overlapVList, int pattern, Map<String, String> queryEdge2dir) {

        String[] srcDestStrings = overlapVList.split(";");
        String[] labels = overlapLabels.split("->");

        Map<Integer, Integer> degree = new HashMap<>();
        Integer src, dest;
        Integer maxDeg = Integer.MIN_VALUE;
        for (String srcDestString : srcDestStrings) {
            String[] srcDest = srcDestString.split("-");
            src = Integer.parseInt(srcDest[0]);
            dest = Integer.parseInt(srcDest[1]);
            degree.put(src, degree.getOrDefault(src, 0) + 1);
            degree.put(dest, degree.getOrDefault(dest, 0) + 1);
            maxDeg = Math.max(degree.get(src), maxDeg);
            maxDeg = Math.max(degree.get(dest), maxDeg);
        }

        overlapLabels = overlapLabels.replaceAll("->", "-");
        if (maxDeg == 3) { // star
            TreeMap<Integer, Integer> sortedMap = new TreeMap<>();
            for (int i = 0; i < labels.length; ++i) {
                sortedMap.put(Integer.parseInt(labels[i]),
                              Integer.parseInt(Constants.forkEdge2dir.get(srcDestStrings[i])));
            }
            String dirLabelSeq = Util.sortEdgeDirByEdge(sortedMap);
            String[] dirsAndLabels = dirLabelSeq.split(";");
            return starCV.get(dirsAndLabels[0]).get(dirsAndLabels[1]);
        } else if (pattern == Constants.C_FORK) { // base of fork
            return forkCV.get(queryEdge2dir.get(overlapVList)).get(overlapLabels);
        } else if (pattern == Constants.C_PATH) { // base of path
            return pathCV.get(queryEdge2dir.get(overlapVList)).get(overlapLabels);
        } else {
            System.out.println("overlapLabels: " + overlapLabels + ", overlapVList: " + overlapVList);
            return null;
        }
    }

    private Pair<String, String> getExtDirAndExtLabel(String labelSeq, String vListString,
        Set<Integer> extension) {

        // only one ext
        for (Integer ext : extension) {
            String[] labels = labelSeq.split("->");
            String[] srcDests = vListString.split(";");
            for (int i = 0; i < labels.length; ++i) {
                if (ext.toString().equals(labels[i])) {
                    return new Pair<>(Constants.forkEdge2dir.get(srcDests[i]), ext.toString());
                }
            }
        }

        System.out.println("ERROR: ext is not one of the labels");
        return null;
    }

    /**
     *
     * @param topology
     * @param coveredLabels
     * @param extendVLists
     * @return ((nextLabels, nextVList), (overlapLabels, overlapVList))
     */
    private Pair<Pair<String, String>, Pair<String, String>> getMinCV(Topology topology,
        Set<Integer> coveredLabels, Integer[][] extendVLists, int queryPattern) {

        Map<String, String> queryEdge2dir = null;
        if (queryPattern == 3) { // fork query
            queryEdge2dir = Constants.forkEdge2dir;
        } else if (queryPattern == 4) { // pi query
            queryEdge2dir = Constants.piEdge2dir;
        }

        Set<Integer> intersection = new HashSet<>();
        Set<Integer> extension = new HashSet<>();

        Double minCV = Double.MAX_VALUE;
        String nextLabelsWithMinCV = null;
        String nextVListWithMinCV = null;
        Pair<String, String> overlapWithMinCV = null;
        for (Integer[] extendVList : extendVLists) {
            String nextLabelSeq = extractPath(topology, extendVList);
            Set<Integer> nextLabelSet = toLabelSet(nextLabelSeq);
            String extendVListString = toVListString(extendVList);
            int pattern = getType(extendVList);

            intersection.clear();
            intersection.addAll(coveredLabels);
            intersection.retainAll(nextLabelSet);
            if (intersection.size() != nextLabelSet.size() - 1) continue;

            extension.clear();
            extension.addAll(nextLabelSet);
            extension.removeAll(intersection);
            if (extension.size() != 1) {
                System.out.println("ERROR: extension != 1");
            }

            Pair<String, String> overlap = getOverlap(coveredLabels, nextLabelSeq, extendVListString);
            Map<String, Map<String, Double>> extDir2extLabel2cv =
                getExt2CVMappingFromOverlapVList(overlap.key, overlap.value, pattern, queryEdge2dir);
            Pair<String, String> extDirAndExtLabel = getExtDirAndExtLabel(nextLabelSeq,
                extendVListString, extension);
//            System.out.println(extDirAndExtLabel);
//            System.out.println(extDir2extLabel2cv.get(extDirAndExtLabel.key));
            if (extDir2extLabel2cv == null ||
                extDir2extLabel2cv.get(extDirAndExtLabel.key) == null) {
                System.out.println("ERROR: ");
                System.out.println("  topology: " + topology);
                System.out.println("  coveredLabels: " + coveredLabels);
                System.out.println("  nextLabelSeq: " + nextLabelSeq);
                System.out.println("  extendVListString: " + extendVListString);
                System.out.println("  overlap: " + overlap);
            }
            Double cv = extDir2extLabel2cv.get(extDirAndExtLabel.key).get(extDirAndExtLabel.value);

            if (cv < minCV) {
                minCV = cv;
                nextLabelsWithMinCV = nextLabelSeq;
                nextVListWithMinCV = extendVListString;
                overlapWithMinCV = overlap;
            }
        }

        return new Pair<>(new Pair<>(nextLabelsWithMinCV, nextVListWithMinCV), overlapWithMinCV);
    }

    private Pair<String, String> getExt(String labelSeq, String vListString, String overlapLabelSeq) {
        Set<String> overlapLabelSet = new HashSet<>(Arrays.asList(overlapLabelSeq.split("->")));
        String[] labelSeqList = labelSeq.split("->");
        String[] vList = vListString.split(";");

        for (int i = 0; i < labelSeqList.length; ++i) {
            if (!overlapLabelSet.contains(labelSeqList[i])) {
                return new Pair<>(labelSeqList[i], vList[i]);
            }
        }

        return null;
    }

    public Pair<Double, Double> estimate(Query query, Integer patternType, int formulaType)
        throws Exception {

        List<Pair<Set<Integer>, Pair<Double, Double>>> alreadyCovered = new ArrayList<>();
        // ({covered_labels}, (cv/entropy, cardinality))
        Set<Pair<Set<Integer>, Pair<Double, Double>>> currentCoveredLabelsAndCard = new HashSet<>();
        Set<Pair<Set<Integer>, Pair<Double, Double>>> nextCoveredLabelsAndCard = new HashSet<>();

        Set<Integer> allLabels = getAllLabels(query.topology);

        String startLabelSeq, nextLabelSeq, vListString, extendVListString;
        Pair<String, String> overlap, ext;
        Set<Integer> nextLabelSet, nextCovered;
        Set<Integer> intersection = new HashSet<>();

        Integer[][] startingDecoms = getStartingDecoms(patternType);

        for (Integer[] vList : startingDecoms) {
            startLabelSeq = extractPath(query.topology, vList);
            vListString = toVListString(vList);
            double est = catalogue.get(patternType).get(vListString).get(startLabelSeq);
            double cv = 0.0;

            currentCoveredLabelsAndCard.clear();
            currentCoveredLabelsAndCard.add(
                new Pair<>(toLabelSet(startLabelSeq), new Pair<>(cv, est)));

            for (int i = 0; i < allLabels.size() - vList.length / 2; ++i) {
                for (Integer[] extendVList : Constants.DECOMPOSITIONS4[patternType]) {
                    nextLabelSeq = extractPath(query.topology, extendVList);
                    nextLabelSet = toLabelSet(nextLabelSeq);
                    extendVListString = toVListString(extendVList);

                    if (!Common.Util.doesEntryFitFormula(getType(extendVList), formulaType)) continue;

                    for (Pair<Set<Integer>, Pair<Double, Double>> coveredAndCard : currentCoveredLabelsAndCard) {
                        if (coveredAndCard.key.equals(allLabels)) {
                            alreadyCovered.add(coveredAndCard);
                            continue;
                        } else {
                            nextCoveredLabelsAndCard.add(coveredAndCard);
                        }

                        intersection.clear();
                        intersection.addAll(coveredAndCard.key);
                        intersection.retainAll(nextLabelSet);
                        if (intersection.size() != nextLabelSet.size() - 1) continue;

                        overlap = getOverlap(coveredAndCard.key, nextLabelSeq, extendVListString);
                        est = coveredAndCard.value.value;
                        est /= catalogue.get(patternType).get(overlap.value).get(overlap.key);
                        est *= catalogue.get(patternType).get(extendVListString).get(nextLabelSeq);

                        ext = getExt(nextLabelSeq, extendVListString, overlap.key);
                        cv = coveredAndCard.value.key;
                        cv += cvCache.get(patternType).get(overlap.value).get(overlap.key)
                            .get(ext.value).get(ext.key);

                        nextCovered = new HashSet<>(coveredAndCard.key);
                        nextCovered.addAll(nextLabelSet);
                        nextCoveredLabelsAndCard.add(new Pair<>(nextCovered, new Pair<>(cv, est)));
                    }

                    currentCoveredLabelsAndCard = nextCoveredLabelsAndCard;
                    nextCoveredLabelsAndCard = new HashSet<>();
                }
            }

            alreadyCovered.addAll(currentCoveredLabelsAndCard);
        }

        Pair<Double, Double> minCvAndEst = new Pair<>(Double.MAX_VALUE, -1.0);
        for (Pair<Set<Integer>, Pair<Double, Double>> coveredAndEst : alreadyCovered) {
            if (coveredAndEst.key.equals(allLabels)) {
                if (coveredAndEst.value.key < minCvAndEst.key) {
                    minCvAndEst = coveredAndEst.value;
                }
            }
        }

        return minCvAndEst;
    }
}
