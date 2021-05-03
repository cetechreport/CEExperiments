package Graphflow;
import CharacteristicSet.CharacteristicSets;
import Common.Pair;
import Common.Query;
import Common.QueryDecomposer;
import Common.Topology;
import Common.Triple;
import Common.Util;
import Graphflow.Parallel.CatalogueConstructionPerVList;
import IMDB.Labels;
import MarkovTable.PropertyFilter.MT;
import org.apache.commons.lang3.StringUtils;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static java.lang.Math.*;


public class Catalogue {
    private Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    private Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();
    private Map<Integer, Map<Integer, List<Integer>>> label2src2dest = new HashMap<>();
    private Map<Integer, Map<Integer, List<Integer>>> label2dest2src = new HashMap<>();
    public Map<Integer, Integer> vid2prodYear = new HashMap<>();

    // query topology -> decomVListString (defining decom topology) -> edge label seq -> count
    public static List<Map<String, Map<String, Long>>> catalogue = new ArrayList<>();

    private CharacteristicSets characteristicSets;
    private MT mt;

    public static Integer[][] decoms = null;

    private int subsetSize = 500000;

    public Catalogue(String graphFile, String destFile) throws Exception {
        readGraph(graphFile);
//        readProperties(propFile);
        parallelConstruct();
//        construct();
//        saveCatalogue();
        collect(destFile);
    }

    public Catalogue(String catalogueFile, int maxLen) throws Exception {
        load(catalogueFile, maxLen);
//        readProperties(propFile);
//        characteristicSets = new CharacteristicSets(graphFile, propFile, "load");
//        mt = new MT(mtFileString.split(","), sampleFileString.split(","), propFile);
    }

    public Catalogue(List<Map<String, Map<String, Long>>> catalogue) {
        this.catalogue = catalogue;
    }

    private void readGraph(String graphFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader csvReader = new BufferedReader(new FileReader(graphFile));
        int[] line;
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            line = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt).toArray();

            src2label2dest.putIfAbsent(line[0], new HashMap<>());
            src2label2dest.get(line[0]).putIfAbsent(line[1], new ArrayList<>());
            src2label2dest.get(line[0]).get(line[1]).add(line[2]);

            dest2label2src.putIfAbsent(line[2], new HashMap<>());
            dest2label2src.get(line[2]).putIfAbsent(line[1], new ArrayList<>());
            dest2label2src.get(line[2]).get(line[1]).add(line[0]);

            label2src2dest.putIfAbsent(line[1], new HashMap<>());
            label2src2dest.get(line[1]).putIfAbsent(line[0], new ArrayList<>());
            label2src2dest.get(line[1]).get(line[0]).add(line[2]);

            label2dest2src.putIfAbsent(line[1], new HashMap<>());
            label2dest2src.get(line[1]).putIfAbsent(line[2], new ArrayList<>());
            label2dest2src.get(line[1]).get(line[2]).add(line[0]);

            tripleString = csvReader.readLine();
        }

        endTime = System.currentTimeMillis();
        System.out.println("Graph Loading: " + ((endTime - startTime) / 1000.0) + " sec");

        csvReader.close();
    }

    private void getPartFiles(List<String> files) {
        try (Stream<Path> paths = Files.walk(Paths.get(System.getProperty("user.dir")))) {
            paths.forEach(filePath -> {
                if (Files.isRegularFile(filePath)) {
                    String filePathString = filePath.toString();
                    if (filePathString.contains("catalogue_part_")) {
                        files.add(filePathString);
                    }
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

        Collections.sort(files);
    }

    private void collect(String destFile) throws Exception {
        List<String> catPartFiles = new ArrayList<>();
        getPartFiles(catPartFiles);
        BufferedWriter writer = new BufferedWriter(new FileWriter(destFile));
        for (String partFile : catPartFiles) {
            BufferedReader reader = new BufferedReader(new FileReader(partFile));
            String line = reader.readLine();
            while (line != null) {
                writer.write(line + "\n");
                line = reader.readLine();
            }
            reader.close();

            File file = new File(partFile);
            if (!file.delete()) {
                System.err.println("ERROR: cannot find file to delete");
                System.err.println("   " + partFile);
                return;
            }
        }
        writer.close();

        File file = new File("decom.csv");
        if (!file.delete()) {
            System.err.println("ERROR: cannot find decom.csv to delete");
        }
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

    public static int getType(Integer[] vList) {
        if (vList.length == 6 || vList.length == 4) {
            return Constants.C_STAR;
        }

        Map<Integer, Integer> occrrences = new HashMap<>();
        for (Integer v : vList) {
            occrrences.put(v, occrrences.getOrDefault(v, 0) + 1);
        }

        int maxCount = Integer.MIN_VALUE;
        for (Integer count : occrrences.values()) {
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

    private Long countFork(Integer[] labelSeq, Integer[] vList) {
        Topology topology = toTopology(labelSeq, vList);

        Map<Integer, Integer> occrrences = new HashMap<>();
        for (Integer v : vList) {
            occrrences.put(v, occrrences.getOrDefault(v, 0) + 1);
        }

        Integer highestDegV = -1;
        Integer secHighestDegV = -1;
        for (Integer v : occrrences.keySet()) {
            if (occrrences.get(v).equals(3)) {
                highestDegV = v;
            } else if (occrrences.get(v).equals(2)) {
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

            for (Integer label : topology.outgoing.get(virtualSrc).keySet()) {
                if (label.equals(midLabel)) continue;
                if (!src2label2dest.containsKey(src) ||
                        !src2label2dest.get(src).containsKey(label)) {
                    countPerSrc *= 0;
                } else {
                    countPerSrc *= src2label2dest.get(src).get(label).size();
                }
            }
            for (Integer label : topology.incoming.get(virtualSrc).keySet()) {
                if (label.equals(midLabel)) continue;
                if (!dest2label2src.containsKey(src) ||
                        !dest2label2src.get(src).containsKey(label)) {
                    countPerSrc *= 0;
                } else {
                    countPerSrc *= dest2label2src.get(src).get(label).size();
                }
            }
            if (countPerSrc == 0) continue;

            for (Integer dest : label2src2dest.get(midLabel).get(src)) {
                Long countPerBranch = countPerSrc;
                for (Integer label : topology.outgoing.get(virtualDest).keySet()) {
                    if (label.equals(midLabel)) continue;
                    if (!src2label2dest.containsKey(dest) ||
                            !src2label2dest.get(dest).containsKey(label)) {
                        countPerBranch *= 0;
                    } else {
                        countPerBranch *= src2label2dest.get(dest).get(label).size();
                    }
                }
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

    @Deprecated
    private Long count(String vListString, String labelSeqString) throws Exception {
        Integer[] labelSeq = toLabelSeq(labelSeqString);
        Integer[] vList = toVList(vListString);
        int type = getType(vList);
        Integer starCenter = -1;
        if (type == Constants.C_FORK) {
            return countFork(labelSeq, vList);
        } else if (type == Constants.C_STAR) {
            starCenter = getCenter(vList);
        }

        Set<Integer> leaves = getLeaves(vList);
        Integer virtualSrc, virtualDest, startDest;

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
            Map<Integer, Set<Integer>> virtual2physicals = new HashMap<>();

            virtual2physicals.putIfAbsent(starCenter, new HashSet<>());
            virtual2physicals.get(starCenter).add(origin);
            virtual2physicals.putIfAbsent(startDest, new HashSet<>());
            virtual2physicals.get(startDest).addAll(listSrcDest.get(labelSeq[0]).get(origin));

            boolean rerun;
            do {
                rerun = false;
                for (int i = 1; i < labelSeq.length; ++i) {
                    virtualSrc = vList[i * 2];
                    virtualDest = vList[i * 2 + 1];

                    if (virtual2physicals.containsKey(virtualSrc) &&
                            virtual2physicals.containsKey(virtualDest)) continue;

                    if (virtual2physicals.containsKey(virtualSrc)) {
                        virtual2physicals.putIfAbsent(virtualDest, new HashSet<>());
                        for (Integer currentV : virtual2physicals.get(virtualSrc)) {
                            if (!src2label2dest.containsKey(currentV)) continue;
                            if (!src2label2dest.get(currentV).containsKey(labelSeq[i])) continue;

                            virtual2physicals.get(virtualDest).addAll(
                                    src2label2dest.get(currentV).get(labelSeq[i])
                            );
                        }
                    } else if (virtual2physicals.containsKey(virtualDest)) {
                        virtual2physicals.putIfAbsent(virtualSrc, new HashSet<>());
                        for (Integer currentV : virtual2physicals.get(virtualDest)) {
                            if (!dest2label2src.containsKey(currentV)) continue;
                            if (!dest2label2src.get(currentV).containsKey(labelSeq[i])) continue;

                            virtual2physicals.get(virtualSrc).addAll(
                                    dest2label2src.get(currentV).get(labelSeq[i])
                            );
                        }
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

    public void parallelConstruct() throws Exception {
        List<Thread> threads = new ArrayList<>();
        Runnable construct;
        Thread thread;

        long startTime = System.currentTimeMillis();
        long endTime;

        int lineNo = 0;

        BufferedReader decomReader = new BufferedReader(new FileReader("decom.csv"));
        String line = decomReader.readLine();
        while (null != line) {
            lineNo++;
            System.out.print("\rConstructing: Line " + lineNo);

            construct = new CatalogueConstructionPerVList(
                    lineNo, src2label2dest, dest2label2src, label2src2dest, label2dest2src, line);

            thread = new Thread(construct);
            threads.add(thread);
            thread.start();

            line = decomReader.readLine();
        }
        decomReader.close();

        for (Thread t : threads) {
            t.join();
        }

        endTime = System.currentTimeMillis();
        System.out.println("\nConstructing: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public void construct() throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter("catalogue.csv"));

        long startTime = System.currentTimeMillis();
        long endTime;

        int lineNo = 0;

        BufferedReader decomReader = new BufferedReader(new FileReader("decom.csv"));
        String[] info;
        String vList;
        String line = decomReader.readLine();
        while (null != line) {
            lineNo++;
            System.out.print("\rConstructing: Line " + lineNo);

            info = line.split(",");
            Integer queryType = Integer.parseInt(info[0]);
//            if (catalogue.size() == queryType) {
//                catalogue.add(new HashMap<>());
//            }

            vList = info[1];
//            catalogue.get(queryType).putIfAbsent(vList, new HashMap<>());
            for (int i = 2; i < info.length; ++i) {
//                catalogue.get(queryType).get(vList).putIfAbsent(info[i], count(vList, info[i]));

                StringJoiner sj = new StringJoiner(",");
                sj.add(queryType.toString());
                sj.add(vList);
                sj.add(info[i]);
                try {
                    sj.add(count(vList, info[i]).toString());
                    writer.write(sj.toString() + "\n");
                } catch (Exception e) {
                    writer.close();
                    throw e;
                }
            }

            line = decomReader.readLine();
        }
        decomReader.close();
        writer.close();

        endTime = System.currentTimeMillis();
        System.out.println("\nConstructing: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void saveCatalogue() throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedWriter writer = new BufferedWriter(new FileWriter("catalogue.csv"));
        for (int type = 0; type < catalogue.size(); ++type) {
            for (String vList : catalogue.get(type).keySet()) {
                StringJoiner sj = new StringJoiner(",");
                sj.add(Integer.toString(type));
                sj.add(vList);

                for (String labelSeq : catalogue.get(type).get(vList).keySet()) {
                    sj.add(labelSeq);
                    sj.add(catalogue.get(type).get(vList).get(labelSeq).toString());
                }
                writer.write(sj.toString() + "\n");
            }
        }
        writer.close();

        endTime = System.currentTimeMillis();
        System.out.println("Saving: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public void load(String catalogueFile, int maxLen) throws Exception {
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

            if (labelSeq.split("->").length <= maxLen) {
                catalogue.get(queryType).putIfAbsent(vList, new HashMap<>());
                catalogue.get(queryType).get(vList).put(labelSeq, count);
            }

            line = catalogueReader.readLine();
        }
        catalogueReader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Loading Catalogue: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private boolean applyFilter(Integer vid, Pair<String, Integer> filter) throws Exception {
        int literal;
        String operator;

        operator = filter.key;
        if (operator.isEmpty()) return true;

        literal = filter.value;

        if (!vid2prodYear.containsKey(vid)) return true;

        switch (operator) {
            case "<":
                return vid2prodYear.get(vid) < literal;
            case ">":
                return vid2prodYear.get(vid) > literal;
            case "<=":
                return vid2prodYear.get(vid) <= literal;
            case ">=":
                return vid2prodYear.get(vid) >= literal;
            case "=":
                return vid2prodYear.get(vid) == literal;
            default:
                throw new Exception("ERROR: unrecognized operator: " + operator);
        }
    }

    private double getRatio(Pair<String, Integer> filter) throws Exception {
        if (filter.key.isEmpty()) return 1.0;

        double qualified = 0;
        for (Integer vid : vid2prodYear.keySet()) {
            if (applyFilter(vid, filter)) qualified++;
        }
        return qualified / vid2prodYear.keySet().size();
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

    public static Set<Integer> toLabelSet(String labelSeq) {
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

    private double computeProportion(Integer[] vList, List<Pair<String, Integer>> filters) throws Exception {
        double ratio = 1.0;

        Set<Integer> computed = new HashSet<>();
        for (int i = 0; i < vList.length; ++i) {
            if (computed.contains(vList[i])) continue;
            computed.add(vList[i]);

            ratio *= getRatio(filters.get(vList[i]));
        }

        return ratio;
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

    private Query getSubQuery(Query query, Set<String> srcDestStrings) throws Exception {
        Topology topology = new Topology();
        Set<Integer> virtuals = new HashSet<>();

        String[] srcDest;
        Integer src, dest;
        for (String srcDestString : srcDestStrings) {
            srcDest = srcDestString.split("-");
            src = Integer.parseInt(srcDest[0]);
            dest = Integer.parseInt(srcDest[1]);
            virtuals.add(src);
            virtuals.add(dest);
        }

        List<Integer> relevantVirtuals = new ArrayList<>(virtuals);
        Collections.sort(relevantVirtuals);

        Map<Integer, Integer> oldIndex2newIndex = new HashMap<>();
        for (int i = 0; i < relevantVirtuals.size(); ++i) {
            oldIndex2newIndex.put(relevantVirtuals.get(i), i);
        }

        for (String srcDestString : srcDestStrings) {
            srcDest = srcDestString.split("-");
            src = Integer.parseInt(srcDest[0]);
            dest = Integer.parseInt(srcDest[1]);
            for (Integer label : query.topology.outgoing.get(src).keySet()) {
                if (query.topology.outgoing.get(src).get(label).contains(dest)) {
                    topology.addEdge(
                            oldIndex2newIndex.get(src), label, oldIndex2newIndex.get(dest)
                    );
                    break;
                }
            }
        }

        List<Pair<String, Integer>> extractedFilters = new ArrayList<>();
        for (Integer v : relevantVirtuals) {
            extractedFilters.add(query.filters.get(v));
        }

        return new Query(topology, extractedFilters);
    }

    public static Pair<String, String> getOverlap(Set<String> visited, String labelSeq, String vList) {
        String[] labelSeqSplitted = labelSeq.split("->");
        String[] vListSplitted = vList.split(";");
        StringJoiner labelSj = new StringJoiner("->");
        StringJoiner vListSj = new StringJoiner(";");

        for (int i = 0; i < vListSplitted.length; ++i) {
            if (visited.contains(vListSplitted[i])) {
                labelSj.add(labelSeqSplitted[i]);
                vListSj.add(vListSplitted[i]);
            }
        }
//        System.out.println("overlap labs: " + labelSj.toString());
//        System.out.println("overlap vlist: " + vListSj.toString());
//        System.out.println();
        return new Pair<>(labelSj.toString(), vListSj.toString());
    }

    private Integer[][][] getDecomByLength(int length) {
        switch (length) {
            case 4:
                return Constants.DECOMPOSITIONS4;
            case 3:
                return Constants.DECOMPOSITIONS3;
            case 2:
                Integer[][][] allLength2 =
                        new Integer[LargeBenchmarkDecompositions.BASE + LargeBenchmarkDecompositions.LENGTH2.length + 1][][];
                for (int i = 0; i < Constants.DECOMPOSITIONS2.length; ++i) {
                    allLength2[i] = Constants.DECOMPOSITIONS2[i];
                }
                for (int i = 0; i < JOBDecompositions.LENGTH2.length; ++i) {
                    int adjusted = JOBDecompositions.JOB_BASE + i + 1;
                    allLength2[adjusted] = JOBDecompositions.LENGTH2[i];
                }
                for (int i = 0; i < LargeBenchmarkDecompositions.LENGTH2.length; ++i) {
                    int adjusted = LargeBenchmarkDecompositions.BASE + i + 1;
                    allLength2[adjusted] = LargeBenchmarkDecompositions.LENGTH2;
                }
                return allLength2;
            default:
                System.out.println("Length not supported");
                return new Integer[][][]{};
        }
    }

    static public void computeDecomByLength(Query query, int maxLen) {
        if (decoms != null) return;

        QueryDecomposer decomposer = new QueryDecomposer();
        decomposer.decompose(query, maxLen);
        decoms = new Integer[decomposer.decompositions.size()][];
        int i = 0;
        for (String vListString : decomposer.decompositions.keySet()) {
            Integer[] vList = Util.toVList(vListString);
            decoms[i] = vList;
            i++;
        }
    }

    public static Integer[][] getDecomByLen(int len) {
        return Arrays.stream(decoms).filter(vList -> vList.length / 2 == len).toArray(Integer[][]::new);
    }

    private Integer[][] getStartingDecoms(Integer patternType, int formulaType, int catLen) {
        if (formulaType == Constants.C_ALL) {
            return getDecomByLength(catLen)[patternType];
        }

        if (patternType.equals(Constants.FORK24)) {
            return Arrays.stream(getDecomByLength(catLen)[patternType])
                    .filter(entry -> getType(entry) == Constants.C_FORK)
                    .toArray(Integer[][]::new);
        } else if (patternType.equals(Constants.PI)) {
            return Arrays.stream(getDecomByLength(catLen)[patternType])
                    .filter(entry -> getType(entry) == Constants.C_PATH)
                    .toArray(Integer[][]::new);
        } else if (patternType.equals(Constants.FORK33)) {
            return Arrays.stream(getDecomByLength(catLen)[patternType])
                    .filter(entry -> getType(entry) == Constants.C_PATH)
                    .toArray(Integer[][]::new);
        } else if (patternType.equals(Constants.FORK34)) {
            return Arrays.stream(getDecomByLength(catLen)[patternType])
                    .filter(entry -> getType(entry) == Constants.C_PATH)
                    .toArray(Integer[][]::new);
        } else if (patternType.equals(Constants.BIFORK)) {
            return Arrays.stream(getDecomByLength(catLen)[patternType])
                    .filter(entry -> getType(entry) == Constants.C_PATH)
                    .toArray(Integer[][]::new);
        } else {
            System.err.println("ERROR: getStartingDecoms - unrecognized pattern");
            return null;
        }
    }

    public static Set<String> toVSet(String vListString) {
        String[] split = vListString.split(",");
        Set<String> VSet = new HashSet<String>();
        for (String edges : split) {
            String[] edge_arr = edges.split(";");
            for (String edge : edge_arr) {
                VSet.add(edge);
            }
        }
        return VSet;
    }

    // implement different combinations including different number of overlapping edges and
    // different number of uniformity assumptions
    private List<Triple<Set<Integer>, String, Double>> getAllEstimates(
            Query query, Integer patternType, int formulaType, int catLen, String VList) throws Exception {
        List<Triple<Set<Integer>, String, Double>> alreadyCovered = new ArrayList<>();
        Set<String> alledges = toVSet(VList);

        computeDecomByLength(query, catLen);
        Integer[][] startingDecoms = getDecomByLen(catLen);

        List<Thread> threads = new ArrayList<>();
        List<EstimateParallel> ests = new ArrayList<>();
        EstimateParallel est_par;
        Thread thread;
        int i = 0;

        for (Integer[] vList : startingDecoms) {
            est_par = new EstimateParallel(i, query, patternType, formulaType, catLen,
                    VList, vList, subsetSize, false);
            ests.add(est_par);
            thread = new Thread(est_par);
            threads.add(thread);
            thread.start();
            i++;
        }

        for(Thread t : threads) {
            t.join();
        }

        for (EstimateParallel r : ests) {
            alreadyCovered.addAll(r.alreadyCovered);
        }

        // System.out.println(alreadyCovered);
        return alreadyCovered.stream()
                .filter(triple -> toVSet(triple.v2).equals(alledges))
                .collect(Collectors.toList());
    }

    public Double[] estimate(Query query, Integer patternType, int formulaType, int catLen) throws Exception {
        List<Triple<Set<Integer>, String, Double>> alreadyCovered =
                getAllEstimates(query, patternType, formulaType, catLen, "");

        // (avg, min, max) based
        Set<Double> allEst = new HashSet<>();
        int numFormula = 0;
        double sum = 0, min = Double.MAX_VALUE, max = -Double.MAX_VALUE;
        for (Triple<Set<Integer>, String, Double> labelsAndEst : alreadyCovered) {
            double est = ((long) (labelsAndEst.v3 * Math.pow(10, 6))) / Math.pow(10, 6);
            if (!allEst.contains(est)) {
                allEst.add(est);
                numFormula++;
                sum += labelsAndEst.v3;
            }

            min = Math.min(min, labelsAndEst.v3);
            max = Math.max(max, labelsAndEst.v3);
        }

        return new Double[] { sum / numFormula, min, max };
    }

    public Map<String, Double> estimateByFormula(
            Query query, Integer patternType, int formulaType, int catLen) throws Exception {

        List<Triple<Set<Integer>, String, Double>> alreadyCovered =
                getAllEstimates(query, patternType, formulaType, catLen, "");

        Map<String, Double> formula2est = new HashMap<>();
        for (Triple<Set<Integer>, String, Double> triple : alreadyCovered) {
            if (formula2est.containsKey(triple.v2)) {
                Double existing = formula2est.get(triple.v2);
                if ((long) (existing * Math.pow(10, 6)) != (long) (triple.v3 * Math.pow(10, 6))) {
                    System.out.println("ERROR: duplicate formula");
                    System.out.println("  triple: " + triple);
                    System.out.println("  existing: " + formula2est.get(triple.v2));
                    return null;
                }
            } else {
                formula2est.put(triple.v2, triple.v3);
            }
        }

        return formula2est;
    }

    public Double[] estimateByHops(Query query, Integer patternType, int formulaType, int catLen, String VList) throws Exception {
        List<Triple<Set<Integer>, String, Double>> alreadyCovered =
                getAllEstimates(query, patternType, formulaType, catLen, VList);

//        Set<Double> allEst = new HashSet<>();
        int numFormula = 0;
        Triple<Double, Double, Double> globalAggr = new Triple<>(Double.MAX_VALUE, Double.MIN_VALUE, 0.0);

        // #hops -> (min, max, avg)
        Map<Integer, Triple<Double, Double, Double>> numHop2aggr = new HashMap<>();
        Map<Integer, Integer> numHops2numFormula = new HashMap<>();
        int maxHop = Integer.MIN_VALUE;
        int minHop = Integer.MAX_VALUE;
        for (Triple<Set<Integer>, String, Double> triple : alreadyCovered) {
//            double est = ((long) (triple.v3 * Math.pow(10, 6))) / Math.pow(10, 6);
            double est = triple.v3;
//            if (allEst.contains(est)) continue;
//            allEst.add(est);

            globalAggr.v1 = Math.min(globalAggr.v1, est);
            globalAggr.v2 = Math.max(globalAggr.v2, est);
            globalAggr.v3 += est;
            numFormula++;

            int formulaLen = triple.v2.split(",").length / 2 + 1;
            numHops2numFormula.put(formulaLen, numHops2numFormula.getOrDefault(formulaLen, 0) + 1);
            numHop2aggr.putIfAbsent(formulaLen, new Triple<>(Double.MAX_VALUE, Double.MIN_VALUE, 0.0));
            maxHop = Math.max(maxHop, formulaLen);
            minHop = Math.min(minHop, formulaLen);

            Triple<Double, Double, Double> stats = numHop2aggr.get(formulaLen);
            stats.v1 = Math.min(stats.v1, est);
            stats.v2 = Math.max(stats.v2, est);
            stats.v3 += est;
            numHop2aggr.put(formulaLen, stats);
        }

        return new Double[] {
                globalAggr.v1,
                globalAggr.v2,
                globalAggr.v3 / numFormula,
                numHop2aggr.get(minHop).v1,
                numHop2aggr.get(minHop).v2,
                numHop2aggr.get(minHop).v3 / numHops2numFormula.get(minHop),
                numHop2aggr.get(maxHop).v1,
                numHop2aggr.get(maxHop).v2,
                numHop2aggr.get(maxHop).v3 / numHops2numFormula.get(maxHop)
        };
    }

    private List<List<Triple<Integer, String, Double>>> getAllEstimatesRandom(
            Query query, Integer patternType, int formulaType, int catLen, String VList) throws Exception {
        List<Triple<Integer, String, Double>> minhops = new ArrayList<>();
        List<Triple<Integer, String, Double>> maxhops = new ArrayList<>();
        List<Triple<Integer, String, Double>> randomhops = new ArrayList<>();;
        Set<String> alledges = toVSet(VList);

        computeDecomByLength(query, catLen);
        Integer[][] startingDecoms = getDecomByLen(catLen);

        List<Thread> threads = new ArrayList<>();
        List<EstimateParallel> ests = new ArrayList<>();
        EstimateParallel est_par;
        Thread thread;
        int threadnum = 100;

        for (int i = 0; i < threadnum; ++i) {
            est_par = new EstimateParallel(i, query, patternType, formulaType, catLen, VList, startingDecoms[0], subsetSize / threadnum, true);
            ests.add(est_par);
            thread = new Thread(est_par);
            threads.add(thread);
            thread.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        for (EstimateParallel r : ests) {
            maxhops.addAll(r.maxhops);
            minhops.addAll(r.minhops);
            randomhops.addAll(r.randomhops);
        }
        List<List<Triple<Integer, String, Double>>> l = new ArrayList<>();
        l.add(minhops);
        l.add(randomhops);
        l.add(maxhops);
        return l;
    }

    public Double[] estimateByHopsRandom(Query query, Integer patternType, int formulaType, int catLen, String VList) throws Exception {
        List<List<Triple<Integer, String, Double>>> l = getAllEstimatesRandom(query, patternType, formulaType, catLen, VList);
        List<Triple<Integer, String, Double>> minhops = l.get(0);
        List<Triple<Integer, String, Double>> randomhops = l.get(1);
        List<Triple<Integer, String, Double>> maxhops = l.get(2);

        int numFormula = 0;
        Triple<Double, Double, Double> globalAggr = new Triple<>(Double.MAX_VALUE, Double.MIN_VALUE, 0.0);

        // #hops -> (min, max, avg)
        Map<Integer, Triple<Double, Double, Double>> numHop2aggr = new HashMap<>();
        int maxLen = toVSet(VList).size() - catLen + 1;
        int minLen = minhops.get(0).v2.split(",").length / 2 + 1;
        Triple<Integer, String, Double> mintriple;
        Triple<Integer, String, Double> maxtriple;
        Triple<Integer, String, Double> randomtriple;

        for (int i = 0; i < subsetSize; ++i) {
            mintriple = minhops.get(i);
            maxtriple = maxhops.get(i);
            randomtriple = randomhops.get(i);

            double minest = ((long) (mintriple.v3 * Math.pow(10, 6))) / Math.pow(10, 6);
            double maxest = ((long) (maxtriple.v3 * Math.pow(10, 6))) / Math.pow(10, 6);
            double randomest = ((long) (randomtriple.v3 * Math.pow(10, 6))) / Math.pow(10, 6);

            globalAggr.v1 = Math.min(globalAggr.v1, randomest);
            globalAggr.v2 = Math.max(globalAggr.v2, randomest);
            globalAggr.v3 += randomest;

            int minformulaLen = mintriple.v2.split(",").length / 2 + 1;
            numHop2aggr.putIfAbsent(minformulaLen, new Triple<>(Double.MAX_VALUE, Double.MIN_VALUE, 0.0));
            Triple<Double, Double, Double> minstats = numHop2aggr.get(minformulaLen);
            minstats.v1 = Math.min(minstats.v1, minest);
            minstats.v2 = Math.max(minstats.v2, minest);
            minstats.v3 += minest;
            numHop2aggr.put(minformulaLen, minstats);

            int maxformulaLen = maxtriple.v2.split(",").length / 2 + 1;
            numHop2aggr.putIfAbsent(maxformulaLen, new Triple<>(Double.MAX_VALUE, Double.MIN_VALUE, 0.0));
            Triple<Double, Double, Double> maxstats = numHop2aggr.get(maxformulaLen);
            maxstats.v1 = Math.min(maxstats.v1, maxest);
            maxstats.v2 = Math.max(maxstats.v2, maxest);
            maxstats.v3 += maxest;
            numHop2aggr.put(maxformulaLen, maxstats);
        }

//        System.out.println("maxLen: " + maxLen);
//        System.out.println("minLen: " + minLen);
//        System.out.println("allHop: " + numFormula);

        return new Double[] {
                globalAggr.v1,
                globalAggr.v2,
                globalAggr.v3 / subsetSize,
                numHop2aggr.get(minLen).v1,
                numHop2aggr.get(minLen).v2,
                numHop2aggr.get(minLen).v3 / subsetSize,
                numHop2aggr.get(maxLen).v1,
                numHop2aggr.get(maxLen).v2,
                numHop2aggr.get(maxLen).v3 / subsetSize
        };
    }

    private Map<String, Map<String, Map<String, Map<String, Double>>>> loadEntropy(String entropyFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        // baseVList -> baseLabelSeq -> extVList -> extLabel -> cv/entropy
        Map<String, Map<String, Map<String, Map<String, Double>>>> uniformityMeasure = new HashMap<>();
        BufferedReader catalogueReader = new BufferedReader(new FileReader(entropyFile));
        String[] info;
        String baseVList, baseLabelSeq, extVList, extLabel;
        String line = catalogueReader.readLine();
        while (null != line) {
            info = line.split(",");

            baseVList = info[1];
            baseLabelSeq = info[2];
            extVList = info[3];
            extLabel = info[4];
            if (info[5].contains("NaN")) continue;
            Double measure = Double.parseDouble(info[5]);
            uniformityMeasure.putIfAbsent(baseVList, new HashMap<>());
            uniformityMeasure.get(baseVList).putIfAbsent(baseLabelSeq, new HashMap<>());
            uniformityMeasure.get(baseVList).get(baseLabelSeq).putIfAbsent(extVList, new HashMap<>());
            uniformityMeasure.get(baseVList).get(baseLabelSeq).get(extVList).putIfAbsent(extLabel, measure);

//            if (uniformityMeasure.get()) {
//                catalogue.get(queryType).putIfAbsent(vList, new HashMap<>());
//                catalogue.get(queryType).get(vList).put(labelSeq, count);
//            }

            line = catalogueReader.readLine();
        }
        catalogueReader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Loading Catalogue: " + ((endTime - startTime) / 1000.0) + " sec");
        return uniformityMeasure;
    }


    public List<Triple<List<Double>, String, Double>> getAllEntropy(
            Query query, Integer patternType, int formulaType, int catLen, String VList, String measurementFile) throws Exception {
        List<Triple<List<Double>, String, Double>> alreadyCovered = new ArrayList<>();
        Set<String> alledges = toVSet(VList);

        computeDecomByLength(query, catLen);
        Integer[][] startingDecoms = getDecomByLen(catLen);

        List<Thread> threads = new ArrayList<>();
        List<EntropyParallel> ests = new ArrayList<>();
        EntropyParallel est_par;
        Thread thread;
        int i = 0;
        Map<String, Map<String, Map<String, Map<String, Double>>>> entropy = loadEntropy(measurementFile);
        for (Integer[] vList : startingDecoms) {
            est_par = new EntropyParallel(i, query, patternType, formulaType, catLen,
                    VList, vList, entropy);
            ests.add(est_par);
            thread = new Thread(est_par);
            threads.add(thread);
            thread.start();
            i++;
        }

        for(Thread t : threads) {
            t.join();
        }

        for (EntropyParallel r : ests) {
            alreadyCovered.addAll(r.alreadyCovered);
        }

        // System.out.println(alreadyCovered);
        return alreadyCovered.stream()
                .filter(triple -> toVSet(triple.v2).equals(alledges))
                .collect(Collectors.toList());
    }

    public Double[] getQErrors(Query query, Integer patternType, int formulaType, int catLen, String VList, boolean sampling, Double trueCard) throws Exception {
        Set<Double> allEst = new HashSet<>();
        Double maxMaxEst;
        if (sampling) {
            List<List<Triple<Integer, String, Double>>> l = getAllEstimatesRandom(query, patternType, formulaType, catLen, VList);
            List<Triple<Integer, String, Double>> minhops = l.get(0);
            List<Triple<Integer, String, Double>> randomhops = l.get(1);
            List<Triple<Integer, String, Double>> maxhops = l.get(2);
            for (int i = 0; i < subsetSize; ++i) {
                Double minEst = minhops.get(i).v3;
                Double maxEst = maxhops.get(i).v3;
                Double randomEst = randomhops.get(i).v3;
                if (!allEst.contains(minEst)) allEst.add(minEst);
                if (!allEst.contains(maxEst)) allEst.add(maxEst);
                if (!allEst.contains(randomEst)) allEst.add(randomEst);
            }
            maxMaxEst = estimateByHopsRandom(query, patternType, formulaType, catLen, VList)[7];
        } else {
            List<Triple<Set<Integer>, String, Double>> alreadyCovered = getAllEstimates(query, patternType, formulaType, catLen, VList);
            for (Triple<Set<Integer>, String, Double> triple : alreadyCovered) {
                Double est = triple.v3;
                if (!allEst.contains(est)) allEst.add(est);
            }
            maxMaxEst = estimateByHops(query, patternType, formulaType, catLen, VList)[7];
        }
        Double[] estimations = allEst.toArray(new Double[allEst.size()]);
        Double[] qErrors = new Double[estimations.length];
        for (int i = 0; i < estimations.length; ++i) {
            // overestimation
            if (estimations[i] >= trueCard) qErrors[i] = estimations[i] / trueCard;
                // underestimation
            else qErrors[i] = - (trueCard / estimations[i]);
        }
        Arrays.sort(qErrors, Comparator.comparingDouble(Math::abs));
        Double[] results = new Double[4];
        results[0] = qErrors[0];
        if (maxMaxEst >= trueCard) results[1] = maxMaxEst / trueCard;
        else results[1] = - (trueCard / maxMaxEst);
        int maxMaxRank = -1;
        for (int i = 0; i < qErrors.length; ++i) {
            if (qErrors[i].equals(results[1])) {
                maxMaxRank = i;
                break;
            }
        }
        // results[2] = (maxMaxRank + 1.0) / (double) qErrors.length;
        results[2] = maxMaxRank + 1.0;
        results[3] = (double) qErrors.length;
        System.out.println("p* q-error: " + results[0]);
        System.out.println("max-max q-error: " + results[1]);
        System.out.println("max-max rank: " + results[2]);
        System.out.println("number of estimations: " + qErrors.length);
        return results;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println("destFile: " + args[1]);
        System.out.println();

        new Catalogue(args[0], args[1]);
    }
}