package MarkovTable;

import Common.Evaluation;
import Common.Pair;
import Common.Path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;

public class MarkovTable {
    private Map<Path, Integer> table;
    private int maxPathLength;
    private boolean hasFromToPath;

    private final int STAR = -1;

    public Map<Path, Map<Integer, Integer>> path2src2occ;

    public MarkovTable(String mtFile, int maxPathLength) throws Exception {
        this.table = new HashMap<>();
        this.maxPathLength = maxPathLength;

        BufferedReader mtReader = new BufferedReader(new FileReader(mtFile));
        String line = mtReader.readLine();
        String[] pathAndCount;
        String[] pathString;
        List<Integer> labels = new ArrayList<>();
        int count;

        while (null != line) {
            pathAndCount = line.split(",");
            pathString = pathAndCount[0].split("->");
            count = Integer.parseInt(pathAndCount[1]);

            for (String label : pathString) {
                labels.add(Integer.parseInt(label));
            }

            table.put(new Path(labels), count);

            labels.clear();
            line = mtReader.readLine();
        }

        mtReader.close();
    }

    public MarkovTable(String csvFilePath, int maxPathLength, boolean hasFromToPath,
        boolean hasLabelMeta) throws Exception {
        this.table = new HashMap<>();
        this.path2src2occ = new HashMap<>();
        this.maxPathLength = maxPathLength;
        this.hasFromToPath = hasFromToPath;

        Map<Integer, Map<Integer, List<Integer>>> allEdges = new HashMap<>();
        Map<Integer, List<Integer>> edgeLabel2destVertices = new HashMap<>();

        BufferedReader csvReader = new BufferedReader(new FileReader(csvFilePath));
        int[] line;
        if (hasLabelMeta) csvReader.readLine();
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            line = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt).toArray();

            // add the triple into the triple store
            allEdges.putIfAbsent(line[0], new HashMap<>());
            allEdges.get(line[0]).putIfAbsent(line[1], new ArrayList<>());
            allEdges.get(line[0]).get(line[1]).add(line[2]);

            edgeLabel2destVertices.putIfAbsent(line[1], new ArrayList<>());
            edgeLabel2destVertices.get(line[1]).add(line[2]);

            tripleString = csvReader.readLine();
        }

        for (Integer edgeLabel : edgeLabel2destVertices.keySet()) {
            Map<Integer, List<List<Integer>>> currentStepNodes2prefixes = new HashMap<>();
            List<Integer> startingLabel = new ArrayList<>();
            startingLabel.add(edgeLabel);
            for (Integer v : edgeLabel2destVertices.get(edgeLabel)) {
                currentStepNodes2prefixes.putIfAbsent(v, new ArrayList<>());
                currentStepNodes2prefixes.get(v).add(startingLabel);
            }

            Map<Integer, List<List<Integer>>> nextStepNodes2prefixes = new HashMap<>();
            Map<Integer, List<Integer>> nextLabels2destNodes;

            List<Integer> edgeLabelPath = new ArrayList<>();
            edgeLabelPath.add(edgeLabel);
            Path tableEntry = new Path(edgeLabelPath);
            if (!hasFromToPath || maxPathLength > 2) {
                table.put(tableEntry, edgeLabel2destVertices.get(edgeLabel).size());
            }

            int pathStep = 1;
            while (pathStep < maxPathLength) {
                // ((fromNode, destNode), (nextLabel, prefix))
                Set<Pair<Pair<Integer, Integer>, Pair<Integer, List<Integer>>>> fromPathChecker =
                    new HashSet<>();
                // ((prefix, nextLabel), fromNode)
                Set<Pair<Pair<List<Integer>, Integer>, Integer>> toPathChecker = new HashSet<>();

                for (Integer fromNode : currentStepNodes2prefixes.keySet()) {
                    if (allEdges.containsKey(fromNode)) {
                        nextLabels2destNodes = allEdges.get(fromNode);
                        for (Integer nextLabel : nextLabels2destNodes.keySet()) {
                            for (Integer destNode : nextLabels2destNodes.get(nextLabel)) {
                                for (List<Integer> prefix : currentStepNodes2prefixes.get(fromNode)) {
                                    edgeLabelPath = new ArrayList<>(prefix);
                                    edgeLabelPath.add(nextLabel);
                                    nextStepNodes2prefixes.putIfAbsent(destNode, new ArrayList<>());
                                    nextStepNodes2prefixes.get(destNode).add(edgeLabelPath);

                                    if (hasFromToPath && pathStep == maxPathLength - 2) continue;

                                    tableEntry = new Path(edgeLabelPath);
                                    table.put(tableEntry, table.getOrDefault(tableEntry, 0) + 1);

                                    if (hasFromToPath && pathStep == maxPathLength - 1) {
                                        Pair<Pair<Integer, Integer>, Pair<Integer, List<Integer>>> fromPath =
                                            new Pair<>(new Pair<>(fromNode, destNode), new Pair<>(nextLabel, prefix));

                                        if (!fromPathChecker.contains(fromPath)) {
                                            tableEntry = new Path(
                                                edgeLabelPath.subList(1, edgeLabelPath.size()),
                                                edgeLabelPath.subList(0, 1),
                                                "fromPath");
                                            table.put(tableEntry, table.getOrDefault(tableEntry, 0) + 1);

                                            fromPathChecker.add(fromPath);
                                        }

                                        Pair<Pair<List<Integer>, Integer>, Integer> toPath =
                                            new Pair<>(new Pair<>(prefix, nextLabel), fromNode);

                                        if (!toPathChecker.contains(toPath)) {
                                            tableEntry = new Path(
                                                edgeLabelPath.subList(0, edgeLabelPath.size() - 1),
                                                edgeLabelPath.subList(
                                                    edgeLabelPath.size() - 1,
                                                    edgeLabelPath.size()),
                                                "toPath");
                                            table.put(tableEntry, table.getOrDefault(tableEntry, 0) + 1);

                                            toPathChecker.add(toPath);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                currentStepNodes2prefixes = new HashMap<>(nextStepNodes2prefixes);
                nextStepNodes2prefixes.clear();
                pathStep++;
            }
        }

        this.print();
        csvReader.close();
    }

    // Assumes we already have length-2 paths in MT
    public void grow(String[] trueCardFiles) throws Exception {
        int originalSize = table.size();
        for (String trueCardFile : trueCardFiles) {
            BufferedReader csvReader = new BufferedReader(new FileReader(trueCardFile));
            String tripleString = csvReader.readLine();
            String[] lineArray;
            List<Integer> edgeList = null;
            Path query;
            double trueCard, qError;
            Map<Path, Integer> p2trueCard = new HashMap<>();
            PriorityQueue<Pair<Double, Path>> qErrorMinHeap =
                new PriorityQueue<>(originalSize, new QErrorPathComparator());

            while (null != tripleString) {
                edgeList = new ArrayList<>();
                lineArray = tripleString.split(",");
                for (int i = 0; i < lineArray.length - 1; ++i) {
                    edgeList.add(Integer.parseInt(lineArray[i]));
                }
                query = new Path(edgeList);
                trueCard = Double.parseDouble(lineArray[lineArray.length - 1]);

                qError = Evaluation.computeQError(this.estimate(query), trueCard);

                // keep the top originalSize largest q-error query paths
                if (qErrorMinHeap.size() < originalSize || qError > qErrorMinHeap.peek().getKey()) {
                    if (qErrorMinHeap.size() >= originalSize) {
                        p2trueCard.remove(qErrorMinHeap.poll().getValue());
                    }
                    qErrorMinHeap.add(new Pair<>(qError, query));
                    p2trueCard.put(query, Integer.parseInt(lineArray[lineArray.length - 1]));
                }

                tripleString = csvReader.readLine();
            }

            maxPathLength = Math.max(edgeList != null ? edgeList.size() : 0, maxPathLength);

            table.putAll(p2trueCard);

            csvReader.close();
        }

//        this.print();
    }

    public void randomGrow(String[] trueCardFiles) throws Exception {
        int originalSize = table.size();
        for (String trueCardFile : trueCardFiles) {
            BufferedReader csvReader = new BufferedReader(new FileReader(trueCardFile));
            String tripleString = csvReader.readLine();
            String[] lineArray;
            List<Integer> edgeList = null;
            Path query;
            List<Path> paths = new ArrayList<>();
            Map<Path, Integer> p2trueCard = new HashMap<>();
            while (null != tripleString) {
                edgeList = new ArrayList<>();
                lineArray = tripleString.split(",");
                for (int i = 0; i < lineArray.length - 1; ++i) {
                    edgeList.add(Integer.parseInt(lineArray[i]));
                }
                query = new Path(edgeList);

                p2trueCard.put(query, Integer.parseInt(lineArray[lineArray.length - 1]));

                paths.add(query);

                tripleString = csvReader.readLine();
            }

            maxPathLength = Math.max(edgeList != null ? edgeList.size() : 0, maxPathLength);

            Random random = new Random();

            for (int i = 0; i < originalSize; ++i) {
                query = paths.get(random.nextInt(paths.size()));
                table.put(query, p2trueCard.get(query));
            }

            csvReader.close();
        }

//        this.print();
    }

    public void suffixShrink(int desiredSize) {
        int heapCap = table.size() - desiredSize;
        // cardinality - path
        PriorityQueue<Pair<Integer, Path>> maxHeap =
            new PriorityQueue<>(heapCap, new CardPathComparatorReverse());

        for (Path path : table.keySet()) {
            // keep the lowest freq ones
            if (maxHeap.size() < heapCap || table.get(path) < maxHeap.peek().getKey()) {
                if (maxHeap.size() >= heapCap) {
                    maxHeap.poll();
                }

                maxHeap.add(new Pair<>(table.get(path), path));
            }
        }

        PriorityQueue<Pair<Integer, Path>> minHeap =
            new PriorityQueue<>(heapCap, new CardPathComparator());
        minHeap.addAll(maxHeap);

        // <prefix, (cardinality, path)>
        Map<Integer, Pair<Integer, Path>> prefix2entry = new HashMap<>();

        // <prefix, (cardinality, #paths)>
        Map<Integer, Pair<Integer, Integer>> suffixStarCard = new HashMap<>();

        int singleStarTotal = 0;
        int singleStarNumPaths = 0;

        while (minHeap.size() >= suffixStarCard.size()) {
            Pair<Integer, Path> entry = minHeap.poll();
            table.remove(entry.getValue());
            if (entry.getValue().length() == 1) {
                singleStarNumPaths++;
                singleStarTotal += entry.getKey();
            } else if (entry.getValue().length() == 2) {
                int prefix = entry.getValue().getEdgeLabelList().get(0);
                int suffix = entry.getValue().getEdgeLabelList().get(1);
                if (STAR == suffix) {
                    if (!suffixStarCard.containsKey(STAR)) { // */* not exists
                        suffixStarCard.put(STAR, new Pair<>(0, 0));
                    }
                    suffixStarCard.get(STAR).key += entry.getKey();
                    suffixStarCard.get(STAR).value += 1;

                    suffixStarCard.remove(prefix);
                } else {
                    if (suffixStarCard.containsKey(prefix)) {
                        List<Integer> edgeList = new ArrayList<>();
                        edgeList.add(prefix);
                        edgeList.add(STAR);
                        Pair<Integer, Path> minHeapEntry = new Pair<>(
                            suffixStarCard.get(prefix).key, new Path(edgeList)
                        );
                        // needs to re-insert the A/* entry in min heap
                        minHeap.remove(minHeapEntry);

                        suffixStarCard.get(prefix).key += entry.getKey();
                        suffixStarCard.get(prefix).value += 1;
                        minHeapEntry.key = suffixStarCard.get(prefix).key;

                        minHeap.add(minHeapEntry);
                    } else if (prefix2entry.containsKey(prefix)) {
                        Pair<Integer, Path> cardPath = prefix2entry.remove(prefix);
                        suffixStarCard.put(prefix, new Pair<>(
                            cardPath.key + entry.key,
                            2
                        ));
                        List<Integer> edgeList = new ArrayList<>();
                        edgeList.add(prefix);
                        edgeList.add(STAR);
                        minHeap.add(new Pair<>(cardPath.key + entry.key, new Path(edgeList)));
                    } else {
                        prefix2entry.put(prefix, entry);
                    }
                }
            }
        }

        // add all leftovers to */*
        for (Integer prefix : prefix2entry.keySet()) {
            if (!suffixStarCard.containsKey(STAR)) { // */* not exists
                suffixStarCard.put(STAR, new Pair<>(0, 0));
            }
            suffixStarCard.get(STAR).key += prefix2entry.get(prefix).getKey();
            suffixStarCard.get(STAR).value += 1;
        }

        for (Integer prefix : suffixStarCard.keySet()) {
            List<Integer> edgeList = new ArrayList<>();
            edgeList.add(prefix);
            edgeList.add(STAR);
            table.put(
                new Path(edgeList),
                suffixStarCard.get(prefix).key / suffixStarCard.get(prefix).value
            );
        }

        List<Integer> singleStar = new ArrayList<>();
        singleStar.add(STAR);
        if (singleStarNumPaths > 0) {
            table.put(new Path(singleStar), singleStarTotal / singleStarNumPaths);
        }

        this.print();
    }

    public void print() {
        for (Path path : table.keySet()) {
            System.out.println(path.toString() + ": " + table.get(path));
        }
    }

    private double estimate(Path path, int subpathLen) {
//        if (path.length() <= 2) {
        if (path.length() <= maxPathLength) {
            return table.getOrDefault(path, 0);
        }

        Path[] subpaths = path.getAllSubpaths(subpathLen);

        List<Integer> singleStarEdge = new ArrayList<>();
        singleStarEdge.add(STAR);
        Path singleStar = new Path(singleStarEdge);
        List<Integer> doubleStarEdge = new ArrayList<>();
        doubleStarEdge.add(STAR);
        doubleStarEdge.add(STAR);
        Path doubleStar = new Path(doubleStarEdge);
        List<Integer> suffixStarEdge = new ArrayList<>();
        suffixStarEdge.add(STAR);
        suffixStarEdge.add(STAR);

        // default to 0 is actually correct, before it was 1 to avoid NaN in RE / QE
        double estimation;
        if (table.containsKey(subpaths[0])) {
            estimation = table.get(subpaths[0]);
        } else {
            if (subpaths[0].length() == 2) {
                suffixStarEdge.set(0, subpaths[0].getEdgeLabelList().get(0));
                Path suffixStar = new Path(suffixStarEdge);
                if (table.containsKey(suffixStar)) {
                    estimation = table.get(suffixStar);
                } else {
                    estimation = table.getOrDefault(doubleStar, 0);
                }
            } else {
                estimation = estimate(subpaths[0], subpaths[0].length() - 1);
            }
        }
        int divisor = 1;
        Path prevPrefix = subpaths[0].getPrefix(1);

        for (int i = 1; i < subpaths.length; ++i) {
            List<Integer> subEdgeLabelList = subpaths[i].getEdgeLabelList();
            if (subpathLen == 2) {
                suffixStarEdge.set(0, subEdgeLabelList.get(0));
            }

            if (table.containsKey(subpaths[i]) || subpathLen > 2) {
                if (table.containsKey(subpaths[i])) {
                    estimation *= table.get(subpaths[i]);
                } else if (subpathLen > 2) {
                    estimation *= estimate(subpaths[i], subpathLen - 1);
                }

                if (hasFromToPath) {
                    Path subPathPrefixFromPath = new Path(
                        subEdgeLabelList.subList(0, subEdgeLabelList.size() - 1),
                        prevPrefix.getEdgeLabelList(),
                        "fromPath"
                    );
                    Path subPathPrefixToPath = new Path(
                        subEdgeLabelList.subList(0, subEdgeLabelList.size() - 1),
                        subEdgeLabelList.subList(subEdgeLabelList.size() - 1, subEdgeLabelList.size()),
                        "toPath"
                    );

                    divisor = Math.max(
                        table.get(subPathPrefixFromPath),
                        table.get(subPathPrefixToPath)
                    );
                } else {
                    Path subPathPrefix = new Path(subEdgeLabelList.subList(0, subEdgeLabelList.size() - 1));
                    if (table.containsKey(subPathPrefix)) {
                        divisor = table.get(subPathPrefix);
                    } else {
                        if (subPathPrefix.length() == 1) {
                            divisor = table.get(singleStar);
                        } else if (subPathPrefix.length() == 2) {
                            suffixStarEdge.set(0, subPathPrefix.getEdgeLabelList().get(0));
                            Path suffixStar = new Path(suffixStarEdge);
                            if (table.containsKey(suffixStar)) {
                                divisor = table.get(suffixStar);
                            } else if (table.containsKey(doubleStar)) {
                                divisor = table.get(doubleStar);
                            }
                        } else {
                            divisor = (int) estimate(subPathPrefix, subPathPrefix.length() - 1);
                        }
                    }
                }
                estimation /= divisor;

                prevPrefix = subpaths[i].getPrefix(1);
            } else {
                if (subpathLen == 2) {
                    Path suffixStar = new Path(suffixStarEdge);
                    if (table.containsKey(suffixStar)) {
                        estimation *= table.get(suffixStar);
                    } else if (table.containsKey(doubleStar)) {
                        estimation *= table.get(doubleStar);
                    } else {
                        return 0;
                    }

                    Path subPathPrefix = new Path(subEdgeLabelList.subList(0, 1));
                    if (table.containsKey(subPathPrefix)) {
                        estimation /= table.get(subPathPrefix);
                    } else {
                        estimation /= table.get(singleStar);
                    }
                } else {
                    return 0;
                }
            }
        }

        return estimation;
    }

    public double estimate(Path path) {
        if (path.length() <= maxPathLength) {
            return table.getOrDefault(path, 0);
        }

        Path[] subpaths = path.getAllSubpaths(maxPathLength);

        // default to 0 is actually correct, before it was 1 to avoid NaN in RE / QE
        double estimation = table.getOrDefault(subpaths[0], 0);
        for (int i = 1; i < subpaths.length; ++i) {
            if (table.containsKey(subpaths[i])) {
                estimation *= table.get(subpaths[i]);
                List<Integer> subEdgeLabelList = subpaths[i].getEdgeLabelList();
                Path subPathPrefix = new Path(subEdgeLabelList.subList(0, subEdgeLabelList.size() - 1));
                estimation /= table.get(subPathPrefix);
            } else {
                estimation = 0;
            }
        }

        return estimation;
    }

    /*
    public double estimate(Path path) {
        return estimate(path, maxPathLength);
    }
    */

    public void save(String tableDest) throws Exception {
        BufferedWriter tableWriter = new BufferedWriter(new FileWriter(tableDest));
        tableWriter.write("Path,#Occurrences\n");
        for (Path path : table.keySet()) {
            tableWriter.write(path + "," + table.get(path) + "\n");
        }
        tableWriter.close();
    }

    class QErrorPathComparator implements Comparator<Pair<Double, Path>> {
        public int compare(Pair<Double, Path> p1, Pair<Double, Path> p2) {
            if (p1.getKey() > p2.getKey()) {
                return 1;
            } else if (p1.getKey() < p2.getKey()) {
                return -1;
            }
            return 0;
        }
    }

    class CardPathComparatorReverse implements Comparator<Pair<Integer, Path>> {
        public int compare(Pair<Integer, Path> p1, Pair<Integer, Path> p2) {
            if (p1.getKey() < p2.getKey()) {
                return 1;
            } else if (p1.getKey() > p2.getKey()) {
                return -1;
            }
            return 0;
        }
    }

    class CardPathComparator implements Comparator<Pair<Integer, Path>> {
        public int compare(Pair<Integer, Path> p1, Pair<Integer, Path> p2) {
            if (p1.getKey() > p2.getKey()) {
                return 1;
            } else if (p1.getKey() < p2.getKey()) {
                return -1;
            }
            return 0;
        }
    }
}
