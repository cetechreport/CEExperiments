package MarkovTable.PropertyFilter;

import Common.Pair;
import Common.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class MTSampling {
    private Map<Integer, List<Pair<Integer, Integer>>> label2srcdest;
    private Map<Integer, Map<Integer, List<Integer>>> src2label2dest;
    private Map<Integer, Map<Integer, List<Integer>>> dest2label2src;
    private Path path;
    private List<List<Integer>> backwardPart;
    private List<List<Integer>> forwardPart;

    private static Map<Integer, Integer> propertiesToFilter;

    private MTSampling(String csvFilePath) throws Exception {
        label2srcdest = new HashMap<>();
        src2label2dest = new HashMap<>();
        dest2label2src = new HashMap<>();

        BufferedReader csvReader = new BufferedReader(new FileReader(csvFilePath));
        int[] line;
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            line = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt).toArray();

            label2srcdest.putIfAbsent(line[1], new ArrayList<>());
            label2srcdest.get(line[1]).add(new Pair<>(line[0], line[2]));

            src2label2dest.putIfAbsent(line[0], new HashMap<>());
            src2label2dest.get(line[0]).putIfAbsent(line[1], new ArrayList<>());
            src2label2dest.get(line[0]).get(line[1]).add(line[2]);

            dest2label2src.putIfAbsent(line[2], new HashMap<>());
            dest2label2src.get(line[2]).putIfAbsent(line[1], new ArrayList<>());
            dest2label2src.get(line[2]).get(line[1]).add(line[0]);

            tripleString = csvReader.readLine();
        }

        csvReader.close();
    }

    class Backward implements Runnable {
        int initSrc;

        public Backward(int initSrc) {
            this.initSrc = initSrc;
        }

        public void run() {
            backwardPart = new ArrayList<>();
            Map<Integer, List<List<Integer>>> currentStepNodes2suffixNodes = new HashMap<>();
            boolean canBackward = dest2label2src.containsKey(initSrc);

            int pathStep = path.length() / 2 - (1 - path.length() % 2);

            List<List<Integer>> initSuffixNodes = new ArrayList<>();
            initSuffixNodes.add(new ArrayList<>());
            currentStepNodes2suffixNodes.putIfAbsent(initSrc, initSuffixNodes);

            Map<Integer, List<List<Integer>>> nextStepNodes2suffixNodes = new HashMap<>();
            List<Integer> vertexPath;

            // going backward
            while (pathStep > 0 && canBackward) {
                int nextLabel = path.getEdgeLabelList().get(pathStep - 1);
                for (Integer dest: currentStepNodes2suffixNodes.keySet()) {
                    if (dest2label2src.containsKey(dest) &&
                        dest2label2src.get(dest).containsKey(nextLabel)) {
                        for (Integer src: dest2label2src.get(dest).get(nextLabel)) {
                            for (List<Integer> suffixNodes: currentStepNodes2suffixNodes.get(dest)) {
                                vertexPath = new LinkedList<>();
                                vertexPath.add(dest);
                                vertexPath.addAll(suffixNodes);
                                nextStepNodes2suffixNodes.putIfAbsent(src, new ArrayList<>());
                                nextStepNodes2suffixNodes.get(src).add(vertexPath);
                            }
                        }
                    }
                }

                currentStepNodes2suffixNodes = new HashMap<>(nextStepNodes2suffixNodes);
                nextStepNodes2suffixNodes.clear();
                pathStep--;
            }

            for (Integer src : currentStepNodes2suffixNodes.keySet()) {
                for (List<Integer> suffixNodes : currentStepNodes2suffixNodes.get(src)) {
                    suffixNodes.add(0, src);
                    backwardPart.add(suffixNodes);
                }
            }
        }
    }

    class Forward implements Runnable {
        int initDest;
        public Forward(int initDest) {
            this.initDest = initDest;
        }

        public void run() {
            forwardPart = new ArrayList<>();
            Map<Integer, List<List<Integer>>> currentStepNodes2prefixNodes = new HashMap<>();
            boolean canForward = src2label2dest.containsKey(initDest);

            int pathStep = path.length() / 2 - (1 - path.length() % 2);

            List<List<Integer>> initPrefixNodes = new ArrayList<>();
            initPrefixNodes.add(new ArrayList<>());
            currentStepNodes2prefixNodes.putIfAbsent(initDest, initPrefixNodes);

//            System.out.println("initDest: " + initDest);

            Map<Integer, List<List<Integer>>> nextStepNodes2prefixNodes = new HashMap<>();
            List<Integer> vertexPath;

            // going forward
            while (pathStep < path.length() - 1 && canForward) {
                int nextLabel = path.getEdgeLabelList().get(pathStep + 1);
                for (Integer src: currentStepNodes2prefixNodes.keySet()) {
//                    System.out.println("src: " + src);
                    if (src2label2dest.containsKey(src) &&
                        src2label2dest.get(src).containsKey(nextLabel)) {
                        for (Integer dest: src2label2dest.get(src).get(nextLabel)) {
//                            System.out.println("dest: " + dest);
                            for (List<Integer> prefixNodes: currentStepNodes2prefixNodes.get(src)) {
                                vertexPath = new ArrayList<>(prefixNodes);
                                vertexPath.add(src);
                                nextStepNodes2prefixNodes.putIfAbsent(dest, new ArrayList<>());
                                nextStepNodes2prefixNodes.get(dest).add(vertexPath);
                            }
                        }
                    }
                }

                currentStepNodes2prefixNodes = new HashMap<>(nextStepNodes2prefixNodes);
                nextStepNodes2prefixNodes.clear();
                pathStep++;
            }

            for (Integer dest: currentStepNodes2prefixNodes.keySet()) {
                for (List<Integer> prefixNodes: currentStepNodes2prefixNodes.get(dest)) {
//                    System.out.println("prefixNodes: " + prefixNodes);
                    prefixNodes.add(dest);
                    forwardPart.add(prefixNodes);
                }
            }
        }
    }

    private List<List<Integer>> samples(Path path, int numSamples) throws Exception {
        this.path = path;

        Random random = new Random(0);

        List<Integer> edgeLabels = path.getEdgeLabelList();
        int middle = edgeLabels.size() / 2 - (1 - edgeLabels.size() % 2);
        int midLabel = edgeLabels.get(middle);

        int initSrc, initDest;
        List<Integer> pathInstance;
        List<List<Integer>> allPathInstances = new ArrayList<>();

        int sampledIndex;
        Set<Integer> sampledIndices = new HashSet<>();
        List<Pair<Integer, Integer>> sampledSrcDest = new ArrayList<>();
        List<Pair<Integer, Integer>> allSrcDest = label2srcdest.get(midLabel);
        if (allSrcDest.size() > 100000) {
            int numSrcDestSamples = (int) (allSrcDest.size() * 0.01);
            while (sampledSrcDest.size() < numSrcDestSamples) {
                sampledIndex = random.nextInt(allSrcDest.size());
                while (sampledIndices.contains(sampledIndex)) {
                    sampledIndex = random.nextInt(allSrcDest.size());
                }
                sampledIndices.add(sampledIndex);

                sampledSrcDest.add(allSrcDest.get(sampledIndex));
            }
        } else {
            sampledSrcDest = allSrcDest;
        }

        System.out.println("[" + midLabel + "]: START (" + sampledSrcDest.size() + ")");
        for (Pair<Integer, Integer> srcdest: sampledSrcDest) {
            initSrc = srcdest.getKey();
            initDest = srcdest.getValue();

            Runnable backRunnable = new Backward(initSrc);
            Thread backward = new Thread(backRunnable);
            backward.start();

            Runnable forRunnable = new Forward(initDest);
            Thread forward = new Thread(forRunnable);
            forward.start();

            backward.join();
            forward.join();

//            System.out.println("backwardPart:" + backwardPart);
//            System.out.println("forwardPart: " + forwardPart);

            for (List<Integer> prefixNodes: backwardPart) {
                for (List<Integer> suffixNodes: forwardPart) {
                    pathInstance = new ArrayList<>(prefixNodes);
                    pathInstance.addAll(suffixNodes);
                    if (pathInstance.size() != path.length() + 1) continue;

                    allPathInstances.add(pathInstance);
                }
            }
        }

        if (-1 == numSamples) {
            return allPathInstances;
        }

//        System.out.println("All Path Instances: " + allPathInstances);

        // sampling
        List<List<Integer>> samples = new ArrayList<>();
        sampledIndices = new HashSet<>();
        while (samples.size() < numSamples) {
            sampledIndex = random.nextInt(allPathInstances.size());
            while (sampledIndices.contains(sampledIndex)) {
                sampledIndex = random.nextInt(allPathInstances.size());
            }
            sampledIndices.add(sampledIndex);

            samples.add(allPathInstances.get(sampledIndex));
        }

        return samples;
    }

    private static boolean applyFilter(List<Integer> sample, String filter) throws Exception {
        String[] filteringCondition = filter.split(",");
        int vIndex = Integer.parseInt(filteringCondition[0]);
        String operator = filteringCondition[1];
        int literal = Integer.parseInt(filteringCondition[2]);

        if (!propertiesToFilter.containsKey(sample.get(vIndex))) {
            return true;
        }

        switch (operator) {
            case "<":
                return propertiesToFilter.get(sample.get(vIndex)) < literal;
            case ">":
                return propertiesToFilter.get(sample.get(vIndex)) > literal;
            case "<=":
                return propertiesToFilter.get(sample.get(vIndex)) <= literal;
            case ">=":
                return propertiesToFilter.get(sample.get(vIndex)) >= literal;
            case "=":
                return propertiesToFilter.get(sample.get(vIndex)) == literal;
            default:
                throw new Exception("ERROR: unrecognized operator: " + operator);
        }
    }

    private static double computeProportion(List<List<Integer>> samples, String[] filters) throws Exception {
        System.out.println("SAMPLES: " + samples);
        double numQualified = 0;
        for (List<Integer> sample: samples) {
            boolean qualified = true;
            for (String filter: filters) {
                if (!applyFilter(sample, filter)) {
                    qualified = false;
                    break;
                }
            }

            if (qualified) numQualified++;
        }

        double proportion = numQualified / samples.size();
        System.out.println(numQualified + " / " + samples.size() + " = " + proportion);
        return proportion;
    }

    private static void prepareProperties(String[] vertexFiles, String[] indices) throws Exception {
        for (int i = 0; i < vertexFiles.length; ++i) {
            String vertexFile = vertexFiles[i];
            int colIndex = Integer.parseInt(indices[i]);

            BufferedReader tsvReader = new BufferedReader(new FileReader(vertexFile));
            tsvReader.readLine(); // Header

            String tuple = tsvReader.readLine();
            while (null != tuple) {
                String[] info = tuple.split("\t");
                if (!info[colIndex].isEmpty()) {
                    propertiesToFilter.put(Integer.parseInt(info[0]), Integer.parseInt(info[colIndex]));
                }
                tuple = tsvReader.readLine();
            }
            tsvReader.close();
        }
    }

    private static int getCardinality(String mtFile, Path path) throws Exception {
        BufferedReader csvReader = new BufferedReader(new FileReader(mtFile));
        String line = csvReader.readLine();
        while (null != line) {
            String[] pathAndCount = line.split(",");
            String pathString = pathAndCount[0];
            if (pathString.equals(path.toSimpleString())) {
                return Integer.parseInt(pathAndCount[1]);
            }

            line = csvReader.readLine();
        }
        csvReader.close();

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println("path: " + args[1]);
        System.out.println("filters: " + args[2]);
        System.out.println("numSamples: " + args[3]);
        System.out.println("vertexFiles: " + args[4]);
        System.out.println("propertyIndices: " + args[5]);
        System.out.println("mtFile: " + args[6]);
        System.out.println();

        String graphFile = args[0];
        System.out.println("Loading graph");
        MTSampling mtSampling = new MTSampling(graphFile);

        String pathString = args[1];
        List<Integer> edgeLabelList = new ArrayList<>();
        for (String edgeLabel: pathString.split(",")) {
            edgeLabelList.add(Integer.parseInt(edgeLabel));
        }
        Path path = new Path(edgeLabelList);

        String[] filters = args[2].split(";");

        int numSamples = Integer.parseInt(args[3]);
        System.out.println("Sampling");
        List<List<Integer>> samples = mtSampling.samples(path, numSamples);

        mtSampling = null;

        String[] vertexFiles = args[4].split(",");
        String[] vertexPropertyIndices = args[5].split(",");

        System.out.println("Preparing properties");
        propertiesToFilter = new HashMap<>();
        prepareProperties(vertexFiles, vertexPropertyIndices);
//        System.out.println(propertiesToFilter);
        double proportion = computeProportion(samples, filters);
        System.out.println("PROPORTION: " + proportion);

        propertiesToFilter = null;

        String mtFile = args[6];
        System.out.println("Searching for actual cardinality in MT");
        int actualCardinality = getCardinality(mtFile, path);

        System.out.println();
        System.out.println("****** ESTIMATION RESULT ******");
        System.out.println(path.toSimpleString() + "," + Math.ceil(proportion * actualCardinality));
    }
}
