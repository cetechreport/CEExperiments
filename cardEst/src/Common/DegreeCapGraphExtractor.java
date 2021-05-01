package Common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DegreeCapGraphExtractor {
    Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();
    Map<Integer, Map<Integer, List<Integer>>> dest2label2src = new HashMap<>();

    private void capDegree(long inMin, long inMax, long outMin, long outMax, String destFile)
        throws Exception{
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedWriter writer = new BufferedWriter(new FileWriter(destFile));

        for (Integer src : src2label2dest.keySet()) {
            for (Integer label : src2label2dest.get(src).keySet()) {
                int outDeg = src2label2dest.get(src).get(label).size();
                if (outDeg >= outMin && outDeg <= outMax) {
                    for (Integer dest : src2label2dest.get(src).get(label)) {
                        int inDeg = dest2label2src.get(dest).get(label).size();
                        if (inDeg >= inMin && inDeg <= inMax) {
                            writer.write(src + "," + label + "," + dest + "\n");
                        }
                    }
                }
            }
        }

        writer.close();

        endTime = System.currentTimeMillis();
        System.out.println("Graph Extracting: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private Pair<Integer, Integer> calcDegMinMax(
        Map<Integer, Map<Integer, List<Integer>>> v2label2v, double minPercent, double maxPercent) {

        List<Integer> allDeg = new ArrayList<>();
        for (Integer v : v2label2v.keySet()) {
            for (Integer label : v2label2v.get(v).keySet()) {
                allDeg.add(v2label2v.get(v).get(label).size());
            }
        }

        Collections.sort(allDeg);

        int minIndex = (int) ((allDeg.size() - 1) * minPercent);
        int maxIndex = (int) ((allDeg.size() - 1) * maxPercent);

        return new Pair<>(allDeg.get(minIndex), allDeg.get(maxIndex));
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

            tripleString = csvReader.readLine();
        }

        endTime = System.currentTimeMillis();
        System.out.println("Graph Loading: " + ((endTime - startTime) / 1000.0) + " sec");

        csvReader.close();
    }

    DegreeCapGraphExtractor(String inputGraph) throws Exception {
        readGraph(inputGraph);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("inputGraph: " + args[0]);
        System.out.println("minPercent: " + args[1]);
        System.out.println("maxPercent: " + args[2]);
        System.out.println("outputGraph: " + args[3]);
        System.out.println();

        double minPercent = Double.parseDouble(args[1]);
        double maxPercent = Double.parseDouble(args[2]);

        DegreeCapGraphExtractor extractor = new DegreeCapGraphExtractor(args[0]);
        Pair<Integer, Integer> inDeg =
            extractor.calcDegMinMax(extractor.dest2label2src, minPercent, maxPercent);
        Pair<Integer, Integer> outDeg =
            extractor.calcDegMinMax(extractor.src2label2dest, minPercent, maxPercent);
        extractor.capDegree(inDeg.key, inDeg.value, outDeg.key, outDeg.value, args[3]);

        System.out.println("minInDeg: " + inDeg.key);
        System.out.println("maxInDeg: " + inDeg.value);
        System.out.println("minOutDeg: " + outDeg.key);
        System.out.println("maxOutDeg: " + outDeg.value);
    }
}
