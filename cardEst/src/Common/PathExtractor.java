package Common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PathExtractor {
    public static void main(String[] args) throws Exception {
        String csvFilePath = args[0];
        int pathLength = Integer.parseInt(args[1]);
        int numLabelsLimit = args.length > 2 ? Integer.parseInt(args[2]) : -1;

        // label->src->list_of_dest
        Map<Integer, Map<Integer, List<Integer>>> label2src2dest = new HashMap<>();
        // src->label->list_of_dest
        Map<Integer, Map<Integer, List<Integer>>> src2label2dest = new HashMap<>();

        int[] tripleList;
        BufferedReader csvReader = new BufferedReader(new FileReader(csvFilePath));
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            tripleList = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt).toArray();

            label2src2dest.putIfAbsent(tripleList[1], new HashMap<>());
            label2src2dest.get(tripleList[1]).putIfAbsent(tripleList[0], new ArrayList<>());
            label2src2dest.get(tripleList[1]).get(tripleList[0]).add(tripleList[2]);

            src2label2dest.putIfAbsent(tripleList[0], new HashMap<>());
            src2label2dest.get(tripleList[0]).putIfAbsent(tripleList[1], new ArrayList<>());
            src2label2dest.get(tripleList[0]).get(tripleList[1]).add(tripleList[2]);

            tripleString = csvReader.readLine();
        }

        Set<Path> paths = new HashSet<>();

        int i = 0;
        for (int label : label2src2dest.keySet()) {
            System.err.print(".");
            if (i > 0 && i % 10 == 0) System.err.println();
            i++;

            Map<Integer, List<Integer>> allFirstEdges = label2src2dest.get(label);

            for (List<Integer> firstDests: allFirstEdges.values()) {
                for (int firstDest : firstDests) {

                    Set<Pair<Integer, List<Integer>>> prevDests = new HashSet<>();
                    List<Integer> prefix = new ArrayList<>();
                    prefix.add(label);
                    prevDests.add(new Pair<>(firstDest, prefix));

                    boolean hasLongEnoughPath = true;

                    for (int p = 1; p < pathLength; ++p) {
                        Set<Pair<Integer, List<Integer>>> currentDests = new HashSet<>();

                        for (Pair<Integer, List<Integer>> prevDest : prevDests) {
                            if (!src2label2dest.containsKey(prevDest.getKey())) continue;

                            Map<Integer, List<Integer>> nextEdges = src2label2dest.get(prevDest.getKey());

                            int numLabels = 0;
                            for (int nextLabel : nextEdges.keySet()) {
                                for (int nextDest : nextEdges.get(nextLabel)) {
                                    List<Integer> nextPrefix = new ArrayList<>(prevDest.getValue());
                                    nextPrefix.add(nextLabel);
                                    currentDests.add(new Pair<>(nextDest, nextPrefix));
                                }
                            }
                            numLabels++;
                            if (numLabelsLimit > 0 && numLabels >= numLabelsLimit) break;
                        }

                        prevDests = currentDests;

                        if (prevDests.isEmpty()) {
                            hasLongEnoughPath = false;
                            break;
                        }
                    }

                    if (hasLongEnoughPath) {
                        for (Pair<Integer, List<Integer>> labelList : prevDests) {
                            paths.add(new Path(labelList.getValue()));
                        }
                    }
                }
            }
        }

        for (Path path : paths) {
            System.out.println(path.toCsv());
        }
    }
}