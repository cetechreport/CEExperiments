package MarkovTable;

import Common.Evaluation;
import Common.Path;
import Common.TrueSimplePathCardinality;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SingleQueryEvaluation extends Evaluation {
    public static Set<Integer> getLabelsForPartition(String partitionFileName) throws Exception {
        BufferedReader csvReader = new BufferedReader(new FileReader(partitionFileName));
        String labelString = csvReader.readLine();
        int[] labelList = Arrays.stream(labelString.split(",")).mapToInt(Integer::parseInt).toArray();
        csvReader.close();
        Set<Integer> labels = new HashSet<>();
        for (int label : labelList) {
            labels.add(label);
        }
        return labels;
    }

    private static boolean mayHavePath(Set<Integer> labels, Path path) {
        Set<Integer> pathLabels = new HashSet<>(path.getEdgeLabelList());
        return !pathLabels.retainAll(labels);
    }

    public static void main(String[] args) throws Exception {
        boolean skipTrue = Boolean.parseBoolean(args[4]);
        String[] trueCardFiles = args.length > 5 ? args[5].split(",") : new String[0];
        File fileOrDir = new File(args[0]);
        String[] partitionFiles;
        boolean isOriginalGraphData;
        if (fileOrDir.isDirectory()) {
            partitionFiles = fileOrDir.list();
            isOriginalGraphData = false;
        } else {
            partitionFiles = new String[1];
            partitionFiles[0] = args[0];
            isOriginalGraphData = true;
        }

        // process query path(s)
        String pathOrFile = args[2];
        Path[] paths;
        List<List<String>> pathList = new ArrayList<>();
        if (pathOrFile.contains("file")) {
            BufferedReader pathsReader = new BufferedReader(new FileReader(args[3]));
            String pathString = pathsReader.readLine();
            while (null != pathString) {
                pathList.add(new ArrayList<>(Arrays.asList(pathString.trim().split(","))));
                pathString = pathsReader.readLine();
            }
            pathsReader.close();
        } else {
            String[] targetPath = args[3].split(",");
            pathList.add(new ArrayList<>(Arrays.asList(targetPath)));
        }

        paths = new Path[pathList.size()];
        for (int i = 0; i < paths.length; ++i) {
            List<String> path = pathList.get(i);
            List<Integer> edgeLabelList = new ArrayList<>();
            for (String label : path) {
                edgeLabelList.add(Integer.parseInt(label));
            }
            paths[i] = new Path(edgeLabelList);
        }

        String[] filteredPartitionFiles = partitionFiles;
        Map<String, MarkovTable> partition2MT = new HashMap<>();
        MarkovTable markovTable;

        for (Path path : paths) {
            // if term_inc partitioning, only consider the files with same starting label
            if (args[0].contains("term_inc")) {
                filteredPartitionFiles = Arrays.stream(partitionFiles)
                    .filter(s -> s.contains(Integer.toString(path.getEdgeLabelList().get(0))))
                    .toArray(String[]::new);
            }

            long numErrors = 0;
            double totalEst = 0;

            for (int i = 0; i < filteredPartitionFiles.length; ++i) {
                if (!isOriginalGraphData) {
                    Set<Integer> partitionLabels = getLabelsForPartition(args[0] + filteredPartitionFiles[i]);
                    if (!mayHavePath(partitionLabels, path)) continue;
                }

                String output = "--------- PARTITION " + filteredPartitionFiles[i] + " ---------\n";
                output += "Building Markov table...: " + args[1] + "\n";

                if (partition2MT.containsKey(filteredPartitionFiles[i])) {
                    markovTable = partition2MT.get(filteredPartitionFiles[i]);
                } else {
                    if (isOriginalGraphData) {
                        markovTable = new MarkovTable(
                            filteredPartitionFiles[i], Integer.parseInt(args[1]), false, false
                        );
//                        markovTable.suffixShrink(Integer.parseInt(args[5]));
//                        markovTable.grow(trueCardFiles);
//                        markovTable.randomGrow(trueCardFiles);
                    } else {
                        markovTable = new MarkovTable(
                            args[0] + filteredPartitionFiles[i], Integer.parseInt(args[1]),
                            false, true
                        );
                    }
                    partition2MT.put(filteredPartitionFiles[i], markovTable);
                }

                // TODO: TrueSimplePathCardinality needs to ignore the first line of partition file as well
                double actual = -1;
                if (!skipTrue) {
                    output += "Building stats for actual cardinalities...\n";
                    TrueSimplePathCardinality trueSimplePathCardinality = new TrueSimplePathCardinality(args[0] + filteredPartitionFiles[i]);
                    actual = trueSimplePathCardinality.compute(path);
                }

                double est = Math.ceil(markovTable.estimate(path));

                totalEst += est;

                if (est != actual) numErrors++;

                double re = computeRelativeError(est, actual);
                output += path.toString() + " (RE): " + (skipTrue ? "--" : actual) + "," + " " +
                    est + ", " + re + "\n";

                if (Math.round(est) != 0 || (!skipTrue && Math.round(actual) != 0)) {
                    System.out.println(output);
                }
            }

            System.out.println("--------- FINAL RESULT ---------");
            System.out.println("# ERRORS: " + numErrors);
            System.out.println("TOTAL ESTIMATION " + "(" + path.toString() + "): " + totalEst);
        }
    }
}
