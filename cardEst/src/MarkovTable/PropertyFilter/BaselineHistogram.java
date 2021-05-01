package MarkovTable.PropertyFilter;

import Common.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BaselineHistogram {
    Map<Integer, Integer> distribution;
    int totalNum = 0;

    private static Map<String, BaselineHistogram> vertexFile2histogram;

    public BaselineHistogram(String vertexFile, int propertyIndex) throws Exception {
        distribution = new HashMap<>();
        BufferedReader tsvReader = new BufferedReader(new FileReader(vertexFile));
        tsvReader.readLine(); // Header
        String line = tsvReader.readLine();
        while (null != line) {
            String[] info = line.split("\t");
            totalNum++;
            if (!info[propertyIndex].isEmpty()) {
                int propertyValue = Integer.parseInt(info[propertyIndex]);
                distribution.put(propertyValue, distribution.getOrDefault(propertyValue, 0) + 1);
            }
            line = tsvReader.readLine();
        }
        tsvReader.close();
    }

    private static double computeProportion(String[] vertexFiles, String filter) throws Exception {
        String[] filteringCondition = filter.split(",");
        int vertexFileIndex = Integer.parseInt(filteringCondition[0]);
        String operator = filteringCondition[1];
        int literal = Integer.parseInt(filteringCondition[2]);

        Map<Integer, Integer> distribution =
            vertexFile2histogram.get(vertexFiles[vertexFileIndex]).distribution;
        double numQualified = 0;
        for (int property: distribution.keySet()) {
            switch (operator) {
                case "<":
                    if (property < literal) {
                        numQualified += distribution.get(property);
                    }
                    break;
                case ">":
                    if (property > literal) {
                        numQualified += distribution.get(property);
                    }
                    break;
                case "<=":
                    if (property <= literal) {
                        numQualified += distribution.get(property);
                    }
                    break;
                case ">=":
                    if (property >= literal) {
                        numQualified += distribution.get(property);
                    }
                    break;
                case "=":
                    if (property == literal) {
                        numQualified += distribution.get(property);
                    }
                    break;
                default:
                    throw new Exception("ERROR: unrecognized operator: " + operator);
            }
        }
        return numQualified / vertexFile2histogram.get(vertexFiles[vertexFileIndex]).totalNum;
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

    private void print() {
        for (int attribute : distribution.keySet()) {
            System.out.println(attribute + "," + distribution.get(attribute));
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("vertexFiles: " + args[0]);
        System.out.println("propertyIndices: " + args[1]);
        System.out.println("path: " + args[2]);
        System.out.println("filters: " + args[3]);
        System.out.println("mtFile: " + args[4]);
        System.out.println();

        String[] vertexFiles = args[0].split(",");
        String[] propertyIndices = args[1].split(",");
        String pathString = args[2];
        String[] filters = args[3].split(";");
        String mtFile = args[4];

        vertexFile2histogram = new HashMap<>();

        System.out.println("Constructing histograms");
        for (int i = 0; i < vertexFiles.length; ++i) {
            String vertexFile = vertexFiles[i];
            int index = Integer.parseInt(propertyIndices[i]);
            BaselineHistogram histogram = new BaselineHistogram(vertexFile, index);
            vertexFile2histogram.put(vertexFile, histogram);

            System.out.println(vertexFile + " - " + index);
            histogram.print();
        }

        List<Integer> edgeLabelList = new ArrayList<>();
        for (String edgeLabel: pathString.split(",")) {
            edgeLabelList.add(Integer.parseInt(edgeLabel));
        }

        Path path = new Path(edgeLabelList);

        System.out.println("Computing proportion");
        double proportion = 1.0;
        for (String filter: filters) {
            proportion *= computeProportion(vertexFiles, filter);
        }
        System.out.println("PROPORTION: " + proportion);

        System.out.println("Searching for actual cardinality in MT");
        int actualCardinality = getCardinality(mtFile, path);

        System.out.println();
        System.out.println("****** ESTIMATION RESULT ******");
        System.out.println(path.toSimpleString() + "," + Math.ceil(proportion * actualCardinality));
    }
}
