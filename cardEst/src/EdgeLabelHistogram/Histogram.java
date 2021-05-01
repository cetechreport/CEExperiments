package EdgeLabelHistogram;

import Common.Path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Histogram {
    private Map<Integer, Integer> vertex2outDeg;
    private Map<Integer, Integer> edgeLabel2occ;
    private Map<Integer, List<Integer>> edgeLabel2dests;
    public Map<Integer, Map<Integer, Integer>> edgeLabel2src2occ;
    private int totalNumEdges;

    public Histogram(String csvFilePath) throws Exception {
        vertex2outDeg = new HashMap<>();
        edgeLabel2occ = new HashMap<>();
        edgeLabel2dests = new HashMap<>();
        edgeLabel2src2occ = new HashMap<>();
        totalNumEdges = 0;

        BufferedReader csvReader = new BufferedReader(new FileReader(csvFilePath));
        int[] line;
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            line = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt).toArray();

            vertex2outDeg.put(line[0], vertex2outDeg.getOrDefault(line[0], 0) + 1);
            edgeLabel2occ.put(line[1], edgeLabel2occ.getOrDefault(line[1], 0) + 1);

            edgeLabel2dests.putIfAbsent(line[1], new ArrayList<>());
            edgeLabel2dests.get(line[1]).add(line[2]);

            edgeLabel2src2occ.putIfAbsent(line[1], new HashMap<>());
            edgeLabel2src2occ.get(line[1]).put(line[0], edgeLabel2src2occ.get(line[1]).
                getOrDefault(line[0], 0) + 1);

            totalNumEdges++;

            tripleString = csvReader.readLine();
        }
    }

    public double estimateWithUniformity(Path path) {
        List<Integer> srcVertexList = path.getSrcVertexList();

        double result = 1;
        Integer edgeLabel;
        for (int i = 0; i < path.getEdgeLabelList().size(); ++i) {
            edgeLabel = path.getEdgeLabelList().get(i);
            if (srcVertexList.get(i) >= 0) {
                result *= ((double) edgeLabel2src2occ.get(edgeLabel).getOrDefault(srcVertexList.get(i), 0))
                    / totalNumEdges;
            } else {
                result *= ((double) edgeLabel2occ.get(edgeLabel)) / totalNumEdges;
            }
        }

        result *= 1.0 / Math.pow(vertex2outDeg.size(), path.length() - 1);

        for (int i = 0; i < path.length(); ++i) {
            result *= (totalNumEdges - i);
        }

        return result;
    }

    public double estimateWithOutDegree(Path path) {
        double result = 1;
        for (int i = 0; i < path.length(); ++i) {
            Integer edgeLabel = path.getEdgeLabelList().get(i);
            result *= (((double) edgeLabel2occ.getOrDefault(edgeLabel, 0)) / totalNumEdges);

            if (edgeLabel2dests.containsKey(edgeLabel) && i < path.length() - 1) {
                int totalOutDegree = 0;
                for (Integer dest : edgeLabel2dests.get(edgeLabel)) {
                    totalOutDegree += vertex2outDeg.getOrDefault(dest, 0);
                }
                result *= (((double) totalOutDegree) / totalNumEdges);
            }
        }

        for (int i = 0; i < path.length(); ++i) {
            result *= (totalNumEdges - i);
        }

        return result;
    }

    public void save(String edgeLabelDest, String srcVertexDest) throws Exception {
        BufferedWriter edgeLabelWriter = new BufferedWriter(new FileWriter(edgeLabelDest));
        edgeLabelWriter.write("EdgeLabel,#Occurrences\n");
        for (Integer edgeLabel : edgeLabel2occ.keySet()) {
            edgeLabelWriter.write(edgeLabel + "," + edgeLabel2occ.get(edgeLabel) + "\n");
        }
        edgeLabelWriter.close();

        BufferedWriter srcVertexWriter = new BufferedWriter(new FileWriter(srcVertexDest));
        srcVertexWriter.write("EdgeLabel,SrcVertex,#Occurrences\n");
        for (Integer edgeLabel : edgeLabel2src2occ.keySet()) {
            for (Integer srcVertex : edgeLabel2src2occ.get(edgeLabel).keySet()) {
                srcVertexWriter.write(
                    edgeLabel + "," + srcVertex + "," + edgeLabel2src2occ.get(edgeLabel).get(srcVertex) + "\n"
                );
            }
        }
        srcVertexWriter.close();
    }
}
