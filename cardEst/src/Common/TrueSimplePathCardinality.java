package Common;

import RDF3X.RdfTriple;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class TrueSimplePathCardinality {

    public Map<Integer, Map<Integer, List<Integer>>> allTriples;
    public Map<Integer, List<Integer>> pred2objs;

    public TrueSimplePathCardinality(String spFilePath) throws Exception {
        // use BFS from every single node? maybe ask Semih
        this.allTriples = new HashMap<>();
        this.pred2objs = new HashMap<>();

        BufferedReader csvReader = new BufferedReader(new FileReader(spFilePath));

        int[] tripleList;
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            tripleList = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt).toArray();

            // add the triple into the triple store
            allTriples.putIfAbsent(tripleList[0], new HashMap<>());
            allTriples.get(tripleList[0]).putIfAbsent(tripleList[1], new ArrayList<>());
            allTriples.get(tripleList[0]).get(tripleList[1]).add(tripleList[2]);

            pred2objs.putIfAbsent(tripleList[1], new ArrayList<>());
            pred2objs.get(tripleList[1]).add(tripleList[2]);

            tripleString = csvReader.readLine();
        }

        csvReader.close();
    }

    public double compute(RdfTriple[] path) {
        List<Integer> currentPredObjs = pred2objs.get(path[0].second);
        List<Integer> nextStepObjs = new ArrayList<>();

        Integer obj;
        int pathStep = 1;
        while (pathStep < path.length) {
            for (int i = 0; i < currentPredObjs.size(); ++i) {
                obj = currentPredObjs.get(i);

                if (allTriples.containsKey(obj) && allTriples.get(obj).containsKey(path[pathStep].second)) {
                    nextStepObjs.addAll(allTriples.get(obj).get(path[pathStep].second));
                }
            }
            currentPredObjs = new ArrayList<>(nextStepObjs);
            nextStepObjs = new ArrayList<>();
            pathStep++;
        }

        return currentPredObjs.size();
    }

    public boolean doesTripleExist(RdfTriple triple) {
        return allTriples.containsKey(triple.first) &&
               allTriples.get(triple.first).containsKey(triple.second) &&
               allTriples.get(triple.first).get(triple.second).contains(triple.third);
    }

    public int compute(Path path) {
        List<Integer> edgeLabelList = path.getEdgeLabelList();
        List<Integer> srcVertexList = path.getSrcVertexList();

        if (srcVertexList.get(0) >= 0) {
            if (!allTriples.containsKey(srcVertexList.get(0)) ||
                !allTriples.get(srcVertexList.get(0)).containsKey(edgeLabelList.get(0))) return 0;
        } else {
            if (!pred2objs.containsKey(path.getEdgeLabelList().get(0))) return 0;
        }

        List<Integer> currentDests;
        if (srcVertexList.get(0) >= 0) {
            currentDests = allTriples.get(srcVertexList.get(0)).get(edgeLabelList.get(0));
        } else {
            currentDests = pred2objs.get(path.getEdgeLabelList().get(0));
        }

        Integer dest;
        int pathStep = 1;
        while (pathStep < path.length()) {
            List<Integer> nextStepDests = new ArrayList<>();
            for (int i = 0; i < currentDests.size(); ++i) {
                dest = currentDests.get(i);

                if (srcVertexList.get(pathStep) >= 0 && !dest.equals(srcVertexList.get(pathStep)))
                    continue;

                if (allTriples.containsKey(dest) && allTriples.get(dest).containsKey(
                    edgeLabelList.get(pathStep))) {
                    nextStepDests.addAll(allTriples.get(dest).get(path.getEdgeLabelList()
                        .get(pathStep)));
                }
            }
            currentDests = new ArrayList<>(nextStepDests);
            pathStep++;
        }

        return currentDests.size();
    }

    public static void main(String[] args) throws Exception {
        String graphFile = args[0];
        String pathFile = args[1];
        TrueSimplePathCardinality trueSimplePathCardinality = new TrueSimplePathCardinality(graphFile);
        BufferedReader csvReader = new BufferedReader(new FileReader(pathFile));

        List<Integer> labelList;
        int cardinality;
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            labelList = new ArrayList<>();
            for (String label : tripleString.split(",")) {
                labelList.add(Integer.parseInt(label));
            }

            cardinality = trueSimplePathCardinality.compute(new Path(labelList));

            System.out.println(tripleString + "," + cardinality);

            tripleString = csvReader.readLine();
        }
    }
}
