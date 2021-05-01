package MarkovTable;

import Common.Evaluation;
import Common.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class PathFileQueryEvaluation extends Evaluation {
    private static Path pathStringToPath(String pathString) {
        String[] labels = pathString.split(",");
        List<Integer> labelList = new ArrayList<>();
        for (String label : labels) {
            labelList.add(Integer.parseInt(label));
        }
        return new Path(labelList);
    }

    public static void main(String[] args) throws Exception {
        String mtFile = args[0];
        int mtPathLen = Integer.parseInt(args[1]);
        String pathFileName = args[2];

        BufferedReader csvReader = new BufferedReader(new FileReader(pathFileName));
        String pathString = csvReader.readLine();

        MarkovTable markovTable = new MarkovTable(mtFile, mtPathLen);
        Path query;
        double est;

        while (null != pathString) {
            query = pathStringToPath(pathString);

            est = Math.ceil(markovTable.estimate(query));

            System.out.println("ESTIMATION: " + query.toSimpleString() + "," + est);

            pathString = csvReader.readLine();
        }

        System.out.println("DONE!");
    }
}
