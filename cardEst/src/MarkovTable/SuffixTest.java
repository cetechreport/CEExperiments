package MarkovTable;

import Common.Path;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SuffixTest {
    private static double computeAvgCv(List<Double> cvs) {
        double sum = 0;
        for (double cv: cvs) {
            sum += cv;
        }
        return sum / cvs.size();
    }

    public static void main(String[] args) throws Exception {
        String basePathFile = args[0];
        String longerPathFile = args[1];

        final int CV_EXC0_INDEX = 9;
        final int CV_INC0_INDEX = 14;

        BufferedReader basePathReader = new BufferedReader(new FileReader(basePathFile));

        // basePath -> its own cv
        Map<Path, Double> basePath2cvExc0 = new HashMap<>();
        Map<Path, Double> basePath2cvInc0 = new HashMap<>();
        // basePath -> cvs of longer paths that have basePath as suffix
        Map<Path, List<Double>> basePath2longerPathCvsExc0 = new HashMap<>();
        Map<Path, List<Double>> basePath2longerPathCvsInc0 = new HashMap<>();

        String line = basePathReader.readLine();
        String[] pathAndStat, basePathString;
        List<Integer> labelList;
        Path path;
        double cvExc0, cvInc0;
        int basePathLen = 0;
        while (null != line) {
            pathAndStat = line.split(",");
            basePathString = pathAndStat[0].split("->");
            cvExc0 = Double.parseDouble(pathAndStat[CV_EXC0_INDEX]);
            cvInc0 = Double.parseDouble(pathAndStat[CV_INC0_INDEX]);

            labelList = new ArrayList<>();
            for (String label: basePathString) {
                labelList.add(Integer.parseInt(label));
            }

            path = new Path(labelList);
            basePathLen = path.length();
            basePath2cvExc0.put(path, cvExc0);
            basePath2cvInc0.put(path, cvInc0);
            basePath2longerPathCvsExc0.put(path, new ArrayList<>());
            basePath2longerPathCvsInc0.put(path, new ArrayList<>());

            line = basePathReader.readLine();
        }

        basePathReader.close();

        BufferedReader longerPathReader = new BufferedReader(new FileReader(longerPathFile));
        line = longerPathReader.readLine();
        Path suffix;
        while (null != line) {
            pathAndStat = line.split(",");
            basePathString = pathAndStat[0].split("->");
            cvExc0 = Double.parseDouble(pathAndStat[CV_EXC0_INDEX]);
            cvInc0 = Double.parseDouble(pathAndStat[CV_INC0_INDEX]);

            labelList = new ArrayList<>();
            for (String label: basePathString) {
                labelList.add(Integer.parseInt(label));
            }

            suffix = new Path(labelList.subList(labelList.size() - basePathLen, labelList.size()));
            if (basePath2longerPathCvsExc0.containsKey(suffix) &&
                basePath2longerPathCvsInc0.containsKey(suffix)) {

                basePath2longerPathCvsExc0.get(suffix).add(cvExc0);
                basePath2longerPathCvsInc0.get(suffix).add(cvInc0);
            } else {
                System.out.println(suffix.toSimpleString() + " DOES NOT EXIST");
            }

            line = longerPathReader.readLine();
        }

        longerPathReader.close();

        double avgExc0, avgInc0;
        for (Path basePath: basePath2longerPathCvsExc0.keySet()) {
            avgExc0 = computeAvgCv(basePath2longerPathCvsExc0.get(basePath));
            avgInc0 = computeAvgCv(basePath2longerPathCvsInc0.get(basePath));
            System.out.println(basePath.toSimpleString() + "," + basePath2cvExc0.get(basePath) + ","
                + avgExc0 + "," + basePath2cvInc0.get(basePath) + "," + avgInc0);
        }
    }
}
