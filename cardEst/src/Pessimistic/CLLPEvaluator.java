package Pessimistic;

import Common.Query;
import Graphflow.Constants;
import org.apache.commons.lang3.time.StopWatch;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class CLLPEvaluator {
    List<String> trueCardFiles = new ArrayList<>();
    List<String> catFiles = new ArrayList<>();
    List<String> catMaxDegFiles = new ArrayList<>();

    Map<Integer, Long> maxOutDeg = new HashMap<>();
    Map<Integer, Long> maxInDeg = new HashMap<>();
    Map<Integer, Long> labelCount = new HashMap<>();

    // decomVListString (defining decom topology) -> edge label seq -> count
    Map<String, Map<String, Long>> catalogue;

    // baseVList -> baseLabelSeq -> extVList -> extLabel -> maxDeg
    Map<String, Map<String, Map<String, Map<String, Integer>>>> catalogueMaxDeg;

    private void getAllFiles(String path, List<String> files) {
        try (Stream<Path> paths = Files.walk(Paths.get(path))) {
            paths.forEach(filePath -> {
                if (Files.isRegularFile(filePath)) {
                    files.add(filePath.toString());
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

        Collections.sort(files);
    }

    public void estimate() throws Exception {
        StopWatch watch = new StopWatch();
        watch.start();

        int total = trueCardFiles.size();
        double progress = 0;

        for (int i = 0; i < trueCardFiles.size(); ++i) {
            List<Query> queries = Query.readQueries(trueCardFiles.get(i));
            readCatalogue(catFiles.get(i), 2);
            readCatalogueMaxDeg(catMaxDegFiles.get(i));

            BufferedWriter resultWriter = new BufferedWriter(new FileWriter("estimation" + (i+1) + ".csv"));

            CLLP cllp = new CLLP(null, null, null, catalogue, catalogueMaxDeg);
            for (Query query : queries) {
                double estWithoutSubmod = cllp.estimate(query, false);
                double estWithSubmod = cllp.estimate(query, true);

                resultWriter.write(query.toString() + "," + estWithoutSubmod + "," + estWithSubmod + "\n");
            }

            resultWriter.close();

            progress += 100.0 / total;
            System.out.print("\rEstimating: " + (int) progress + "%");
        }

        watch.stop();
        System.out.println("\rEstimating: " + (watch.getTime() / 1000.0) + " sec");
    }

    public void prepare(
        String catDir, String catMaxDegDir, String trueDir) throws Exception {
//        readMaxDeg(maxDegFile);
//        readLabelCount(labelcountFile);
        getAllFiles(catDir, catFiles);
        getAllFiles(catMaxDegDir, catMaxDegFiles);
        getAllFiles(trueDir, trueCardFiles);
    }

    protected void readMaxDeg(String maxDegFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader reader = new BufferedReader(new FileReader(maxDegFile));
        String line = reader.readLine();
        while (line != null) {
            String[] info = line.split(",");
            Integer dir = Integer.parseInt(info[0]);
            if (dir.equals(Constants.FORWARD)) {
                maxOutDeg.put(Integer.parseInt(info[1]), Long.parseLong(info[2]));
            } else {
                maxInDeg.put(Integer.parseInt(info[1]), Long.parseLong(info[2]));
            }
            line = reader.readLine();
        }
        reader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Loading MaxDeg: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    protected void readLabelCount(String labelCountFile) throws Exception {
        long startTime = System.currentTimeMillis();
        long endTime;

        BufferedReader reader = new BufferedReader(new FileReader(labelCountFile));
        String line = reader.readLine();
        while (line != null) {
            String[] info = line.split(",");
            labelCount.put(Integer.parseInt(info[0]), Long.parseLong(info[1]));
            line = reader.readLine();
        }
        reader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Loading LabelCount: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public void readCatalogue(String catalogueFile, int maxLen) throws Exception {
        catalogue = new HashMap<>();

        BufferedReader catalogueReader = new BufferedReader(new FileReader(catalogueFile));
        String[] info;
        String vList, labelSeq;
        String line = catalogueReader.readLine();
        while (null != line) {
            info = line.split(",");

            vList = info[3];
            labelSeq = info[4];

            if (labelSeq.split("->").length <= maxLen) {
                Long count = Long.parseLong(info[5]);
                catalogue.putIfAbsent(vList, new HashMap<>());
                catalogue.get(vList).put(labelSeq, count);
            }

            line = catalogueReader.readLine();
        }
        catalogueReader.close();
    }

    protected void readCatalogueMaxDeg(String catalogueMaxDegFile) throws Exception {
        catalogueMaxDeg = new HashMap<>();
        BufferedReader reader = new BufferedReader(new FileReader(catalogueMaxDegFile));
        String line = reader.readLine();
        while (line != null) {
            String[] info = line.split(",");
            catalogueMaxDeg.putIfAbsent(info[3], new HashMap<>());
            catalogueMaxDeg.get(info[3]).putIfAbsent(info[4], new HashMap<>());
            catalogueMaxDeg.get(info[3]).get(info[4]).putIfAbsent(info[5], new HashMap<>());
            catalogueMaxDeg.get(info[3]).get(info[4]).get(info[5])
                .putIfAbsent(info[6], Integer.parseInt(info[7]));
            line = reader.readLine();
        }
        reader.close();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("catDir: " + args[0]);
        System.out.println("catMaxDegDir: " + args[1]);
        System.out.println("trueCardDir: " + args[2]);
        System.out.println();

        CLLPEvaluator evaluator = new CLLPEvaluator();
        evaluator.prepare(args[0], args[1], args[2]);
        evaluator.estimate();
    }
}
