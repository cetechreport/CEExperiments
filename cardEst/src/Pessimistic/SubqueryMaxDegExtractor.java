package Pessimistic;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.StringJoiner;

public class SubqueryMaxDegExtractor {
    private void extractMaxDeg(String distFile, String maxDegFile) throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(maxDegFile));
        BufferedReader reader = new BufferedReader(new FileReader(distFile));
        String line = reader.readLine();
        String[] info;
        while (line != null) {
            info = line.split(",");
            String patternType = info[0];
            String baseVList = info[1];
            String baseLabelSeq = info[2];
            String extVList = info[3];
            String extLabel = info[4];

            int maxDeg = Integer.MIN_VALUE;
            for (int i = 5; i < info.length; i += 2) {
                Integer deg = Integer.parseInt(info[i]);
                Long numInstances = Long.parseLong(info[i + 1]);
                if (!numInstances.equals(0L)) {
                    maxDeg = Math.max(maxDeg, deg);
                }
            }

            StringJoiner maxDegEntry = new StringJoiner(",");
            maxDegEntry.add(patternType).add(baseVList).add(baseLabelSeq).add(extVList).add(extLabel);
            maxDegEntry.add(Integer.toString(maxDeg));
            writer.write(maxDegEntry.toString() + "\n");

            line = reader.readLine();
        }
        reader.close();
        writer.close();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("distFile: " + args[0]);
        System.out.println("maxDegFile: " + args[1]);
        System.out.println();

        SubqueryMaxDegExtractor extractor = new SubqueryMaxDegExtractor();
        extractor.extractMaxDeg(args[0], args[1]);
    }
}
