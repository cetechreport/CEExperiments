package TrueCardinality;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class PatternSampler {
    final int NUM_SAMPLES = 20;
    final int NUM_LINES = 100;

    private void sample(String patternFile, String destFile) throws Exception {
        String patternType = "";
        String vList = "";
        Set<String> labelSeqSet = new HashSet<>();

        BufferedReader reader = new BufferedReader(new FileReader(patternFile));
        String line = reader.readLine();
        while (line != null) {
            String[] info = line.split(",");
            patternType = info[0];
            vList = info[1];
            for (int i = 2; i < info.length; ++i) {
                labelSeqSet.add(info[i]);
            }

            line = reader.readLine();
        }
        reader.close();

        List<String> labelSeqs = new ArrayList<>(labelSeqSet);

        Set<Integer> sampledIndices = new HashSet<>();
        Random random = new Random(0);

        List<String> samples = new ArrayList<>();

        if (labelSeqs.size() > NUM_SAMPLES) {
            int sampledIndex;
            while (samples.size() < NUM_SAMPLES) {
                sampledIndex = random.nextInt(labelSeqs.size());
                while (sampledIndices.contains(sampledIndex)) {
                    sampledIndex = random.nextInt(labelSeqs.size());
                }
                sampledIndices.add(sampledIndex);

                samples.add(labelSeqs.get(sampledIndex));
            }
        } else {
            samples.addAll(labelSeqs);
        }

        BufferedWriter writer = new BufferedWriter(new FileWriter(destFile));
        writer.write(patternType + "," + vList);
        for (int i = 0; i < NUM_SAMPLES; ++i) {
            writer.write("," + samples.get(i));
        }
        writer.write("\n");
        writer.close();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("patternFile: " + args[0]);
        System.out.println("destFile: " + args[1]);
        System.out.println();

        PatternSampler sampler = new PatternSampler();
        sampler.sample(args[0], args[1]);
    }
}
