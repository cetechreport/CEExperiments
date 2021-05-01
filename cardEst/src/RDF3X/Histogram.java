package RDF3X;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Histogram {
    public HistogramType type;
    public ArrayList<HistogramBucket> buckets;
    public String dataFilePath;
    public int numTriples;

    public Histogram(String csvFilePath, HistogramType histogramType) throws Exception {
        String[] pathStrList = csvFilePath.split("(\\.)|/");
        this.dataFilePath = pathStrList[pathStrList.length - 2];
        this.type = histogramType;
        this.buckets = new ArrayList<>();

        HashMap<Integer, HashMap<Integer, ArrayList<Integer>>> allTriples = new HashMap<>();

        HashMap<Integer, Integer> first2numFirst = new HashMap<>();
        HashMap<Integer, Integer> first2numSec = new HashMap<>();
        HashMap<Integer, Integer> first2numThird = new HashMap<>();
        HashMap<Integer, Integer> sec2numFirst = new HashMap<>();
        HashMap<Integer, Integer> sec2numSec = new HashMap<>();
        HashMap<Integer, Integer> sec2numThird = new HashMap<>();
        HashMap<Integer, Integer> third2numFirst = new HashMap<>();
        HashMap<Integer, Integer> third2numSec = new HashMap<>();
        HashMap<Integer, Integer> third2numThird = new HashMap<>();

        BufferedReader csvReader = new BufferedReader(new FileReader(csvFilePath));
        int[] tripleList;
        String tripleString = csvReader.readLine();
        while (null != tripleString) {
            tripleList = Arrays.stream(tripleString.split(",")).mapToInt(Integer::parseInt)
                .toArray();
            numTriples++;

            // add the triple into the triple store
            allTriples.putIfAbsent(tripleList[0], new HashMap<>());
            allTriples.get(tripleList[0]).putIfAbsent(tripleList[1], new ArrayList<>());
            allTriples.get(tripleList[0]).get(tripleList[1]).add(tripleList[2]);

            first2numFirst.put(tripleList[0], first2numFirst.getOrDefault(tripleList[0], 0) + 1);

            // create an entry if not yet exists
            first2numSec.putIfAbsent(tripleList[0], 0);
            // for those 2nd that have not yet confirmed to have an corresponding 1st,
            // we use negative counting
            if (first2numSec.containsKey(tripleList[1])) {
                int current = first2numSec.get(tripleList[1]);
                if (current >= 0) {
                    first2numSec.put(tripleList[1], current + 1);
                } else {
                    first2numSec.put(tripleList[1], current - 1);
                }
            } else {
                first2numSec.put(tripleList[1], -1);
            }
            if (first2numSec.get(tripleList[0]) < 0) {
                first2numSec.put(tripleList[0], Math.abs(first2numSec.get(tripleList[0])));
            }

            // create an entry if not yet exists
            first2numThird.putIfAbsent(tripleList[0], 0);
            // for those 3rd that have not yet confirmed to have an corresponding 1st,
            // we use negative counting
            if (first2numThird.containsKey(tripleList[2])) {
                int current = first2numThird.get(tripleList[2]);
                if (current >= 0) {
                    first2numThird.put(tripleList[2], current + 1);
                } else {
                    first2numThird.put(tripleList[2], current - 1);
                }
            } else {
                first2numThird.put(tripleList[2], -1);
            }
            if (first2numThird.get(tripleList[0]) < 0) {
                first2numThird.put(tripleList[0], Math.abs(first2numThird.get(tripleList[0])));
            }

            sec2numSec.put(tripleList[1], sec2numSec.getOrDefault(tripleList[1], 0) + 1);

            // create an entry if not yet exists
            sec2numFirst.putIfAbsent(tripleList[1], 0);
            // for those 1st that have not yet confirmed to have an corresponding 2nd,
            // we use negative counting
            if (sec2numFirst.containsKey(tripleList[0])) {
                int current = sec2numFirst.get(tripleList[0]);
                if (current >= 0) {
                    sec2numFirst.put(tripleList[0], current + 1);
                } else {
                    sec2numFirst.put(tripleList[0], current - 1);
                }
            } else {
                sec2numFirst.put(tripleList[0], -1);
            }
            if (sec2numFirst.get(tripleList[1]) < 0) {
                sec2numFirst.put(tripleList[1], Math.abs(sec2numFirst.get(tripleList[1])));
            }

            // create an entry if not yet exists
            sec2numThird.putIfAbsent(tripleList[1], 0);
            // for those 3rd that have not yet confirmed to have an corresponding 2nd,
            // we use negative counting
            if (sec2numThird.containsKey(tripleList[2])) {
                int current = sec2numThird.get(tripleList[2]);
                if (current >= 0) {
                    sec2numThird.put(tripleList[2], current + 1);
                } else {
                    sec2numThird.put(tripleList[2], current - 1);
                }
            } else {
                sec2numThird.put(tripleList[2], -1);
            }
            if (sec2numThird.get(tripleList[1]) < 0) {
                sec2numThird.put(tripleList[1], Math.abs(sec2numThird.get(tripleList[1])));
            }

            third2numThird.put(tripleList[2], third2numThird.getOrDefault(tripleList[2], 0) + 1);

            // create an entry if not yet exists
            third2numFirst.putIfAbsent(tripleList[2], 0);
            // for those 1st that have not yet confirmed to have an corresponding 3rd,
            // we use negative counting
            if (third2numFirst.containsKey(tripleList[0])) {
                int current = third2numFirst.get(tripleList[0]);
                if (current >= 0) {
                    third2numFirst.put(tripleList[0], current + 1);
                } else {
                    third2numFirst.put(tripleList[0], current - 1);
                }
            } else {
                third2numFirst.put(tripleList[0], -1);
            }
            if (third2numFirst.get(tripleList[2]) < 0) {
                third2numFirst.put(tripleList[2], Math.abs(third2numFirst.get(tripleList[2])));
            }

            // create an entry if not yet exists
            third2numSec.putIfAbsent(tripleList[2], 0);
            // for those 1st that have not yet confirmed to have an corresponding 3rd,
            // we use negative counting
            if (third2numSec.containsKey(tripleList[1])) {
                int current = third2numSec.get(tripleList[1]);
                if (current >= 0) {
                    third2numSec.put(tripleList[1], current + 1);
                } else {
                    third2numSec.put(tripleList[1], current - 1);
                }
            } else {
                third2numSec.put(tripleList[1], -1);
            }
            if (third2numSec.get(tripleList[2]) < 0) {
                third2numSec.put(tripleList[2], Math.abs(third2numSec.get(tripleList[2])));
            }

            tripleString = csvReader.readLine();
        }

        first2numSec.entrySet().removeIf(e->e.getValue() <= 0);
        first2numThird.entrySet().removeIf(e->e.getValue() <= 0);
        sec2numFirst.entrySet().removeIf(e->e.getValue() <= 0);
        sec2numThird.entrySet().removeIf(e->e.getValue() <= 0);
        third2numFirst.entrySet().removeIf(e->e.getValue() <= 0);
        third2numSec.entrySet().removeIf(e->e.getValue() <= 0);

        for (Map.Entry<Integer, HashMap<Integer, ArrayList<Integer>>> triple : allTriples.entrySet()) {
            for (Map.Entry<Integer, ArrayList<Integer>> suffix : triple.getValue().entrySet()) {
                HistogramBucket bucket = new HistogramBucket();
                bucket.startingTriple = new RdfTriple(triple.getKey(), suffix.getKey(), suffix
                    .getValue().get(0));
                bucket.endingTriple = new RdfTriple(triple.getKey(), suffix.getKey(), suffix
                    .getValue().get(suffix.getValue().size() - 1));

                bucket.numTriples = suffix.getValue().size();
                bucket.numDistinctTwoPrefix = suffix.getValue().size();
                bucket.numDistinctOnePrefix = suffix.getValue().size();
                bucket.firstEqFirst = first2numFirst.get(triple.getKey());
                bucket.firstEqSec = first2numSec.getOrDefault(triple.getKey(), 0);
                bucket.firstEqThird = first2numThird.getOrDefault(triple.getKey(), 0);
                bucket.secEqFirst = sec2numFirst.getOrDefault(suffix.getKey(), 0);
                bucket.secEqSec = sec2numSec.getOrDefault(suffix.getKey(), 0);
                bucket.secEqThird = sec2numThird.getOrDefault(suffix.getKey(), 0);
                bucket.thirdEqFirst = 0;
                bucket.thirdEqSec = 0;
                bucket.thirdEqThird = 0;

                for (Integer obj : suffix.getValue()) {
                    bucket.thirdEqFirst += third2numFirst.getOrDefault(obj, 0);
                    bucket.thirdEqSec += third2numSec.getOrDefault(obj, 0);
                    bucket.thirdEqThird += third2numThird.getOrDefault(obj, 0);
                }

                this.buckets.add(bucket);
            }
        }
    }

    public void save() throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter(this.dataFilePath +
            "_histogram", true));
        writer.write("numBuckets: " + buckets.size() + "\n");
        for (HistogramBucket bucket : buckets) {
            bucket.save(writer);
        }
        writer.close();
    }
}
