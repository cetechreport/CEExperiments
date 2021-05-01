package TrueCardinality;

import Common.Pair;

import java.util.HashMap;

public class JOBQueryTrueCardinality extends TrueCardinality {
    JOBQueryTrueCardinality(String graphFile, String queryFile) throws Exception {
        patternType2pivot = new HashMap<Integer, Integer>() {{
            // 3-path
            put(101, 1);
            // 3-star
            put(102, 2);
            put(103, 1);
            put(104, 3);
            // 4-star
            put(105, 2);
            // 4-fork
            put(106, 0);
            put(107, 4);
            put(108, 3);
            // 5-star
            put(109, 3);
            // 5-pi
            put(110, 4);
            // 6-bifork
            put(111, 5);
        }};

        vListSplit = new HashMap<String, Pair<String, String>>() {{
            put("0-2;1-2;1-3", new Pair<>("1-2;0-2", "1-3"));
            put("0-2;1-2;2-3", new Pair<>("0-2", "1-2;2-3"));
            put("0-1;1-2;1-3", new Pair<>("0-1", "1-2;1-3"));
            put("0-3;1-3;2-3", new Pair<>("0-3", "1-3;2-3"));
            put("0-2;1-2;2-3;2-4", new Pair<>("1-2;0-2", "2-3;2-4"));
            put("0-1;0-2;0-3;3-4", new Pair<>("0-2;0-1", "0-3;3-4"));
            put("0-1;0-4;2-4;3-4", new Pair<>("0-4;0-1", "2-4;3-4"));
            put("0-1;0-3;2-3;3-4", new Pair<>("0-3;0-1", "2-3;3-4"));
            put("0-3;1-3;2-3;3-4;3-5", new Pair<>("1-3;0-3", "2-3;3-4;3-5"));
            put("0-1;1-2;1-4;3-4;4-5", new Pair<>("1-4;1-2;0-1", "3-4;4-5"));
            put("0-1;0-2;0-5;3-5;4-5;5-6", new Pair<>("0-5;0-2;0-1", "3-5;4-5;5-6"));
        }};
        labelSeqSplitIndex = new HashMap<String, Integer>() {{
            put("0-2;1-2;1-3", 2); // 101
            put("0-2;1-2;2-3", 1); // 102
            put("0-1;1-2;1-3", 1); // 103
            put("0-3;1-3;2-3", 1); // 104
            put("0-2;1-2;2-3;2-4", 2); // 105
            put("0-1;0-2;0-3;3-4", 2); // 106
            put("0-1;0-4;2-4;3-4", 2); // 107
            put("0-1;0-3;2-3;3-4", 2); // 108
            put("0-3;1-3;2-3;3-4;3-5", 2); // 109
            put("0-1;1-2;1-4;3-4;4-5", 3); // 110
            put("0-1;0-2;0-5;3-5;4-5;5-6", 3); // 111
        }};
        readGraph(graphFile);
        readQueries(queryFile);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("graphFile: " + args[0]);
        System.out.println("queryFile: " + args[1]);
        System.out.println("destFile: " + args[2]);
        System.out.println();

        TrueCardinality trueCardinality = new JOBQueryTrueCardinality(args[0], args[1]);
        trueCardinality.prepare();
        trueCardinality.compute(args[2]);
    }
}
