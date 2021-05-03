package Graphflow;

import Common.Query;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;

public class LargeBenchmarkQueryDecomposer extends QueryDecomposer {
    // patternType -> decomVListString (defining decom topology) -> set of edge label seq
    private Map<Integer, Map<String, Set<String>>> queried = new HashMap<>();
    public static HashMap<String, Integer>  patternToPatternType = new HashMap<String, Integer>() {{
        put("0-1;0-2;0-3;0-4;0-5;0-6", 201);
        put("0-1;0-2;0-3;0-4;0-5;5-6", 202);
        put("0-1;0-2;0-3;3-4;4-5;4-6", 203);
        put("0-1;0-2;0-3;3-4;4-5;5-6", 204);
        put("0-1;0-3;2-3;2-4;4-5;5-6", 205);

        put("0-1;0-2;0-3;0-4;0-5;0-6;0-7", 206);
        put("0-1;0-2;0-3;0-4;0-5;0-6;6-7", 207);
        put("0-1;0-2;0-3;0-4;4-5;5-6;5-7", 208);
        put("0-1;0-2;0-3;0-4;4-5;5-6;6-7", 209);
        put("0-1;1-3;2-3;2-4;4-6;5-6;6-7", 210);
        put("0-1;0-3;2-3;2-5;4-5;4-6;6-7", 211);

        put("0-2;1-2;2-3;2-4;2-5;2-6;2-7;2-8", 212);
        put("0-1;0-2;0-3;0-4;0-5;0-6;0-7;7-8", 213);
        put("0-1;0-2;0-3;0-4;0-5;0-6;6-7;7-8", 214);
        put("0-1;0-2;0-3;0-4;4-5;5-6;6-7;6-8", 215);
        put("0-2;1-2;1-3;3-5;4-5;4-6;4-7;4-8", 216);
        put("0-1;1-2;2-4;3-4;3-6;5-6;5-7;5-8", 217);
        put("0-1;0-3;2-3;2-5;4-5;4-6;6-7;7-8", 218);

        put("0-3;1-3;2-3;3-4;3-5;3-6", 219);
        put("0-3;1-3;2-3;3-4;3-5;3-6;3-7", 220);
        put("0-3;1-3;2-3;3-4;3-5;3-6;6-7", 221);
        put("0-2;1-2;2-3;2-4;4-6;5-6;5-7", 222);
        put("0-4;1-4;2-4;3-4;4-5;4-6;4-7;4-8", 223);
        put("0-4;1-4;2-4;3-4;4-5;4-6;4-7;7-8", 224);
        put("0-2;1-2;1-6;3-6;4-6;5-6;6-7;6-8", 225);
        put("0-2;1-2;2-4;3-4;3-7;5-7;6-7;7-8", 226);
        put("0-1;0-3;2-3;2-5;4-5;4-7;6-7;7-8", 227);
    }};
    @Override
    public void decompose(Query query, int patternType) {
        String labelSeq, vListString;
        for (Integer[] patternVList : LargeBenchmarkDecompositions.LENGTH3) {
            vListString = toVListString(patternVList);
            queried.get(patternType).putIfAbsent(vListString, new HashSet<>());
            labelSeq = extractEdgeLabels(query.topology, patternVList);
            queried.get(patternType).get(vListString).add(labelSeq);
        }
        for (Integer[] patternVList : LargeBenchmarkDecompositions.LENGTH2) {
            vListString = toVListString(patternVList);
            queried.get(patternType).putIfAbsent(vListString, new HashSet<>());
            labelSeq = extractEdgeLabels(query.topology, patternVList);
            queried.get(patternType).get(vListString).add(labelSeq);
        }
        for (Integer[] patternVList : LargeBenchmarkDecompositions.LENGTH1) {
            vListString = toVListString(patternVList);
            queried.get(patternType).putIfAbsent(vListString, new HashSet<>());
            labelSeq = extractEdgeLabels(query.topology, patternVList);
            queried.get(patternType).get(vListString).add(labelSeq);
        }
    }

    @Override
    protected void saveDecompositions() throws Exception {
        BufferedWriter writer = new BufferedWriter(new FileWriter("decom.csv"));

        int total = 0;
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (Map<String, Set<String>> vList2labelSeqs : queried.values()) {
            for (String vList : vList2labelSeqs.keySet()) {
                total += vList2labelSeqs.get(vList).size();
            }
        }

        for (Integer patternType : queried.keySet()) {
            for (String vList : queried.get(patternType).keySet()) {
                StringJoiner sj = new StringJoiner(",");
                sj.add(patternType.toString());
                sj.add(vList);

                for (String labelSeq : queried.get(patternType).get(vList)) {
                    sj.add(labelSeq);

                    progress += 100.0 / total;
                    System.out.print("\rSaving: " + (int) progress + "%");
                }
                writer.write(sj.toString() + "\n");
            }
        }
        writer.close();

        endTime = System.currentTimeMillis();
        System.out.println("\nSaving: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public LargeBenchmarkQueryDecomposer(int patternType) {
        queried.put(patternType, new HashMap<>());
    }

    public static String convertToStr(int[] deg) {
        String deg_str = "";
        for(int i : deg) {
            deg_str += Integer.toString(i);
        }
        return deg_str;
    }

    public static String getPattern(String queryFile) throws Exception {

        BufferedReader reader = new BufferedReader(new FileReader(queryFile));
        String line = reader.readLine();
        reader.close();

        String[] info = line.split(",");
        return info[0];
//        String[] edges = info[0].split(";");
//        String[] temp = new String[edges.length];
//        System.arraycopy(edges, 0, temp, 0, edges.length);
//        int max = 0;
//        for (int i = 0; i < edges.length; ++i) {
//            String[] vertices = edges[i].split("-");
//            int a = Integer.parseInt(vertices[0]);
//            int b = Integer.parseInt(vertices[1]);
//            int max_of_ab = Math.max(a, b);
//            if (max < max_of_ab) {
//                max = max_of_ab;
//            }
//        }
//        edges = temp;
//        int[] deg = new int[max + 1];
//        for (int i = 0; i < edges.length; ++i) {
//            String[] vertices = edges[i].split("-");
//            int a = Integer.parseInt(vertices[0]);
//            int b = Integer.parseInt(vertices[1]);
//            deg[a] += 1;
//            deg[b] += 1;
//        }
//        Arrays.sort(deg);
//        String pattern = convertToStr(deg);
    }

    public static void getCatEntr(String pattern) {
        // size-1 join
        String[] edges = pattern.split(";");
        LargeBenchmarkDecompositions.LENGTH1 = new Integer[edges.length][2];
        for(int i = 0; i < edges.length; ++i) {
            String[] vertices = edges[i].split("-");
            int a = Integer.parseInt(vertices[0]);
            int b = Integer.parseInt(vertices[1]);
            LargeBenchmarkDecompositions.LENGTH1[i][0] = a;
            LargeBenchmarkDecompositions.LENGTH1[i][1] = b;
        }

        // size-2 join
        List<Integer[]> catLen2 = new ArrayList<>();
        for(int i = 0; i < LargeBenchmarkDecompositions.LENGTH1.length; ++i) {
            int ei1 = LargeBenchmarkDecompositions.LENGTH1[i][0];
            int ei2 = LargeBenchmarkDecompositions.LENGTH1[i][1];
            for(int j = i + 1; j < LargeBenchmarkDecompositions.LENGTH1.length; ++j) {
                int ej1 = LargeBenchmarkDecompositions.LENGTH1[j][0];
                int ej2 = LargeBenchmarkDecompositions.LENGTH1[j][1];
                if (ej1 == ei1 || ej1 == ei2 || ej2 == ei1 || ej2 == ei2) {
                    catLen2.add(new Integer[] {ei1, ei2, ej1, ej2});
                }
            }
        }
        LargeBenchmarkDecompositions.LENGTH2 = new Integer[catLen2.size()][4];
        catLen2.toArray(LargeBenchmarkDecompositions.LENGTH2);

        // size-3 join
        List<Integer[]> catLen3 = new ArrayList<>();
        for(int i = 0; i < LargeBenchmarkDecompositions.LENGTH1.length; ++i) {
            int ei1 = LargeBenchmarkDecompositions.LENGTH1[i][0];
            int ei2 = LargeBenchmarkDecompositions.LENGTH1[i][1];
            for(int j = i + 1; j < LargeBenchmarkDecompositions.LENGTH1.length; ++j) {
                int ej1 = LargeBenchmarkDecompositions.LENGTH1[j][0];
                int ej2 = LargeBenchmarkDecompositions.LENGTH1[j][1];

                for (int k = j + 1; k < LargeBenchmarkDecompositions.LENGTH1.length; ++k) {
                    int ek1 = LargeBenchmarkDecompositions.LENGTH1[k][0];
                    int ek2 = LargeBenchmarkDecompositions.LENGTH1[k][1];
                    HashSet<Integer> uniqueVertices = new HashSet<Integer>();
                    uniqueVertices.add(ei1);
                    uniqueVertices.add(ei2);
                    uniqueVertices.add(ej1);
                    uniqueVertices.add(ej2);
                    uniqueVertices.add(ek1);
                    uniqueVertices.add(ek2);
                    if (uniqueVertices.size() == 4) {
                        catLen3.add(new Integer[] {ei1, ei2, ej1, ej2, ek1, ek2});
                    }

                }
            }
        }
        LargeBenchmarkDecompositions.LENGTH3 = new Integer[catLen3.size()][6];
        catLen3.toArray(LargeBenchmarkDecompositions.LENGTH3);
    }



    public static void main(String[] args) throws Exception {
        String pattern = getPattern(args[0]);
        int patternType;
        if (patternToPatternType.containsKey(pattern)) {
            patternType = patternToPatternType.get(pattern);
        } else {
            patternType = LargeBenchmarkDecompositions.BASE + 1 + patternToPatternType.size();
            patternToPatternType.put(pattern, patternType);
        }

        System.out.println("queries: " + args[0]);
        System.out.println("patternType: " + patternType);
        System.out.println();

        getCatEntr(pattern);

        List<Query> queries = Query.readQueries(args[0]);

        QueryDecomposer decomposer = new LargeBenchmarkQueryDecomposer(patternType);

        int numQueries = queries.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (int i = 0; i < queries.size(); ++i) {
            progress += 100.0 / numQueries;
            System.out.print("\rDecomposing: " + (int) progress + "%");

            decomposer.decompose(queries.get(i), patternType);
        }

        endTime = System.currentTimeMillis();
        System.out.println("\nDecomposing: " + ((endTime - startTime) / 1000.0) + " sec");

        decomposer.saveDecompositions();
    }
}
