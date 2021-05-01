package MarkovTable.PropertyFilter;

import Common.Pair;
import Common.Path;
import Common.Query;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringJoiner;

public class QueryEvaluation {
    private String mtFile;
    private String sampleFile;
    private String propFile;

    private Map<Path, Integer> mt;
    private int mtLen;
    private Map<Path, List<List<Integer>>> samples;
    private Map<Integer, Integer> vid2prodYear;

    private List<Query> queries;
    private Map<Integer, List<Integer>> extensions;
    private final int NUM_LABELS = 127;
    private final Integer ALSO_KNOWN_AS = 1;
    private final Integer IS_KEYWORD_OF = 100;
    private final Integer IS_EPISODE_OF = 4;

    private final Integer[] COMPANY_TYPES = new Integer[] {
        124, 123, 119, 2, 121, 118, 120, 126, 3, 127, 4, 122, 125, 117
    };

    private final Integer[] INFO_TYPES = new Integer[] {
        23, 57, 18, 29, 21, 6, 66, 32, 62, 55, 53, 88, 44, 52, 51, 25, 13, 46, 71, 24, 87,
        38, 63, 79, 15, 19, 81, 67, 37, 17, 76, 84, 92, 8, 96, 30, 68, 70, 33, 36, 85, 77, 49, 7,
        69, 72, 12, 10, 83, 65, 28, 45, 47, 86, 60, 91, 11, 20, 56, 34, 90, 80, 98, 78, 74, 39,
        50, 22, 9, 97, 40, 95, 5, 82, 73, 27, 14, 42, 61, 94, 59, 75, 89, 64, 93, 41, 16, 54, 58,
        99, 26, 35, 31, 48, 43
    };

    private final Integer[] LINK_TYPES = new Integer[] {
        105, 104, 110, 114, 103, 101, 106, 113, 107, 108, 112, 115, 109, 111, 102, 116
    };

    private final Integer[] ROLE_TYPES = new Integer[] {
        124, 123, 119, 121, 118, 120, 126, 127, 122, 125, 117
    };

    private final int NO_FILTER = -1;
    private final int PROD_YEAR_INDEX = 4;
    private final int MIN_PROD_YEAR = 1880;
    private final int MAX_PROD_YEAR = 2019;

    private Random random;

    private int getRandomProdYear() {
        return random.nextInt(MAX_PROD_YEAR - MIN_PROD_YEAR + 1) + MIN_PROD_YEAR;
    }

    private boolean applyFilter(List<Integer> sample, List<Pair<String, Integer>> filters)
            throws Exception {
        int vid, literal;
        String operator;

        boolean qualified = true;

        for (int i = 0; i < sample.size(); ++i) {
            operator = filters.get(i).key;
            if (operator.isEmpty()) continue;

            vid = sample.get(i);
            literal = filters.get(i).value;

            if (!vid2prodYear.containsKey(vid)) continue;

            switch (operator) {
                case "<":
                    qualified = qualified && vid2prodYear.get(vid) < literal;
                    break;
                case ">":
                    qualified = qualified && vid2prodYear.get(vid) > literal;
                    break;
                case "<=":
                    qualified = qualified && vid2prodYear.get(vid) <= literal;
                    break;
                case ">=":
                    qualified = qualified && vid2prodYear.get(vid) >= literal;
                    break;
                case "=":
                    qualified = qualified && vid2prodYear.get(vid) == literal;
                    break;
                default:
                    throw new Exception("ERROR: unrecognized operator: " + operator);
            }
        }

        return qualified;
    }

    private double computeProportion(Query query) throws Exception {
        double numQualified = 0;

        for (List<Integer> sample: samples.get(query.path)) {
            boolean qualified = applyFilter(sample, query.filters);
            if (qualified) numQualified++;
        }

        double proportion = numQualified / samples.get(query.path).size();
//        System.out.println(numQualified + " / " + samples.get(query.path).size() + " = " + proportion);
        return proportion;
    }

    private Query[] breakdown(Query query, int length) {
        Query[] subqueries = new Query[query.path.length() - length + 1];
        for (int i = 0; i < subqueries.length; ++i) {
            subqueries[i] = new Query(
                query.path.subPath(i, i + length),
                query.filters.subList(i, i + length + 1)
            );
        }
        return subqueries;
    }

    public void evaluate() throws Exception {
        int numQueries = queries.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        StringJoiner result = new StringJoiner("\n");

        double proportion, estimation;
        for (Query query : queries) {
            estimation = 1.0;

            if (query.path.length() > mtLen) {
                Query[] numerators = breakdown(query, mtLen);
                for (Query subQuery: numerators) {
                    if (!mt.containsKey(subQuery.path)) {
                        estimation = 0;
                        break;
                    }

                    proportion = computeProportion(subQuery);
                    estimation *= mt.get(subQuery.path) * proportion;
                }

                if (estimation > 0) {
                    Query[] denominators = breakdown(query, mtLen - 1);
                    for (int i = 1; i < denominators.length - 1; ++i) {
                        proportion = computeProportion(denominators[i]);
                        estimation /= (mt.get(denominators[i].path) * proportion);
                    }

                    result.add(query.toString() + "," + (int) Math.ceil(estimation));
                }
            } else if (samples.containsKey(query.path)) {
                proportion = computeProportion(query);
                estimation = mt.get(query.path) * proportion;
                result.add(query.toString() + "," + (int) Math.ceil(estimation));
            }

            progress += 100.0 / numQueries;
//            System.out.print("\rEvaluate Queries: " + (int) progress + "%");
        }

        endTime = System.currentTimeMillis();
        System.out.println("Evaluate " + numQueries + " Queries: "
            + ((endTime - startTime) / 1000.0) + " sec");

        System.out.println("------- EVALUATION RESULT -------");
        System.out.println(result.toString());
    }

    public void generateQueries() {
        Query query;
        List<Pair<String, Integer>> filters;
        Integer[] labels;
        Path path;

        for (int i = 0; i < LINK_TYPES.length; ++i) {
            for (int j = 0; j < INFO_TYPES.length; ++j) {
                labels = new Integer[] {
                    IS_EPISODE_OF, LINK_TYPES[i], INFO_TYPES[j]
                };
                path = new Path(Arrays.asList(labels));

                filters = new ArrayList<>();
                filters.add(new Pair<>(">", getRandomProdYear()));
                filters.add(new Pair<>("", NO_FILTER));
                filters.add(new Pair<>("", NO_FILTER));
                filters.add(new Pair<>("", NO_FILTER));

                query = new Query(path, filters);
                queries.add(query);

                filters = new ArrayList<>(filters);
                filters.set(2, new Pair<>("<", getRandomProdYear()));
                query = new Query(path, filters);
                queries.add(query);

                for (int k = 0; k < ROLE_TYPES.length; ++k) {
                    labels = new Integer[] {
                        LINK_TYPES[i], INFO_TYPES[j], ROLE_TYPES[k]
                    };
                    path = new Path(Arrays.asList(labels));

                    filters = new ArrayList<>();
                    filters.add(new Pair<>("", NO_FILTER));
                    filters.add(new Pair<>(">", getRandomProdYear()));
                    filters.add(new Pair<>("", NO_FILTER));
                    filters.add(new Pair<>("", NO_FILTER));

                    query = new Query(path, filters);
                    queries.add(query);
                }
            }
        }
    }

    public void init() throws Exception {
        mt = new HashMap<>();
        samples = new HashMap<>();
        vid2prodYear = new HashMap<>();
        mtLen = 1;

        String[] pathAndCount, labelList;
        Path path;

        BufferedReader mtReader = new BufferedReader(new FileReader(mtFile));
        String line = mtReader.readLine();
        while (null != line) {
            pathAndCount = line.split(",");

            labelList = pathAndCount[0].split("->");
            path = new Path(new ArrayList<>());
            for (String label : labelList) {
                path.append(Integer.parseInt(label));
            }
            mtLen = Math.max(mtLen, path.length());

            mt.put(path, Integer.parseInt(pathAndCount[1]));

            line = mtReader.readLine();
        }
        mtReader.close();

        String[] pathAndSamples, vertexList;
        List<Integer> sample;

        BufferedReader sampleReader = new BufferedReader(new FileReader(sampleFile));
        line = sampleReader.readLine();
        while (null != line) {
            pathAndSamples = line.split(",");

            labelList = pathAndSamples[0].split("->");
            path = new Path(new ArrayList<>());
            for (String label : labelList) {
                path.append(Integer.parseInt(label));
            }

            samples.putIfAbsent(path, new ArrayList<>());
            for (int i = 1; i < pathAndSamples.length; ++i) {
                vertexList = pathAndSamples[i].split("->");
                sample = new ArrayList<>();
                for (String v : vertexList) {
                    sample.add(Integer.parseInt(v));
                }
                samples.get(path).add(sample);
            }

            line = sampleReader.readLine();
        }
        sampleReader.close();

        String[] properties;

        BufferedReader tsvReader = new BufferedReader(new FileReader(propFile));
        tsvReader.readLine(); // Header

        line = tsvReader.readLine();
        while (null != line) {
            properties = line.split("\t");
            if (!properties[PROD_YEAR_INDEX].isEmpty()) {
                vid2prodYear.put(
                    Integer.parseInt(properties[0]), Integer.parseInt(properties[PROD_YEAR_INDEX])
                );
            }
            line = tsvReader.readLine();
        }
        tsvReader.close();
    }

    public QueryEvaluation(String mtFile, String sampleFile, String propFile) {
        this.mtFile = mtFile;
        this.sampleFile = sampleFile;
        this.propFile = propFile;

        random = new Random(0);
        queries = new ArrayList<>();
        extensions = new HashMap<>();

        for (int i = 1; i <= NUM_LABELS; ++i) {
            extensions.put(i, new ArrayList<>());
        }

        extensions.get(ALSO_KNOWN_AS).add(IS_EPISODE_OF);

        for (int i = 0; i < COMPANY_TYPES.length; ++i) {
            extensions.get(COMPANY_TYPES[i]).addAll(Arrays.asList(ALSO_KNOWN_AS));
            extensions.get(COMPANY_TYPES[i]).addAll(Arrays.asList(INFO_TYPES));
            extensions.get(COMPANY_TYPES[i]).addAll(Arrays.asList(LINK_TYPES));
            extensions.get(COMPANY_TYPES[i]).addAll(Arrays.asList(IS_EPISODE_OF));
        }

        for (int i = 0; i < LINK_TYPES.length; ++i) {
            extensions.get(LINK_TYPES[i]).addAll(Arrays.asList(ALSO_KNOWN_AS));
            extensions.get(LINK_TYPES[i]).addAll(Arrays.asList(INFO_TYPES));
            extensions.get(LINK_TYPES[i]).addAll(Arrays.asList(LINK_TYPES));
            extensions.get(LINK_TYPES[i]).addAll(Arrays.asList(IS_EPISODE_OF));
        }

        extensions.get(IS_KEYWORD_OF).addAll(Arrays.asList(ALSO_KNOWN_AS));
        extensions.get(IS_KEYWORD_OF).addAll(Arrays.asList(INFO_TYPES));
        extensions.get(IS_KEYWORD_OF).addAll(Arrays.asList(LINK_TYPES));
        extensions.get(IS_KEYWORD_OF).addAll(Arrays.asList(IS_EPISODE_OF));

        extensions.get(IS_EPISODE_OF).addAll(Arrays.asList(ALSO_KNOWN_AS));
        extensions.get(IS_EPISODE_OF).addAll(Arrays.asList(INFO_TYPES));
        extensions.get(IS_EPISODE_OF).addAll(Arrays.asList(LINK_TYPES));
        extensions.get(IS_EPISODE_OF).addAll(Arrays.asList(IS_EPISODE_OF));

        for (int i = 0; i < ROLE_TYPES.length; ++i) {
            extensions.get(ROLE_TYPES[i]).addAll(Arrays.asList(ALSO_KNOWN_AS));
            extensions.get(ROLE_TYPES[i]).addAll(Arrays.asList(INFO_TYPES));
            extensions.get(ROLE_TYPES[i]).addAll(Arrays.asList(LINK_TYPES));
            extensions.get(ROLE_TYPES[i]).addAll(Arrays.asList(IS_EPISODE_OF));
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("mtFile: " + args[0]);
        System.out.println("sampleFile: " + args[1]);
        System.out.println("propFile: " + args[2]);
        System.out.println();

        String mtFile = args[0];
        String sampleFile = args[1];
        String propFile = args[2];

        QueryEvaluation queryEvaluation = new QueryEvaluation(mtFile, sampleFile, propFile);
        queryEvaluation.init();
        queryEvaluation.generateQueries();
        queryEvaluation.evaluate();
    }
}
