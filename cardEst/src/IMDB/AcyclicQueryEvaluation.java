package IMDB;

import CharacteristicSet.CSetsMTCombined;
import CharacteristicSet.CharacteristicSets;
import Common.Pair;
import Common.Query;
import Common.Topology;
import Common.Util;
import PartitionedEstimation.PartitionedCLLP;
import PartitionedEstimation.PartitionedCatalogue;
import PartitionedEstimation.PartitionedCatalogueRunnable;
import Graphflow.CVCatalogue;
import Graphflow.Catalogue;
import Graphflow.Constants;
import MarkovTable.PropertyFilter.MT;
import Pessimistic.Pessimistic;
import Pessimistic.CLLP;
import PureSampling.NodePureSampling;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.StringJoiner;

public class AcyclicQueryEvaluation {
    public List<Query> queries = new ArrayList<>();
    public List<Long> trueCard = new ArrayList<>();
    public List<Integer> queryTypeBoundaries = new ArrayList<>();
    private Random random = new Random(0);

    private List<Pair<String, Integer>> copyFilters(List<Pair<String, Integer>> filters) {
        List<Pair<String, Integer>> cloned = new ArrayList<>();

        for (Pair<String, Integer> filter : filters) {
            cloned.add(new Pair<>(filter.key, filter.value));
        }

        return cloned;
    }

    public void sampleQueries(int sampleSize) {
        Random random = new Random(0);
        Set<Integer> sampledIndices = new HashSet<>();

        int shareSize = sampleSize / queryTypeBoundaries.size();

        List<Query> samples = new ArrayList<>();
        List<Query> oneTypeSamples = new ArrayList<>();
        List<Query> subListQueries;
        List<Integer> newBoundaries = new ArrayList<>();
        List<Long> subListTrueCard;
        List<Long> oneTypeTrueCard = new ArrayList<>();
        List<Long> newTrueCard = new ArrayList<>();

        for (int i = 0; i < queryTypeBoundaries.size(); ++i) {
            sampledIndices.clear();
            oneTypeSamples.clear();
            oneTypeTrueCard.clear();

            int start = i == 0 ? 0 : queryTypeBoundaries.get(i - 1);
            subListQueries = queries.subList(start, queryTypeBoundaries.get(i));
            subListTrueCard = trueCard.subList(start, queryTypeBoundaries.get(i));

            if (subListQueries.size() > shareSize) {
                int sampledIndex;
                while (oneTypeSamples.size() < shareSize) {
                    sampledIndex = random.nextInt(subListQueries.size());
                    while (sampledIndices.contains(sampledIndex)) {
                        sampledIndex = random.nextInt(subListQueries.size());
                    }
                    sampledIndices.add(sampledIndex);

                    oneTypeSamples.add(subListQueries.get(sampledIndex));
                    oneTypeTrueCard.add(subListTrueCard.get(sampledIndex));
                }
            } else {
                oneTypeSamples = subListQueries;
                oneTypeTrueCard = subListTrueCard;
            }
            samples.addAll(oneTypeSamples);
            newBoundaries.add(samples.size());
            newTrueCard.addAll(oneTypeTrueCard);
        }

        queries = samples;
        queryTypeBoundaries = newBoundaries;
        trueCard = newTrueCard;

        System.out.println("Query size: " + queries.size());
        System.out.println("Boundaries: " + queryTypeBoundaries);
    }

    private void sampleQueriesOfType(int fromIndex, int toIndex, int sampleSize) {
        List<Query> queriesOfType = queries.subList(fromIndex, toIndex);

        Random random = new Random(0);
        Set<Integer> sampledIndices = new HashSet<>();

        List<Query> samples = new ArrayList<>();

        if (queriesOfType.size() > sampleSize) {
            int sampledIndex;
            while (samples.size() < sampleSize) {
                sampledIndex = random.nextInt(queriesOfType.size());
                while (sampledIndices.contains(sampledIndex)) {
                    sampledIndex = random.nextInt(queriesOfType.size());
                }
                sampledIndices.add(sampledIndex);

                samples.add(queriesOfType.get(sampledIndex));
            }

            queries.removeAll(queriesOfType);
            queries.addAll(samples);
            queryTypeBoundaries.set(queryTypeBoundaries.size() - 1, queries.size());
        }
    }

    private int getRandomProdYear() {
        return random.nextInt(Labels.MAX_PROD_YEAR - Labels.MIN_PROD_YEAR + 1)
            + Labels.MIN_PROD_YEAR;
    }

    private List<Pair<String, Integer>> initFilters(int numVertices) {
        List<Pair<String, Integer>> filters = new ArrayList<>();
        for (int i = 0; i < numVertices; ++i) {
            filters.add(new Pair<>("", Labels.NO_FILTER));
        }
        return filters;
    }

    public AcyclicQueryEvaluation(int numEdges) {
        if (4 == numEdges) {
            queries4();
        } else if (5 == numEdges) {
            queries5();
        } else if (6 == numEdges) {
            queries6();
        }
    }

    private void queries5() {
        Query query;
        Topology topology;
        List<Pair<String, Integer>> filters;

        long startTime = System.currentTimeMillis();
        long endTime;

        final int SIZE_OF_EACH_TYPE = 80000;
        int typeStartIndex = 0;

        // STAR: title1->aka, comp->title1, title1->title2, title1->info1, title1->info2
        for (int i = 0; i < Labels.COMPANY_TYPES.length; ++i) {
            for (int j = 0; j < Labels.LINK_TYPES.length; ++j) {
                for (int k = 0; k < Labels.INFO_TYPES.length; ++k) {
                    for (int l = 0; l < Labels.INFO_TYPES.length; ++l) {
                        if (k == l) continue;

                        topology = new Topology();
                        topology.addEdge(0, Labels.COMPANY_TYPES[i], 1);
                        topology.addEdge(1, Labels.ALSO_KNOWN_AS, 2);
                        topology.addEdge(1, Labels.LINK_TYPES[j], 3);
                        topology.addEdge(1, Labels.INFO_TYPES[k], 4);
                        topology.addEdge(1, Labels.INFO_TYPES[l], 5);

                        filters = initFilters(6);
                        filters.get(1).key = ">";
                        filters.get(1).value = getRandomProdYear();
                        filters.get(3).key = ">";
                        filters.get(3).value = getRandomProdYear();
                        query = new Query(topology, filters);
                        queries.add(query);
                    }
                }
            }
        }
        queryTypeBoundaries.add(queries.size());
        sampleQueriesOfType(typeStartIndex, queries.size(), SIZE_OF_EACH_TYPE);
        endTime = System.currentTimeMillis();
        System.out.println("STAR: " + ((endTime - startTime) / 1000.0) + " sec");

        startTime = System.currentTimeMillis();
        // PATH: name->title1<-title2->title3<-title4<-company
        for (int i = 0; i < Labels.ROLE_TYPES.length; ++i) {
            for (int j = 0; j < Labels.LINK_TYPES.length; ++j) {
                for (int k = 0; k < Labels.LINK_TYPES.length; ++k) {
                    if (j == k) continue;
                    for (int l = 0; l < Labels.COMPANY_TYPES.length; ++l) {
                        for (int m = 0; m < Labels.LINK_TYPES.length; ++m) {
                            if (j == m || k == m) continue;

                            topology = new Topology();
                            topology.addEdge(0, Labels.ROLE_TYPES[i], 2);
                            topology.addEdge(1, Labels.LINK_TYPES[j], 2);
                            topology.addEdge(1, Labels.LINK_TYPES[k], 5);
                            topology.addEdge(3, Labels.COMPANY_TYPES[l], 4);
                            topology.addEdge(4, Labels.LINK_TYPES[m], 5);

                            filters = initFilters(6);
                            filters.get(1).key = "<";
                            filters.get(1).value = getRandomProdYear();
                            filters.get(4).key = "<";
                            filters.get(4).value = getRandomProdYear();
                            query = new Query(topology, filters);
                            queries.add(query);
                        }
                    }
                }
            }
        }
        typeStartIndex = queryTypeBoundaries.get(queryTypeBoundaries.size() - 1);
        queryTypeBoundaries.add(queries.size());
        sampleQueriesOfType(typeStartIndex, queries.size(), SIZE_OF_EACH_TYPE);
        endTime = System.currentTimeMillis();
        System.out.println("PATH: " + ((endTime - startTime) / 1000.0) + " sec");

        startTime = System.currentTimeMillis();
        // T_SHAPE: aka<-name->title1<-title2<-company, title1->info
        for (int i = 0; i < Labels.ROLE_TYPES.length; ++i) {
            for (int j = 0; j < Labels.COMPANY_TYPES.length; ++j) {
                for (int k = 0; k < Labels.LINK_TYPES.length; ++k) {
                    for (int l = 0; l < Labels.INFO_TYPES.length; ++l) {
                        topology = new Topology();
                        topology.addEdge(0, Labels.ALSO_KNOWN_AS, 1);
                        topology.addEdge(0, Labels.ROLE_TYPES[i], 4);
                        topology.addEdge(2, Labels.COMPANY_TYPES[j], 3);
                        topology.addEdge(3, Labels.LINK_TYPES[k], 4);
                        topology.addEdge(4, Labels.INFO_TYPES[l], 5);

                        filters = initFilters(6);
                        filters.get(4).key = ">";
                        filters.get(4).value = getRandomProdYear();
                        query = new Query(topology, filters);
                        queries.add(query);
                    }
                }
            }
        }
        typeStartIndex = queryTypeBoundaries.get(queryTypeBoundaries.size() - 1);
        queryTypeBoundaries.add(queries.size());
        sampleQueriesOfType(typeStartIndex, queries.size(), SIZE_OF_EACH_TYPE);
        endTime = System.currentTimeMillis();
        System.out.println("T_SHAPE: " + ((endTime - startTime) / 1000.0) + " sec");

        startTime = System.currentTimeMillis();
        // FORK: aka<-name->title1->title2, title1->info, title1<-company
        for (int i = 0; i < Labels.ROLE_TYPES.length; ++i) {
            for (int j = 0; j < Labels.COMPANY_TYPES.length; ++j) {
                for (int k = 0; k < Labels.LINK_TYPES.length; ++k) {
                    for (int l = 0; l < Labels.INFO_TYPES.length; ++l) {
                        topology = new Topology();
                        topology.addEdge(0, Labels.ALSO_KNOWN_AS, 1);
                        topology.addEdge(0, Labels.ROLE_TYPES[i], 3);
                        topology.addEdge(2, Labels.COMPANY_TYPES[j], 3);
                        topology.addEdge(3, Labels.LINK_TYPES[k], 4);
                        topology.addEdge(3, Labels.INFO_TYPES[l], 5);

                        filters = initFilters(6);
                        filters.get(3).key = "<";
                        filters.get(3).value = getRandomProdYear();
                        query = new Query(topology, filters);
                        queries.add(query);
                    }
                }
            }
        }
        typeStartIndex = queryTypeBoundaries.get(queryTypeBoundaries.size() - 1);
        queryTypeBoundaries.add(queries.size());
        sampleQueriesOfType(typeStartIndex, queries.size(), SIZE_OF_EACH_TYPE);
        endTime = System.currentTimeMillis();
        System.out.println("FORK: " + ((endTime - startTime) / 1000.0) + " sec");

        startTime = System.currentTimeMillis();
        // PI: aka<-name->info2, name->title1->info, title1->title2
        for (int i = 0; i < Labels.INFO_TYPES.length; ++i) {
            for (int j = 0; j < Labels.LINK_TYPES.length; ++j) {
                for (int k = 0; k < Labels.ROLE_TYPES.length; ++k) {
                    topology = new Topology();
                    topology.addEdge(0, Labels.ALSO_KNOWN_AS, 1);
                    topology.addEdge(0, Labels.INFO_TYPES[i], 2);
                    topology.addEdge(0, Labels.LINK_TYPES[j], 5);
                    topology.addEdge(3, Labels.ROLE_TYPES[k], 5);
                    topology.addEdge(4, Labels.IS_KEYWORD_OF, 5);

                    filters = initFilters(6);
                    filters.get(5).key = "<";
                    filters.get(5).value = getRandomProdYear();
                    query = new Query(topology, filters);
                    queries.add(query);
                }
            }
        }
        typeStartIndex = queryTypeBoundaries.get(queryTypeBoundaries.size() - 1);
        queryTypeBoundaries.add(queries.size());
        sampleQueriesOfType(typeStartIndex, queries.size(), SIZE_OF_EACH_TYPE);
        endTime = System.currentTimeMillis();
        System.out.println("PI: " + ((endTime - startTime) / 1000.0) + " sec");

        System.out.println("Boundaries: " + queryTypeBoundaries);
    }

    private void queries4() {
        Query query;
        Topology topology;
        List<Pair<String, Integer>> filters;

        final int SIZE_OF_EACH_TYPE = 50000;

        long startTime = System.currentTimeMillis();
        long endTime;

        // STAR:
        Random random = new Random(0);
        for (int j = 0; j < Labels.LINK_TYPES.length; ++j) {
            for (int k = 0; k < Labels.LINK_TYPES.length; ++k) {
                if (j == k) continue;
                for (int m = 0; m < Labels.INFO_TYPES.length; ++m) {

//                    int lottery = random.nextInt(100);
//                    if (lottery != 0) continue;

                    topology = new Topology();
                    topology.addEdge(0, Labels.ALSO_KNOWN_AS, 1);
                    topology.addEdge(0, Labels.LINK_TYPES[j], 2);
                    topology.addEdge(0, Labels.LINK_TYPES[k], 3);
                    topology.addEdge(0, Labels.INFO_TYPES[m], 4);

                    filters = initFilters(5);
                    query = new Query(topology, filters);
                    queries.add(query);
                }
            }
        }

        queryTypeBoundaries.add(queries.size());
        sampleQueriesOfType(0, queries.size(), SIZE_OF_EACH_TYPE);
        endTime = System.currentTimeMillis();
        System.out.println("STAR: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    private void queries6() {
        Query query;
        Topology topology;
        List<Pair<String, Integer>> filters;

        long startTime = System.currentTimeMillis();
        long endTime;

        final int SIZE_OF_EACH_TYPE = 50000;
        int typeStartIndex = 0;

        Random random = new Random(0);

        // STAR:
        for (int i = 0; i < Labels.COMPANY_TYPES.length; ++i) {
            for (int j = 0; j < Labels.LINK_TYPES.length; ++j) {
                for (int k = 0; k < Labels.LINK_TYPES.length; ++k) {
                    if (j == k) continue;
                    for (int l = 0; l < Labels.INFO_TYPES.length; ++l) {
                        for (int m = 0; m < Labels.INFO_TYPES.length; ++m) {
                            if (l == m) continue;

                            int lottery = random.nextInt(100);
                            if (lottery != 0) continue;

                            topology = new Topology();
                            topology.addEdge(0, Labels.COMPANY_TYPES[i], 1);
                            topology.addEdge(1, Labels.ALSO_KNOWN_AS, 2);
                            topology.addEdge(1, Labels.LINK_TYPES[j], 3);
                            topology.addEdge(1, Labels.INFO_TYPES[l], 4);
                            topology.addEdge(1, Labels.LINK_TYPES[k], 5);
                            topology.addEdge(1, Labels.INFO_TYPES[m], 6);

                            filters = initFilters(7);
//                            filters.get(1).key = ">";
//                            filters.get(1).value = getRandomProdYear();
//                            filters.get(3).key = ">";
//                            filters.get(3).value = getRandomProdYear();
                            query = new Query(topology, filters);
                            queries.add(query);
                        }
                    }
                }
            }
        }
        queryTypeBoundaries.add(queries.size());
        sampleQueriesOfType(typeStartIndex, queries.size(), SIZE_OF_EACH_TYPE);
        endTime = System.currentTimeMillis();
        System.out.println("STAR: " + ((endTime - startTime) / 1000.0) + " sec");

        startTime = System.currentTimeMillis();
        // PATH:
        for (int i = 0; i < Labels.LINK_TYPES.length; ++i) {
            for (int j = 0; j < Labels.ROLE_TYPES.length; ++j) {
                for (int k = 0; k < Labels.ROLE_TYPES.length; ++k) {
                    if (j == k) continue;
                    for (int l = 0; l < Labels.LINK_TYPES.length; ++l) {
                        if (i == l) continue;
                        for (int m = 0; m < Labels.INFO_TYPES.length; ++m) {

                            int lottery = random.nextInt(100);
                            if (lottery > 5) continue;

                            topology = new Topology();
                            topology.addEdge(0, Labels.IS_KEYWORD_OF, 1);
                            topology.addEdge(1, Labels.LINK_TYPES[i], 3);
                            topology.addEdge(2, Labels.ROLE_TYPES[j], 3);
                            topology.addEdge(2, Labels.ROLE_TYPES[k], 5);
                            topology.addEdge(4, Labels.LINK_TYPES[l], 5);
                            topology.addEdge(4, Labels.INFO_TYPES[m], 6);

                            filters = initFilters(7);
//                            filters.get(1).key = "<";
//                            filters.get(1).value = getRandomProdYear();
//                            filters.get(4).key = "<";
//                            filters.get(4).value = getRandomProdYear();
                            query = new Query(topology, filters);
                            queries.add(query);
                        }
                    }
                }
            }
        }
        typeStartIndex = queryTypeBoundaries.get(queryTypeBoundaries.size() - 1);
        queryTypeBoundaries.add(queries.size());
        sampleQueriesOfType(typeStartIndex, queries.size(), SIZE_OF_EACH_TYPE);
        endTime = System.currentTimeMillis();
        System.out.println("PATH: " + ((endTime - startTime) / 1000.0) + " sec");

        startTime = System.currentTimeMillis();
        // T_SHAPE:
        for (int i = 0; i < Labels.ROLE_TYPES.length; ++i) {
            for (int j = 0; j < Labels.COMPANY_TYPES.length; ++j) {
                for (int k = 0; k < Labels.LINK_TYPES.length; ++k) {
                    for (int l = 0; l < Labels.LINK_TYPES.length; ++l) {
                        if (k == l) continue;
                        for (int m = 0; m < Labels.INFO_TYPES.length; ++m) {
                            topology = new Topology();
                            topology.addEdge(0, Labels.ALSO_KNOWN_AS, 1);
                            topology.addEdge(0, Labels.ROLE_TYPES[i], 5);
                            topology.addEdge(2, Labels.COMPANY_TYPES[j], 3);
                            topology.addEdge(3, Labels.LINK_TYPES[k], 5);
                            topology.addEdge(4, Labels.LINK_TYPES[l], 5);
                            topology.addEdge(4, Labels.INFO_TYPES[m], 6);

                            filters = initFilters(7);
//                            filters.get(5).key = ">";
//                            filters.get(5).value = getRandomProdYear();
                            query = new Query(topology, filters);
                            queries.add(query);
                        }
                    }
                }
            }
        }
        typeStartIndex = queryTypeBoundaries.get(queryTypeBoundaries.size() - 1);
        queryTypeBoundaries.add(queries.size());
        sampleQueriesOfType(typeStartIndex, queries.size(), SIZE_OF_EACH_TYPE);
        endTime = System.currentTimeMillis();
        System.out.println("T_SHAPE: " + ((endTime - startTime) / 1000.0) + " sec");

        startTime = System.currentTimeMillis();
        // FORK24:
        for (int i = 0; i < Labels.ROLE_TYPES.length; ++i) {
            for (int j = 0; j < Labels.LINK_TYPES.length; ++j) {
                for (int k = 0; k < Labels.INFO_TYPES.length; ++k) {
                    for (int l = 0; l < Labels.COMPANY_TYPES.length; ++l) {
                        topology = new Topology();
                        topology.addEdge(0, Labels.ALSO_KNOWN_AS, 1);
                        topology.addEdge(0, Labels.ROLE_TYPES[i], 4);
                        topology.addEdge(2, Labels.COMPANY_TYPES[l], 4);
                        topology.addEdge(3, Labels.IS_KEYWORD_OF, 4);
                        topology.addEdge(4, Labels.LINK_TYPES[j], 5);
                        topology.addEdge(4, Labels.INFO_TYPES[k], 6);

                        filters = initFilters(7);
//                        filters.get(4).key = "<";
//                        filters.get(4).value = getRandomProdYear();
                        query = new Query(topology, filters);
                        queries.add(query);
                    }
                }
            }
        }
        typeStartIndex = queryTypeBoundaries.get(queryTypeBoundaries.size() - 1);
        queryTypeBoundaries.add(queries.size());
        sampleQueriesOfType(typeStartIndex, queries.size(), SIZE_OF_EACH_TYPE);
        endTime = System.currentTimeMillis();
        System.out.println("FORK: " + ((endTime - startTime) / 1000.0) + " sec");

        startTime = System.currentTimeMillis();
        // PI:
        for (int i = 0; i < Labels.INFO_TYPES.length; ++i) {
            for (int j = 0; j < Labels.LINK_TYPES.length; ++j) {
                for (int k = 0; k < Labels.LINK_TYPES.length; ++k) {
                    if (j == k) continue;
                    for (int l = 0; l < Labels.ROLE_TYPES.length; ++l) {
                        topology = new Topology();
                        topology.addEdge(0, Labels.ALSO_KNOWN_AS, 1);
                        topology.addEdge(0, Labels.INFO_TYPES[i], 2);
                        topology.addEdge(0, Labels.LINK_TYPES[j], 3);
                        topology.addEdge(3, Labels.LINK_TYPES[k], 6);
                        topology.addEdge(4, Labels.ROLE_TYPES[l], 6);
                        topology.addEdge(5, Labels.IS_KEYWORD_OF, 6);

                        filters = initFilters(7);
//                        filters.get(6).key = "<";
//                        filters.get(6).value = getRandomProdYear();
                        query = new Query(topology, filters);
                        queries.add(query);
                    }
                }
            }
        }
        typeStartIndex = queryTypeBoundaries.get(queryTypeBoundaries.size() - 1);
        queryTypeBoundaries.add(queries.size());
        sampleQueriesOfType(typeStartIndex, queries.size(), SIZE_OF_EACH_TYPE);
        endTime = System.currentTimeMillis();
        System.out.println("PI: " + ((endTime - startTime) / 1000.0) + " sec");

        startTime = System.currentTimeMillis();
        // FORK33:
        for (int i = 0; i < Labels.LINK_TYPES.length; ++i) {
            for (int j = 0; j < Labels.LINK_TYPES.length; ++j) {
                if (i == j) continue;

                for (int k = 0; k < Labels.ROLE_TYPES.length; ++k) {
                    for (int l = 0; l < Labels.COMPANY_TYPES.length; ++l) {
                        topology = new Topology();
                        topology.addEdge(0, Labels.IS_KEYWORD_OF, 1);
                        topology.addEdge(1, Labels.LINK_TYPES[i], 2);
                        topology.addEdge(2, Labels.LINK_TYPES[j], 5);
                        topology.addEdge(3, Labels.COMPANY_TYPES[l], 5);
                        topology.addEdge(4, Labels.ROLE_TYPES[k], 5);
                        topology.addEdge(5, Labels.ALSO_KNOWN_AS, 6);

                        filters = initFilters(7);
                        query = new Query(topology, filters);
                        queries.add(query);
                    }
                }
            }
        }
        typeStartIndex = queryTypeBoundaries.get(queryTypeBoundaries.size() - 1);
        queryTypeBoundaries.add(queries.size());
        sampleQueriesOfType(typeStartIndex, queries.size(), SIZE_OF_EACH_TYPE);
        endTime = System.currentTimeMillis();
        System.out.println("FORK33: " + ((endTime - startTime) / 1000.0) + " sec");

        System.out.println("Boundaries: " + queryTypeBoundaries);
    }

    public AcyclicQueryEvaluation(String[] existingPathFiles) throws Exception {
        Topology topology;
        List<Pair<String, Integer>> filters;

        String operator;
        int literal;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (String file : existingPathFiles) {
            BufferedReader csvReader = new BufferedReader(new FileReader(file));
            String[] queryString, edge;
            String line = csvReader.readLine();
            while (null != line) {
                queryString = line.split(",");

                int numEdges = Integer.parseInt(queryString[0]);

                topology = new Topology();
                for (int e = 1; e <= numEdges; e++) {
                    edge = queryString[e].split("(-\\[)|(]>)");
                    topology.addEdge(
                        Integer.parseInt(edge[0]),
                        Integer.parseInt(edge[1]),
                        Integer.parseInt(edge[2])
                    );
                }

                int numFilters = Integer.parseInt(queryString[numEdges + 1]);

                filters = initFilters(numFilters);
                for (int f = 0; f < numFilters; ++f) {
                    String filter = queryString[f + numEdges + 2];
                    if (!filter.equals("-1")) {
                        if (filter.charAt(1) == '=') {
                            operator = filter.substring(0, 2);
                            literal = Integer.parseInt(filter.substring(2));
                        } else {
                            operator = filter.substring(0, 1);
                            literal = Integer.parseInt(filter.substring(1));
                        }

                        filters.get(f).key = operator;
                        filters.get(f).value = literal;
                    }
                }

                queries.add(new Query(topology, filters));
                trueCard.add(Long.parseLong(queryString[queryString.length - 1]));

                line = csvReader.readLine();
            }

            csvReader.close();
            queryTypeBoundaries.add(queries.size());
        }

        endTime = System.currentTimeMillis();
        System.out.println("Query Loading: " + ((endTime - startTime) / 1000.0) + " sec");
    }

    public static void main(String[] args) throws Exception {
        String method = args[0];
        System.out.println("estimationMethod: " + method);

        final int NUM_QUERY_EACH_TYPE = 200;
        final int NUM_QUERY_TYPES = 6;

        BufferedWriter resultWriter = new BufferedWriter(new FileWriter("estimation.csv"));
        AcyclicQueryEvaluation queryEvaluation = null;
            //new AcyclicQueryEvaluation(args[args.length - 1].split(","));
//        AcyclicQueryEvaluation queryEvaluation = new AcyclicQueryEvaluation(6);
//        queryEvaluation.sampleQueries(NUM_QUERY_EACH_TYPE * NUM_QUERY_TYPES);
//        queryEvaluation.sampleQueries(NUM_QUERY_EACH_TYPE);
        List<Query> queries = Query.readQueries(args[args.length - 1]);

        int numQueries = queries.size();
        double progress = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        if (method.contains("sampling")) {
            System.out.println("graphFile: " + args[1]);
            System.out.println("propFile: " + args[2]);
            System.out.println("sampleSize: " + args[3]);
            System.out.println("queries: " + args[4]);
            System.out.println();

            double estimation;
            NodePureSampling nodePureSampling = new NodePureSampling(args[1], args[2], Integer.parseInt(args[3]));

            System.out.println("------- EVALUATION RESULT -------");
            for (Query query : queryEvaluation.queries) {
                estimation = nodePureSampling.estimate(query);
                resultWriter.write(query.toString() + "," + estimation + "\n");

                progress += 100.0 / numQueries;
                System.out.print("\rEstimating: " + (int) progress + "%");
            }
        } else if (method.contains("charmt")) {
            System.out.println("graphFile: " + args[1]);
            System.out.println("propFile: " + args[2]);
            System.out.println("saveOrLoad: " + args[3]);
            System.out.println("mtFileString: " + args[4]);
            System.out.println("sampleFileString: " + args[5]);
            System.out.println("numMTSamples: " + args[6]);
            System.out.println("queries: " + args[7]);
            System.out.println();

            Double[] estimations;
            CSetsMTCombined cSetsMTCombined = new CSetsMTCombined(
                args[1], args[2], args[3], args[4], args[5], Integer.parseInt(args[6])
            );
            for (Query query : queryEvaluation.queries) {
                estimations = cSetsMTCombined.estimate(query);
                StringJoiner sj = new StringJoiner(",");
                sj.add(query.toString())
                    .add(estimations[0].toString())
                    .add(estimations[1].toString());
                resultWriter.write(sj.toString() + "\n");

                progress += 100.0 / numQueries;
                System.out.print("\rEstimating: " + (int) progress + "%");
            }
        } else if (method.contains("mt")) {
            System.out.println("mtFiles: " + args[1]);
            System.out.println("sampleFiles: " + args[2]);
            System.out.println("propFile: " + args[3]);
            System.out.println("queries: " + args[4]);
            System.out.println();

            String[] mtFiles = args[1].split(",");
            String[] sampleFiles = args[2].split(",");

            Double[] estimations;
            MT mt = new MT(mtFiles, sampleFiles, args[3]);
            System.out.println("------- EVALUATION RESULT -------");
            for (Query query : queryEvaluation.queries) {
                estimations = mt.estimate(query);
                StringJoiner sj = new StringJoiner(",");
                sj.add(query.toString())
                  .add(estimations[0].toString())
                  .add(estimations[1].toString())
                  .add(estimations[2].toString());
                resultWriter.write(sj.toString() + "\n");

                progress += 100.0 / numQueries;
                System.out.print("\rEstimating: " + (int) progress + "%");
            }
        } else if (method.contains("char")) {
            System.out.println("graphFile: " + args[1]);
            System.out.println("propFile: " + args[2]);
            System.out.println("saveOrLoad: " + args[3]);
            System.out.println("queries: " + args[4]);
            System.out.println();

            double estimation;
            CharacteristicSets cSets = new CharacteristicSets(args[1], args[2], args[3]);
            System.out.println("------- EVALUATION RESULT -------");
            for (Query query : queryEvaluation.queries) {
                estimation = cSets.estimate(query);
                resultWriter.write(query.toString() + "," + estimation + "\n");

                progress += 100.0 / numQueries;
                System.out.print("\rEstimating: " + (int) progress + "%");
            }
        } else if (method.contains("cvcat")) {
            System.out.println("catalogue: " + args[1]);
            System.out.println("starCVFiles: " + args[2]);
            System.out.println("forkCVFiles: " + args[3]);
            System.out.println("pathCVFiles: " + args[4]);
            System.out.println("queries: " + args[5]);
            System.out.println();

            Pair<Double, Double> estimation;
            CVCatalogue cvCatalogue = new CVCatalogue(args[1], null, args[3], args[4]);
            for (int i = 0; i < queryEvaluation.queries.size(); ++i) {
                // TODO: to remove
                if (i / NUM_QUERY_EACH_TYPE != 3) continue;

                Query query = queryEvaluation.queries.get(i);
                estimation = cvCatalogue.estimate(query, i / NUM_QUERY_EACH_TYPE, Constants.C_ALL);
                resultWriter.write(query.toString() + "," + estimation + "\n");

                progress += 100.0 / numQueries;
                System.out.print("\rEstimating: " + (int) progress + "%");
            }
        } else if (method.contains("entropy")) {
            System.out.println("catalogueFile: " + args[1]);
            System.out.println("entropyFile: " + args[2]);
            System.out.println("patternType: " + args[3]);
            System.out.println("formulaType: " + args[4]);
            System.out.println("queryFile: " + args[5]);
            System.out.println();

            int formulaType = Util.getFormulaType(args[4]);
            if (formulaType == -1) return;

            Pair<Double, Double> entropyAndEstimation;
            CVCatalogue cvCatalogue = new CVCatalogue(args[1], args[2]);
            System.out.println("------- EVALUATION RESULT -------");
            for (int i = 0; i < queryEvaluation.queries.size(); ++i) {
                Query query = queryEvaluation.queries.get(i);
                entropyAndEstimation = cvCatalogue.estimate(query, Integer.parseInt(args[3]), formulaType);
                resultWriter.write(query.toString() + "," + entropyAndEstimation + "\n");

                progress += 100.0 / numQueries;
                System.out.print("\rEstimating: " + (int) progress + "%");
            }
        } else if (method.contains("pcat")) {
            System.out.println("partType: " + args[1]);
            System.out.println("pcatFile: " + args[2]);
            System.out.println("maxLen: " + args[3]);
            System.out.println("patternType: " + args[4]);
            System.out.println("formulaType: " + args[5]);
            System.out.println("queries: " + args[6]);
            System.out.println();

            int formulaType = Util.getFormulaType(args[5]);
            if (formulaType == -1) return;

            int partType = args[1].contains("hash") ? Constants.HASH : Constants.DEG;
            int catLen = Integer.parseInt(args[3]);
            int patternType = Integer.parseInt(args[4]);

            List<Thread> threads = new ArrayList<>();
            List<PartitionedCatalogueRunnable> runnables = new ArrayList<>();
            PartitionedCatalogueRunnable runnable;
            Thread thread;

            PartitionedCatalogue pcat = new PartitionedCatalogue(args[2], catLen);
            // hacky way to fill hashIdCombs in PartitionedCatalogue
            pcat.prepareEstimate(queryEvaluation.queries.get(0), partType);

            System.out.println("------- EVALUATION RESULT -------");
            for (int i = 0; i < queryEvaluation.queries.size(); ++i) {
                Query query = queryEvaluation.queries.get(i);
                runnable = new PartitionedCatalogueRunnable(
                    i, pcat, query, patternType, formulaType, catLen);
                runnables.add(runnable);
                thread = new Thread(runnable);
                threads.add(thread);
                thread.start();
            }

            for (Thread t : threads) {
                t.join();
            }

            Double[] estimations;
            for (int i = 0; i < queryEvaluation.queries.size(); ++i) {
                Query query = queryEvaluation.queries.get(i);
                estimations = runnables.get(i).getEstimates();
                StringJoiner sj = new StringJoiner(",");
                sj.add(query.toString())
                    .add(estimations[0].toString())
                    .add(estimations[1].toString())
                    .add(estimations[2].toString());
                resultWriter.write(sj.toString() + "\n");

                progress += 100.0 / numQueries;
                System.out.print("\rEstimating: " + (int) progress + "%");
            }
        } else if (method.contains("cat")) {
            System.out.println("catalogueFile: " + args[1]);
            System.out.println("debug: " + args[2]);
            System.out.println("maxLen: " + args[3]);
            System.out.println("patternType: " + args[4]);
            System.out.println("formulaType: " + args[5]);
            System.out.println("queries: " + args[6]);
            System.out.println();

            boolean debug = Boolean.parseBoolean(args[2]);
            int formulaType = Util.getFormulaType(args[5]);
            if (formulaType == -1) return;

            int catLen = Integer.parseInt(args[3]);

            Double[] estimations;
            Catalogue catalogue = new Catalogue(args[1], catLen);
            System.out.println("------- EVALUATION RESULT -------");
            for (int i = 0; i < queries.size(); ++i) {
                Query query = queries.get(i);
//                estimations = catalogue.estimate(query, i / NUM_QUERY_EACH_TYPE, debug);
                estimations = catalogue.estimateByHops(query, Integer.parseInt(args[4]), formulaType, catLen);

                StringJoiner sj = new StringJoiner(",");
                sj.add(query.toString());
                for (Double est : estimations) {
                    sj.add(est.toString());
                }
                resultWriter.write(sj.toString() + "\n");

                progress += 100.0 / numQueries;
                if (!debug) {
                    System.out.print("\rEstimating: " + (int) progress + "%");
                }
            }
        } else if (method.contains("pess")) {
            System.out.println("catalogueFile: " + args[1]);
            System.out.println("maxDegFile: " + args[2]);
            System.out.println("patternType: " + args[3]);
            System.out.println("queries: " + args[4]);
            System.out.println();

            Pessimistic pessimistic = new Pessimistic(args[1], args[2]);
            System.out.println("------- EVALUATION RESULT -------");
            for (int i = 0; i < queryEvaluation.queries.size(); ++i) {
                Query query = queryEvaluation.queries.get(i);
                Integer patternType = Integer.parseInt(args[3]);

                double estWithoutSubmod = pessimistic.estimate(query, patternType, false);
                double estWithSubmod = pessimistic.estimate(query, patternType, true);

                resultWriter.write(
                    query.toString() + "," + estWithoutSubmod + "," + estWithSubmod + "\n");

                progress += 100.0 / numQueries;
                System.out.print("\rEstimating: " + (int) progress + "%");
            }
        } else if (method.contains("pcllp")) {
            System.out.println("partType: " + args[1]);
            System.out.println("maxDegFile: " + args[2]);
            System.out.println("labelCountFile: " + args[3]);
            System.out.println("catalogueFile: " + args[4]);
            System.out.println("catMaxDegFile: " + args[5]);
            System.out.println("queries: " + args[6]);
            System.out.println();

            int partType = args[1].contains("hash") ? Constants.HASH : Constants.DEG;
            String catalogueFile = args[4].contains("null") ? null : args[4];
            String catMaxDegFile = args[5].contains("null") ? null : args[5];

            PartitionedCLLP cllp =
                new PartitionedCLLP(args[2], args[3], catalogueFile, catMaxDegFile);
            // hacky way to fill hashIdCombs in PartitionedCatalogue
            cllp.prepareEstimate(queries.get(0), partType);

            System.out.println("------- EVALUATION RESULT -------");
            for (int i = 0; i < queries.size(); ++i) {
                Query query = queries.get(i);

                double estWithoutSubmod = cllp.estimate(query, false);
                double estWithSubmod = cllp.estimate(query, true);

                resultWriter.write(
                    query.toString() + "," + estWithoutSubmod + "," + estWithSubmod + "\n");

                progress += 100.0 / numQueries;
                System.out.print("\rEstimating: " + (int) progress + "%");
            }
        } else if (method.contains("cllp")) {
            System.out.println("maxDegFile: " + args[1]);
            System.out.println("labelCountFile: " + args[2]);
            System.out.println("catalogueFile: " + args[3]);
            System.out.println("catalogueMaxLen: " + args[4]);
            System.out.println("catMaxDegFile: " + args[5]);
            System.out.println("queries: " + args[6]);
            System.out.println();

            String catalogueFile = args[3].contains("null") ? null : args[3];
            int catMaxLen = args[3].contains("null") ? 0 : Integer.parseInt(args[4]);
            String catMaxDegFile = args[5].contains("null") ? null : args[5];

            CLLP cllp = new CLLP(args[1], args[2], catalogueFile, catMaxLen, catMaxDegFile);
            System.out.println("------- EVALUATION RESULT -------");
            for (int i = 0; i < queries.size(); ++i) {
                Query query = queries.get(i);

                double estWithoutSubmod = cllp.estimate(query, false);
                double estWithSubmod = cllp.estimate(query, true);

                resultWriter.write(
                    query.toString() + "," + estWithoutSubmod + "," + estWithSubmod + "\n");

                progress += 100.0 / numQueries;
                System.out.print("\rEstimating: " + (int) progress + "%");
            }
        } else if (method.contains("true")) {
            System.out.println("queries: " + args[1]);
            System.out.println();

            for (int i = 0; i < queryEvaluation.queries.size(); ++i) {
                Query query = queryEvaluation.queries.get(i);
                Long actual = queryEvaluation.trueCard.get(i);
                resultWriter.write(query.toString() + "," + actual + "\n");

                progress += 100.0 / numQueries;
                System.out.print("\rExtracting: " + (int) progress + "%");
            }
        }
        resultWriter.close();

        endTime = System.currentTimeMillis();
        System.out.println("\nEstimating: " + ((endTime - startTime) / 1000.0) + " sec");
    }
}