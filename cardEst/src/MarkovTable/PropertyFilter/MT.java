package MarkovTable.PropertyFilter;

import Common.Pair;
import Common.Path;
import Common.Query;
import Common.Topology;
import IMDB.Labels;
import MarkovTable.Constants;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MT {
    // length -> type (index) -> mt
    public Map<Integer, List<Map<Path, Long>>> mts = new HashMap<>();

    public Map<Integer, List<Map<Path, List<List<Integer>>>>> samples = new HashMap<>();

    public Map<Integer, Integer> vid2prodYear = new HashMap<>();

    // length -> type (index) -> {<path, vList>}
    public Map<Integer, List<Set<Pair<LinkedHashSet<Integer>, List<Integer>>>>> decomPaths = new HashMap<>();

    private Set<Pair<LinkedHashSet<Integer>, List<Integer>>> get3Paths(
            Topology topology, int backwardDir, int forwardDir) {
        List<Map<Integer, List<Integer>>> backward;
        List<Map<Integer, List<Integer>>> forward;

        if (backwardDir == Constants.OUTGOING) {
            backward = topology.outgoing;
        } else {
            backward = topology.incoming;
        }

        if (forwardDir == Constants.OUTGOING) {
            forward = topology.outgoing;
        } else {
            forward = topology.incoming;
        }

        List<Integer> vertexList;
        LinkedHashSet<Integer> path;

        Set<Pair<LinkedHashSet<Integer>, List<Integer>>> paths = new HashSet<>();

        for (int midLeft = 0; midLeft < topology.src2dest2label.size(); ++midLeft) {
            for (Integer midRight : topology.src2dest2label.get(midLeft).keySet()) {
                Integer midLabel = topology.src2dest2label.get(midLeft).get(midRight);
                for (Integer backLabel : backward.get(midLeft).keySet()) {
                    for (Integer backEnd : backward.get(midLeft).get(backLabel)) {
                        for (Integer forLabel : forward.get(midRight).keySet()) {
                            for (Integer forEnd : forward.get(midRight).get(forLabel)) {
                                if (backwardDir == Constants.OUTGOING &&
                                    midLabel.equals(backLabel) && backEnd.equals(midRight)) continue;
                                if (forwardDir == Constants.INCOMING &&
                                    midLabel.equals(forLabel) && forEnd.equals(midLeft)) continue;

                                path = new LinkedHashSet<>();
                                path.add(backLabel);
                                path.add(midLabel);
                                path.add(forLabel);

                                vertexList = new ArrayList<>();
                                vertexList.add(backEnd);
                                vertexList.add(midLeft);
                                vertexList.add(midRight);
                                vertexList.add(forEnd);

                                paths.add(new Pair<>(path, vertexList));
                            }
                        }
                    }
                }
            }
        }

        return paths;
    }

    public void decomTo3Paths(Topology topology) {
        decomPaths.putIfAbsent(3, new ArrayList<>());
        decomPaths.get(3).clear();
        decomPaths.get(3).add(get3Paths(topology, Constants.INCOMING, Constants.OUTGOING));
        decomPaths.get(3).add(get3Paths(topology, Constants.INCOMING, Constants.INCOMING));
        decomPaths.get(3).add(get3Paths(topology, Constants.OUTGOING, Constants.OUTGOING));
        decomPaths.get(3).add(get3Paths(topology, Constants.OUTGOING, Constants.INCOMING));
    }

    private void decomTo2Paths(Topology topology) {
        Map<Integer, List<Integer>> firstStepNodes2prefixes = new HashMap<>();
        List<Integer> prefixes, vertexList;
        LinkedHashSet<Integer> path;
        Set<Pair<LinkedHashSet<Integer>, List<Integer>>> pathsType1 = new HashSet<>();
        Set<Pair<LinkedHashSet<Integer>, List<Integer>>> pathsType2 = new HashSet<>();
        Set<Pair<LinkedHashSet<Integer>, List<Integer>>> pathsType3 = new HashSet<>();

        for (int i = 0; i < topology.outgoing.size(); ++i) {
            firstStepNodes2prefixes.clear();

            for (Integer label : topology.outgoing.get(i).keySet()) {
                for (Integer dest : topology.outgoing.get(i).get(label)) {
                    prefixes = new ArrayList<>();
                    prefixes.add(label);
                    firstStepNodes2prefixes.putIfAbsent(dest, prefixes);
                }
            }

            for (Integer middle : firstStepNodes2prefixes.keySet()) {
                for (Integer prefix : firstStepNodes2prefixes.get(middle)) {
                    for (Integer label : topology.outgoing.get(middle).keySet()) {
                        path = new LinkedHashSet<>();
                        path.add(prefix);
                        path.add(label);

                        vertexList = new ArrayList<>();
                        vertexList.add(i);
                        vertexList.add(middle);
                        vertexList.add(topology.outgoing.get(middle).get(label).get(0));

                        pathsType1.add(new Pair<>(path, vertexList));
                    }

                    for (Integer label : topology.incoming.get(middle).keySet()) {
                        if (topology.incoming.get(middle).get(label).size() == 1 &&
                            topology.incoming.get(middle).get(label).get(0) == i) continue;

                        path = new LinkedHashSet<>();
                        path.add(prefix);
                        path.add(label);

                        vertexList = new ArrayList<>();
                        vertexList.add(i);
                        vertexList.add(middle);
                        vertexList.add(topology.incoming.get(middle).get(label).get(0));

                        pathsType2.add(new Pair<>(path, vertexList));
                    }
                }
            }

            firstStepNodes2prefixes.clear();

            for (Integer label : topology.incoming.get(i).keySet()) {
                for (Integer src : topology.incoming.get(i).get(label)) {
                    prefixes = new ArrayList<>();
                    prefixes.add(label);
                    firstStepNodes2prefixes.putIfAbsent(src, prefixes);
                }
            }

            for (Integer middle : firstStepNodes2prefixes.keySet()) {
                for (Integer prefix : firstStepNodes2prefixes.get(middle)) {
                    for (Integer label : topology.outgoing.get(middle).keySet()) {
                        if (topology.outgoing.get(middle).get(label).size() == 1 &&
                            topology.outgoing.get(middle).get(label).get(0) == i) continue;

                        path = new LinkedHashSet<>();
                        path.add(prefix);
                        path.add(label);

                        vertexList = new ArrayList<>();
                        vertexList.add(i);
                        vertexList.add(middle);
                        vertexList.add(topology.outgoing.get(middle).get(label).get(0));

                        pathsType3.add(new Pair<>(path, vertexList));
                    }
                }
            }
        }

        decomPaths.putIfAbsent(2, new ArrayList<>());
        decomPaths.get(2).clear();
        decomPaths.get(2).add(pathsType1);
        decomPaths.get(2).add(pathsType2);
        decomPaths.get(2).add(pathsType3);
    }

    public Path toPath(LinkedHashSet<Integer> twoPath) {
        List<Integer> edgeList = new ArrayList<>();
        edgeList.addAll(twoPath);
        return new Path(edgeList);
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

    public double computeProportion(Query query, int entryType) throws Exception {
        double numQualified = 0;

        for (List<Integer> sample: samples.get(query.path.length()).get(entryType).get(query.path)) {
            boolean qualified = applyFilter(sample, query.filters);
            if (qualified) numQualified++;
        }

        double proportion = numQualified / samples.get(query.path.length()).get(entryType).get(query.path).size();
//        System.out.println(numQualified + " / " + samples.get(query.path).size() + " = " + proportion);
        return proportion;
    }

    public List<Pair<String, Integer>> extractFilters(
            List<Pair<String, Integer>> filters, List<Integer> vertexList) {

        List<Pair<String, Integer>> extracted = new ArrayList<>();
        for (Integer rVertex : vertexList) {
            extracted.add(filters.get(rVertex));
        }

        return extracted;
    }

    private boolean hasAllLabels(Set<Pair<Set<Integer>, Double>> alreadyCovered, Set<Integer> all) {
        for (Pair<Set<Integer>, Double> coveredLabelsAndCard : alreadyCovered) {
            if (coveredLabelsAndCard.key.equals(all)) return true;
        }
        return false;
    }

    public Pair<Double, Double> estimate3Path(Query query, int entryType) throws Exception {
        int prefixType = 0;
        int suffixType = 0;
        switch (entryType) {
            case Constants.ENTRY_TYPE_1:
                prefixType = Constants.ENTRY_TYPE_1;
                suffixType = Constants.ENTRY_TYPE_1;
                break;
            case Constants.ENTRY_TYPE_2:
                prefixType = Constants.ENTRY_TYPE_1;
                suffixType = Constants.ENTRY_TYPE_2;
                break;
            case Constants.ENTRY_TYPE_3:
                prefixType = Constants.ENTRY_TYPE_3;
                suffixType = Constants.ENTRY_TYPE_1;
                break;
            case Constants.ENTRY_TYPE_4:
                prefixType = Constants.ENTRY_TYPE_3;
                suffixType = Constants.ENTRY_TYPE_2;
                break;
        }

        Query prefix = new Query(query.path.subPath(0, 2), query.filters.subList(0, 3));
        Query suffix = new Query(query.path.subPath(1, 3), query.filters.subList(1, 4));
        Query middle = new Query(query.path.subPath(1, 2), query.filters.subList(1, 3));

        double prefixProportion = computeProportion(prefix, prefixType);
        double suffixProportion = computeProportion(suffix, suffixType);
        double middleProportion = computeProportion(middle, Constants.ENTRY_TYPE_1);

        long prefixCard = mts.get(2).get(prefixType).get(prefix.path);
        long suffixCard = mts.get(2).get(suffixType).get(suffix.path);
        long middleCard = mts.get(1).get(Constants.ENTRY_TYPE_1).get(middle.path);

        return new Pair<>(
            ((double) prefixCard) / middleCard * suffixCard,
            prefixProportion / middleProportion * suffixProportion
        );
    }

    private Set<Integer> getAllLabels(Topology topology) {
        Set<Integer> allLabels = new HashSet<>();
        for (Map<Integer, List<Integer>> label2dests : topology.outgoing) {
            allLabels.addAll(label2dests.keySet());
        }
        return allLabels;
    }

    public int getPathType(List<Integer> vList) {
        int middleLeftIndex = -1;
        int middleRightIndex = -1;
        if (vList.size() == 4) {
            middleLeftIndex = 1;
            middleRightIndex = 2;
        } else if (vList.size() == 3) {
            middleLeftIndex = 1;
            middleRightIndex = 1;
        } else if (vList.size() == 2) {
            return Constants.ENTRY_TYPE_1;
        }

        if (vList.get(0) < vList.get(middleLeftIndex)) {
            if (vList.get(middleRightIndex) < vList.get(vList.size() - 1)) {
                return Constants.ENTRY_TYPE_1;
            } else {
                return Constants.ENTRY_TYPE_2;
            }
        } else {
            if (vList.get(middleRightIndex) < vList.get(vList.size() - 1)) {
                return Constants.ENTRY_TYPE_3;
            } else {
                return Constants.ENTRY_TYPE_4;
            }
        }
    }

    public Query getOverlappedQuery(Pair<LinkedHashSet<Integer>, List<Integer>> pathAndVList,
            Set<Integer> coveredLabels, List<Pair<String, Integer>> filters, List<Integer> vList) {
        Set<Integer> intersection = new HashSet<>(pathAndVList.key);
        intersection.retainAll(coveredLabels);

        Path path = new Path(new ArrayList<>());
        int vIndex = 0;
        List<Integer> relevantVList = new ArrayList<>();
        if (pathAndVList.key.size() == 2 && intersection.size() == 1) {
            for (Integer label : pathAndVList.key) {
                if (intersection.contains(label)) {
                    path.append(label);
                    relevantVList.addAll(pathAndVList.value.subList(vIndex, vIndex + 2));
                    vList.addAll(relevantVList);
                    return new Query(path, extractFilters(filters, relevantVList));
                }
                vIndex++;
            }
        } else if (pathAndVList.key.size() == 3 && intersection.size() == 2) {
            for (Integer label : pathAndVList.key) {
                if (intersection.contains(label)) {
                    path.append(label);
                    if (path.length() == 2) {
                        relevantVList.add(pathAndVList.value.get(vIndex + 1));
                    } else {
                        relevantVList.addAll(pathAndVList.value.subList(vIndex, vIndex + 2));
                    }

                    if (path.length() == 2) {
                        vList.addAll(relevantVList);
                        return new Query(path, extractFilters(filters, relevantVList));
                    }
                } else {
                    path.getEdgeLabelList().clear();
                    relevantVList.clear();
                }
                vIndex++;
            }
        }

        return null;
    }

    private Set<Pair<Set<Integer>, Double>> extend(Query query, Set<Pair<Set<Integer>, Double>>
            currentCoveredLabelsAndCard, int len) throws Exception {
        Path path;
        Query subquery;
        double numeratorProportion, denominatorProportion;
        List<Integer> vList = new ArrayList<>();
        Set<Integer> coveredLabels;
        Set<Pair<Set<Integer>, Double>> nextCoveredLabelsAndCard = new HashSet<>();
        Pair<Set<Integer>, Double> labelsAndCard;

        for (int i = 0; i < decomPaths.get(len).size(); ++i) {
            Set<Pair<LinkedHashSet<Integer>, List<Integer>>> pathsOfType = decomPaths.get(len).get(i);
            for (Pair<LinkedHashSet<Integer>, List<Integer>> mergingPath : pathsOfType) {
                path = toPath(mergingPath.key);

                subquery = new Query(path, extractFilters(query.filters, mergingPath.value));

                double numeratorCard = 0;
                if (mts.get(len).get(i).containsKey(path)) {
                    numeratorProportion = computeProportion(subquery, i);
                } else if (len == 3) {
                    Pair<Double, Double> cardAndProportion = estimate3Path(subquery, i);
                    numeratorCard = cardAndProportion.key;
                    numeratorProportion = cardAndProportion.value;
                } else {
                    Path reversed = new Path(path);
                    Collections.reverse(reversed.getEdgeLabelList());
                    if (mts.get(len).get(i).containsKey(reversed)) continue;

                    numeratorProportion = 0;
                }

                for (Pair<Set<Integer>, Double> coveredAndCard : currentCoveredLabelsAndCard) {
                    nextCoveredLabelsAndCard.add(coveredAndCard);

                    vList.clear();
                    subquery = getOverlappedQuery(mergingPath, coveredAndCard.key, query.filters, vList);
                    if (null == subquery) continue;

                    int overlappedType = getPathType(vList);
                    denominatorProportion = computeProportion(subquery, overlappedType);

                    coveredLabels = new HashSet<>();
                    coveredLabels.addAll(coveredAndCard.key);
                    coveredLabels.addAll(mergingPath.key);

                    if (numeratorCard > 0 || mts.get(len).get(i).containsKey(path)) {
                        double numerator = numeratorProportion;
                        if (numeratorCard > 0) {
                            numerator *= numeratorCard;
                        } else {
                            numerator *= mts.get(len).get(i).get(path);
                        }

                        double denominator = denominatorProportion;
                        if (subquery.path.length() == 2) {
                            denominator *= mts.get(2).get(overlappedType).get(subquery.path);
                        } else {
                            denominator *= mts.get(1).get(0).get(subquery.path);
                        }

                        labelsAndCard = new Pair<>(
                            coveredLabels, coveredAndCard.value / denominator * numerator
                        );
                    } else {
                        labelsAndCard = new Pair<>(coveredLabels, 0.0);
                    }
                    nextCoveredLabelsAndCard.add(labelsAndCard);
                }
                currentCoveredLabelsAndCard = nextCoveredLabelsAndCard;
                nextCoveredLabelsAndCard = new HashSet<>();
            }
        }

        return currentCoveredLabelsAndCard;
    }

    public Double[] estimate(Query query) throws Exception {
        decomTo2Paths(query.topology);
        decomTo3Paths(query.topology);

        double numeratorProportion;
        Path startPath;
        Query subquery;
        Set<Integer> allLabels = getAllLabels(query.topology);
        Set<Pair<Set<Integer>, Double>> alreadyCovered = new HashSet<>();
        Set<Pair<Set<Integer>, Double>> coveredLabelsAndCard = new HashSet<>();
        Pair<Set<Integer>, Double> labelsAndCard;

        for (int len = 3; len >= 2; --len) {
            if (len == 2 && hasAllLabels(alreadyCovered, allLabels)) break;

            boolean no3Paths = len == 2 && alreadyCovered.isEmpty();
            for (int s = 0; s < decomPaths.get(len).size(); ++s) {
                Set<Pair<LinkedHashSet<Integer>, List<Integer>>> startPathOfType = decomPaths.get(len).get(s);

                if (len == 3 || no3Paths) {
                    for (Pair<LinkedHashSet<Integer>, List<Integer>> startPathVList : startPathOfType) {
                        startPath = toPath(startPathVList.key);
                        subquery = new Query(startPath, extractFilters(query.filters, startPathVList.value));
                        if (mts.get(len).get(s).containsKey(startPath)) {
                            numeratorProportion = computeProportion(subquery, s);
                            labelsAndCard = new Pair<>(
                                new HashSet<>(startPathVList.key),
                                mts.get(len).get(s).get(startPath) * numeratorProportion
                            );
                        } else if (len == 2) {
                            Path reversed = new Path(startPath);
                            Collections.reverse(reversed.getEdgeLabelList());
                            if (mts.get(len).get(s).containsKey(reversed)) continue;

                            labelsAndCard = new Pair<>(new HashSet<>(startPathVList.key), 0.0);
                        } else {
                            Pair<Double, Double> cardAndProportion = estimate3Path(subquery, s);
                            labelsAndCard = new Pair<>(
                                new HashSet<>(startPathVList.key),
                                cardAndProportion.key * cardAndProportion.value
                            );
                        }
                        coveredLabelsAndCard.add(labelsAndCard);
                        coveredLabelsAndCard = extend(query, coveredLabelsAndCard, len);
                        alreadyCovered.addAll(coveredLabelsAndCard);
                    }
                } else {
                    coveredLabelsAndCard = extend(query, coveredLabelsAndCard, len);
                    alreadyCovered.addAll(coveredLabelsAndCard);
                }
            }
        }

        int numFormula = 0;
        double sum = 0, min = Double.MAX_VALUE, max = -Double.MAX_VALUE;
        for (Pair<Set<Integer>, Double> labelsAndEst : alreadyCovered) {
            if (!labelsAndEst.key.equals(allLabels)) continue;
            numFormula++;

            sum += labelsAndEst.value;
            min = Math.min(min, labelsAndEst.value);
            max = Math.max(max, labelsAndEst.value);
        }

        return new Double[] { sum / numFormula, min, max };
    }

    public MT(String[] mtFiles, String[] sampleFiles, String propFile) throws Exception {
        String[] pathAndCount, labelList;
        Path path;
        String line;
        int pathLength = 0;

        long startTime = System.currentTimeMillis();
        long endTime;

        for (String mtFile : mtFiles) {
            Map<Path, Long> mt = new HashMap<>();
            BufferedReader mtReader = new BufferedReader(new FileReader(mtFile));
            line = mtReader.readLine();
            while (null != line) {
                pathAndCount = line.split(",");

                labelList = pathAndCount[0].split("->");
                path = new Path(new ArrayList<>());
                for (String label : labelList) {
                    path.append(Integer.parseInt(label));
                }
                pathLength = path.length();

                mt.put(path, Long.parseLong(pathAndCount[1]));

                line = mtReader.readLine();
            }
            mtReader.close();

            mts.putIfAbsent(pathLength, new ArrayList<>());
            mts.get(pathLength).add(mt);
        }

        String[] pathAndSamples, vertexList;
        List<Integer> sample;

        for (String sampleFile : sampleFiles) {
            Map<Path, List<List<Integer>>> samplesOfMT = new HashMap<>();
            BufferedReader sampleReader = new BufferedReader(new FileReader(sampleFile));
            line = sampleReader.readLine();
            while (null != line) {
                pathAndSamples = line.split(",");

                labelList = pathAndSamples[0].split("->");
                path = new Path(new ArrayList<>());
                for (String label : labelList) {
                    path.append(Integer.parseInt(label));
                }
                pathLength = path.length();

                samplesOfMT.putIfAbsent(path, new ArrayList<>());
                for (int i = 1; i < pathAndSamples.length; ++i) {
                    vertexList = pathAndSamples[i].split("->");
                    sample = new ArrayList<>();
                    for (String v : vertexList) {
                        sample.add(Integer.parseInt(v));
                    }
                    samplesOfMT.get(path).add(sample);
                }

                line = sampleReader.readLine();
            }
            sampleReader.close();

            samples.putIfAbsent(pathLength, new ArrayList<>());
            samples.get(pathLength).add(samplesOfMT);
        }

        String[] properties;

        BufferedReader tsvReader = new BufferedReader(new FileReader(propFile));
        tsvReader.readLine(); // Header

        line = tsvReader.readLine();
        while (null != line) {
            properties = line.split("\t");
            if (!properties[Labels.PROD_YEAR_INDEX].isEmpty()) {
                vid2prodYear.put(
                    Integer.parseInt(properties[0]), Integer.parseInt(properties[Labels.PROD_YEAR_INDEX])
                );
            }
            line = tsvReader.readLine();
        }
        tsvReader.close();

        endTime = System.currentTimeMillis();
        System.out.println("Loading MT: " + ((endTime - startTime) / 1000.0) + " sec");
    }
}
