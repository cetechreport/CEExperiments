# CEExperimentsSIGMOD

The datasets and queries will be made public upon publication of our technical paper.

## Query and dataset format

1. Query
  * A query contains 3 parts: edges, edge labels, true cardinality, separated by ```,```. 
  * Each edge is denoted by ```<src>-<dest>```, separated by ```;```. Edges should be in lexicographical order.
  * Edge lables should have the same order as edges and joined with ```->```.
  * The following is an example of a 4-path with edge label 1,2,3,4 and true cardinality 10.
  ```
  0-1;1-2;2-3;3-4,1->2->3->4,10
  ```

2. Dataset
  * Each line in the dataset corresponds to one edge in the dataset graph.
  * Each line should be ```<src>,<label>,<dest>```.
  * The following is an example of a dataset.
  ```
  1,10,2
  3,10,4
  1,11,5
  ```

## Run CEExperiment
  We use the phrases "Markov table" and "Catalogue" interchangeably in the descriptions below.
* Decompose the query file. Note that all queries in one query file must be of the same shape, i.e., edges must be exactly the same and in the same order.
```
  java -Xmx100G -cp CEExperiments.jar Graphflow.LargeBenchmarkQueryDecomposer <query file>
```
* Generate Markov table (Acyclic queries)
```
  java -Xmx100G -cp CEExperiments.jar Graphflow.Catalogue <dataset> <query file>
```
* Generate Markov table (Cyclic queries)
```
  java -Xmx100G -cp CEExperiments.jar Graphflow.Catalogue <dataset> <query file>
  java -Xmx100G -cp CEExperiments.jar Graphflow.TriangleCatalogue <dataset> <query file> <catalogue destination> <catalogue max degree destination> <cycle closing method>
```
  cycle closing method is one of ```baseline```, ```allInclusive```, and ```avgSampledExentsionRate```
  
## Acyclic Cardinality Estimation
```
java -Xmx100G -cp CEExperiments.jar IMDB.AcyclicQueryEvaluation cat <dataset> <debug> <max len> <formula type> <random> <query file>
```
  in our experiments, we set ```<debug>``` as ```false```, ```<max len>``` as ```3```, ```<formula type>``` as ```all```, and ```random``` as ```false```.
* All queries in the query file must be of the same shape, i.e., edges must be exactly the same and in the same order.
* Each line in the output file has the format of edges, labels, estimations. The order of estimations is all-min, all-max, all-avg, min-min, min-max, min-avg, max-min, max-max, max-avg, and p*.

## Cyclic Cardinality Estimation
```
java -Xmx100G -cp CEExperiments.jar Graphflow.CyclicQueryEvaluation <cycle closing method> <catalogue file> <hop mode> <query file> <random> <all est>
```
  in our experiments, we set ```<hop mode>``` as ```true```, ```<random>``` as ```false```, and ```<all est>``` as ```true```.
* All queries in the query file must be of the same shape, i.e., edges must be exactly the same and in the same order.
* Each line in the output file has the format of edges, labels, estimations. The order of estimations is all-min, all-max, all-avg, min-min, min-max, min-avg, max-min, max-max, max-avg, and p*.
* cycle closing method is one of ```baseline```, ```avgSampledExentsionRate```, and ```midEdge```
