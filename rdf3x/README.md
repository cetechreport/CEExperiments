# RDF-3X Experiment

We chose the open-source RDF database management system GH-RDF3X (an extension of the the original RDF-3X engine) from the GitHub repository https://github.com/gh-rdf3x/gh-rdf3x.
We modified the following files to achieve cardinality injection:
  * cts/plangen/PlanGen.cpp
  * include/cts/plangen/PlanGen.hpp
  * tools/rdf3xquery/rdf3xquery.cpp

## Run Experiment
* Go to the bin directory.
```
  cd bin/
```
* Load a dataset. Note that our datasets are stored such that each line is a comma delimited list of src, label, and dest. Therefore, we need to convert <data> to the N-Triples format first.
```
  convertToN3.py <data> data.n3
  ./rdf3xload data data.n3
```
* Query the dataset using a specified cardinality estimator and output an average query processing time.
```
  ./query_time <query file> <data> <estimator> <output file>
```
  For acyclic queries, estimator is one of ```minMin```, ```allMin```, ```maxMin```, ```minAvg```, ```allAvg```, ```maxAvg```, ```minMax```, ```allMax```, and ```maxMax```.
  For cyclic queries, estimator is one of ```baselineMinMin```, ```midEdgeMaxMax```, and ```trigExtMaxMax```. To use the plan that rdf3x selects without cardinality injection, pass in ```<estimator>``` as ```rdf3x```.
 
 Note that the jar file of the project CEExperiments should be put in the bin/ directory in order for the estimators to work.
