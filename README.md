# spark-sample-erp-bordeaux

---

## Installation

pip install -r requirements.txt

## Configuration
Configuration is in resources/configuration/spark-erp-bordeaux.yml :
- to change field type to geometry type for parquet : *partition: "geometry_type"*
- to change input or output files : read.input or write.output
- activating or not the streaming was not carried out for questions of time

## Start

Exercice 1 :
- python(3/.exe) json_to_parquet.py

Exercice 2 and 3:
- python(3/.exe) compute_visitors.py

*() for Linux or Windows*
## Result
In resources/output/ :
- bor_erp.parquet (exercice 1) 
- street_visitors_accommodation.csv (exercices 2 and 3)

## Improvements
- Add a streaming mode
- Refacto of the flatten part
- Improve regex for street names
- Process compute_visitors with pyspark sql to compare with rdd
- Improve TUs because mutations_report.html is explicit
- Add TI

## Mutation testing
We talked about it so I added it, to launch :
1. cosmic-ray init setup-mutation-testing.toml mutations.sqlite -> init
2. cosmic-ray --verbosity=INFO baseline setup-mutation-testing.toml -> verify configuration
3. cosmic-ray exec setup-mutation-testing.toml mutations.sqlite -> execution

To see the progress :
1. open new terminal
2. cr-report mutations.sqlite --show-pending

To generate the report:
1. cr-html mutations.sqlite > mutations_report.html

## Advice
Use Pycharm Community Edition for import project and run tests ;-)