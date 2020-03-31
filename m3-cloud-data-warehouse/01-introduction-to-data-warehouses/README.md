# Introduction to Data Warehouses

## Operation vs Analytical Processes

Operational databases:
* Good for operations
* No redundancy, high integrity
* Slow for analytics (too many joins)
* Hard to understand

Data warehouse: system that enable us to support analytical processes
OLTP: online transactional processing
OLAP: online analytical processing

## Data Warehouse: Technical Perspective

DWH1: Copy of transaction data specifically structured for query and analysis

ETL: Extract, Transform, Load

Data In (Transaction data) -> ETL -> Dimensional Models -> Data Out (For Query and Analysis)

DWH Goals:
* Easy to understand
* Performant
* Quality assured
* Handles new questions well
* Secure

## Dimension Modeling

Star schema better for analysis (performance, easy to understand) than 3NF.

Numbers and metrics are good candidates for fact tables.
Physical attributes are good candidates for dimension tables.

## DWH architecture

* Kimball's Bus Architecture (Same database to all departments)
* Independent Data Marts (A different database to each department)
* Inmon's Corporate Information Factory (CIF)
    * 2 ETLs 
        * Source systemns -> 3NF DB
        * 3NF DB -> Departmental data marts
* Hybrid Kimball & Inmon CIF

## OLAP Cubes

Is an aggregation of a fact metric on a number of dimensions.

E.g.: Movie, Branch, Month

|Feb|NY|SF|Paris|
|---|---|---|---|
|Avatar|$25,000|$5,000|$15,000|
|Star Wars|$15,000|$7,000|$10,000|
|Batman|$5,000|$3,000|$10,000|

|Mar|NY|SF|Paris|
|---|---|---|---|
|Avatar|$5,000|$3,000|$10,000|
|Star Wars|$8,000|$6,000|$11,000|
|Batman|$20,000|$5,000|$11,000|


## OLAP Cubes: Roll-up and drill down

Examples
Roll-up: Sum up the sales of the cities by country
Drill down: Break the sales of the cities by districts

The OLAP cubes stores the fines grain of data, in case we need to drill down

## OLAP Cubes: Slice and dice

Slice: Reduce one dimension of the cube. E.g.: getting just one of the months.
Dice: Same number of dimensions, but restricting the values. E.g.: Month [FEB, MAR], Branch [NY], Movie [Avatar, Star wars]

## OLAP Cubes: Query optimization

Group by Cube (movie, branch, month) will aggregate all possible combinations of groupings (revenue by [[], [movie], [branch], [month], [movie, branch], [movie, month], [branch, month], [movie, branch, month]]) with one pass to the fact table.

## Data Warehouse Technologies

* MOLAP - Pre-aggregate OLAP cubes
* ROLAP - Compute OLAP cubes on the fly