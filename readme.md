##### Original Course link:
[(CS186)Intro to Database Systems - Fall 2013](https://sites.google.com/site/cs186fall2013/home)

##### About building
* Alter 'sourceversion' in build.xml to '1.9' to make Ant using JDK 9.
##### Progress
project 1:
[x] Tuple and TupleDesc   
[x] Catalog  
[x] BufferPool  
[x] HeapFile access method  
[x] Operators  

project 2:
[x] Filter and Join  
[x] Aggregates  
[x] HeapFile Mutability  
[x] Insertion and deletion  
[x] Page eviction  

##### Notes:
When catalog.txt and data.data is placed in project top level of folder. The command line can't read table file due to path issue.
Catalog.java used getParent to retrieve parent directory of catalog.txt if invoked by parser.
If parser gets a relative path without parent directory, the original code results in path that begins with 'null'.
A fix is to use no parent path if the return value is null. 
```
SimpleDB>  select d.f1, d.f2 from data d;
 select d.f1, d.f2 from data d;
Started a new transaction tid = 0
Added scan of table d
Added select list field d.f1
Added select list field d.f2
d.f1	d.f2	
------------------
1 10

2 20

3 30

4 40

5 50

5 50


 6 rows.
Transaction 0 committed.
----------------
10.03 seconds

SimpleDB> 
```

##### Contest(Optional)

* Added an in-memory hash join for equal predicate.
It is similar to GRACE join but doesn't write out buckets as both hash tables fits in memory at the same time.


```sql
SELECT p.title
FROM papers p
WHERE p.title LIKE 'selectivity';

```
```
Started a new transaction tid = 2
Added scan of table p
Added select list field p.title
p.title	
------------
Optimizing ethanol production selectivity.

Development of feedforward receptive field structure of a simple cell and its contribution to the orientation selectivity: a mod

Influences of formant bandwidth and auditory frequency selectivity on identification of place of articulation in stop consonants

A theoretical entropy score as a single value to express inhibitor selectivity.

ASH structure alignment package: Sensitivity and selectivity in domain classification.


 5 rows.
Transaction 2 committed.
----------------
0.26 seconds


```

```sql
SELECT p.title, v.name
FROM papers p, authors a, paperauths pa, venues v
WHERE a.name = 'E. F. Codd'
AND pa.authorid = a.id
AND pa.paperid = p.id
AND p.venueid = v.id;
```
```sql
Added table : authors with schema INT_TYPE(id),STRING_TYPE(name)
Added table : venues with schema INT_TYPE(id),STRING_TYPE(name),INT_TYPE(year),INT_TYPE(type)
Added table : papers with schema INT_TYPE(id),STRING_TYPE(title),INT_TYPE(venueid)
Added table : paperauths with schema INT_TYPE(paperid),INT_TYPE(authorid)
Computing table stats.
Done.
SimpleDB> SELECT p.title, v.name
FROM papers p, authors a, paperauths pa, venues v
WHERE a.name = 'E. F. Codd'
AND pa.authorid = a.id
AND pa.paperid = p.id
AND p.venueid = v.id;SELECT p.title, v.name
SimpleDB> FROM papers p, authors a, paperauths pa, venues v
SimpleDB> WHERE a.name = 'E. F. Codd'
SimpleDB> AND pa.authorid = a.id
SimpleDB> AND pa.paperid = p.id
SimpleDB> AND p.venueid = v.id;

Started a new transaction tid = 0
Added scan of table p
Added scan of table a
Added scan of table pa
Added scan of table v
Added join between pa.authorid and a.id
Added join between pa.paperid and p.id
Added join between p.venueid and v.id
Added select list field p.title
Added select list field v.name
p.title	v.name	
-----------------------
Further Normalization of the Data Base Relational Model. IBM Research Report  San Jose  California

Universal  Relation Fails to Replace Relational Model (letter to the editor). IEEE Software

Interactive Support for Non-Programmers: The Relational and Network Approaches. IBM Research Report  San Jose  California

RENDEZVOUS Version 1: An Experimental English Language Query Formulation System for Casual Users of Relational Data Bases. IBM Research Report

Data Base Sublanguage Founded on the Relational Calculus. IBM Research Report  San Jose  California

Relational Completeness of Data Base Sublanguages. In: R. Rustin (ed.): Database Systems: 65-98  Prentice Hall and IBM Research Report RJ 987  San Jose  California

Derivability  Redundancy and Consistency of Relations Stored in Large Data Banks. IBM Research Report  San Jose  California

The Capabilities of Relational Database Management Systems. IBM Research Report  San Jose  California

Seven Steps to Rendezvous with the Casual User. IFIP Working Conference Data Base Management

Normalized Data Base Structure: A Brief Tutorial. IBM Research Report  San Jose  California

The Gamma-0 n-ary Relational Data Base Interface Specifications of Objects and Operations. IBM Research Report


 11 rows.
Transaction 0 committed.
----------------
1.07 seconds

SimpleDB> 
```

```sql
SELECT a2.name, count(p.id)
FROM papers p, authors a1, authors a2, paperauths pa1, paperauths pa2
WHERE a1.name = 'Michael Stonebraker'
AND pa1.authorid = a1.id 
AND pa1.paperid = p.id 
AND pa2.authorid = a2.id 
AND pa1.paperid = pa2.paperid
GROUP BY a2.name
ORDER BY a2.name;
```
```sql
Added table : authors with schema INT_TYPE(id),STRING_TYPE(name)
Added table : venues with schema INT_TYPE(id),STRING_TYPE(name),INT_TYPE(year),INT_TYPE(type)
Added table : papers with schema INT_TYPE(id),STRING_TYPE(title),INT_TYPE(venueid)
Added table : paperauths with schema INT_TYPE(paperid),INT_TYPE(authorid)
Computing table stats.
Done.
SimpleDB> SELECT a2.name, count(p.id)
SimpleDB> FROM papers p, authors a1, authors a2, paperauths pa1, paperauths pa2
SimpleDB> WHERE a1.name = 'Michael Stonebraker'
SimpleDB> AND pa1.authorid = a1.id 
SimpleDB> AND pa1.paperid = p.id 
SimpleDB> AND pa2.authorid = a2.id 
SimpleDB> AND pa1.paperid = pa2.paperid
SimpleDB> GROUP BY a2.name
SimpleDB> ORDER BY a2.name;
Started a new transaction tid = 0
Added scan of table p
Added scan of table a1
Added scan of table a2
Added scan of table pa1
Added scan of table pa2
Added join between pa1.authorid and a1.id
Added join between pa1.paperid and p.id
Added join between pa2.authorid and a2.id
Added join between pa1.paperid and pa2.paperid
GROUP BY FIELD : a2.name
Added select list field a2.name
Aggregate field is p.id, agg fun is : count
Added select list field p.id
	 with aggregator count
a2.name	aggName(count)(p.id)	
-------------------------------------
Akhil Kumar 1

Dale Skeen 1

Eric N. Hanson 1

Lawrence A. Rowe 1

Michael Hirohama 1

Michael Stonebraker 8

Spyros Potamianos 1


 7 rows.
Transaction 0 committed.
----------------
7.65 seconds

SimpleDB> 

```