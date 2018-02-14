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

```sql
SELECT p.title
FROM papers p
WHERE p.title LIKE 'selectivity';

```
```
Added table : authors with schema INT_TYPE(id),STRING_TYPE(name)
Added table : venues with schema INT_TYPE(id),STRING_TYPE(name),INT_TYPE(year),INT_TYPE(type)
Added table : papers with schema INT_TYPE(id),STRING_TYPE(title),INT_TYPE(venueid)
Added table : paperauths with schema INT_TYPE(paperid),INT_TYPE(authorid)
Computing table stats.
Done.
SimpleDB> SELECT p.title
SimpleDB> FROM papers p
SimpleDB> WHERE p.title LIKE 'selectivity';
Started a new transaction tid = 0
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
Transaction 0 committed.
----------------
0.51 seconds

SimpleDB> 

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

```