# postgresql-tests
This repository contains code that performs stress testing on a PostgreSQL database. 
The main scenario:
* The main thread generates random values ​​for a given number of data points.
* The number of values to generate ​​and the number of datapoints can be passed as parameters to main.cs.
* Then the main thread groups the values ​​in chunks and writes them to the database in separate threads.
* Each chunk contains values ​​for writing to one database table.
* The size of the chunk can also be set up.
* Each database table has the following columns - timestamp(for date and time) , p_01(for datapoint value), s_01(for datapoint status).
* The UNNEST plsql function is used to make fast bulk inserts.
![](https://github.com/treshnikov/postgresql-tests/blob/master/img/console.png)
