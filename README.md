# ScyllaIOForApacheBeam
This is starting of Development of Scylla Io for apache beam using apache beam design considerations and scylla optimized java driver.

# Overall Features planned For Scylla IO
a) Read Feature
b) Write Feature
c) Delete Feature

# Current Cassandra Design
a) Algorithm which uses Number of Task slots 

# 

# Improved Design compared to CassandraIO
V1.0
a) Number oF task slots -> number of bounded sources -> For Each Bounded Source -> Multiple Queries equally distributed across each bounded source (Todo: when there is not an equal distribution algorthm)
b) Enable write feature   (Use exact same code for write feature from forked cassandra IO code)
c) Complete Unit Testing  (Use forked cassandra IO code)
d) Push to Maven Repo

V1.5
a) Prepared Statements
b) 

