CREATE KEYSPACE meetups WITH REPLICATION = {
    'class' : 'SimpleStrategy', 'replication_factor' : 1
    };
USE meetups;