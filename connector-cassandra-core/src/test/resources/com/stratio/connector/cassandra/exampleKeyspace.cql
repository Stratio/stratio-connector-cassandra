CREATE KEYSPACE example WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' };

USE example;

create TABLE example.users (id int, name text, age int, bool boolean, PRIMARY KEY (id, name)) With comment='Users table';
create TABLE example.information (id int, phrase text, email text, score double , PRIMARY KEY (id)) With comment='Information table';
#Create lucene index
#CREATE CUSTOM INDEX stratio_lucene_index_1 ON demo.users (stratio_lucene_index_1) USING 'com.stratio.cassandra.index.RowIndex' WITH OPTIONS = {'refresh_seconds':'1', 'schema':'{default_analyzer:"org.apache.lucene.analysis.standard.StandardAnalyzer",fields:{name:{type:"string"}, gender:{type:"string"}, email:{type:"string"}, age:{type:"integer"}, bool:{type:"boolean"}, phrase:{type:"text", analyzer:"org.apache.lucene.analysis.en.EnglishAnalyzer"}}}'};

#Create Cassandra indexes
create INDEX on users (name);
create INDEX on information (phrase);

#Insert test data without phrase
INSERT INTO example.users (id,name,age,bool) VALUES (1, 'user1', 41, true);
INSERT INTO example.users (id,name,age,bool) VALUES (2, 'user2', 42, true);
INSERT INTO example.users (id,name,age,bool) VALUES (3, 'user3', 43, true);
INSERT INTO example.users (id,name,age,bool) VALUES (4, 'user4', 44, true);


