# About #

Native connector for Cassandra using Crossdata.

## Requirements ##

[Stratio Cassandra](https://github.com/Stratio/stratio-cassandra) must be installed and started.  
[Crossdata] (https://github.com/Stratio/crossdata) is needed to interact with this connector.

## Compiling Stratio Connector Cassandra ##

To automatically build execute the following command:


    > mvn clean compile install


## Running the Stratio Connector Cassandra ##


    > mvn exec:java -Dexec.mainClass="com.stratio.connector.cassandra.CassandraConnector"



## Build an executable Connector Cassandra ##

To generate the executable execute the following command:


    > mvn crossdata-connector:install


To run Connector Cassandra execute:


    > target/stratio-connector-cassandra-0.1.0-SNAPSHOT/bin/stratio-connector-cassandra-0.1.0-SNAPSHOT start


To stop the connector execute:


    > target/stratio-connector-cassandra-0.1.0-SNAPSHOT/bin/stratio-connector-cassandra-0.1.0-SNAPSHOT stop


## How to use Cassandra Connector ##

 1. Start [crossdata-server and then crossdata-shell](https://github.com/Stratio/crossdata).  
 2. https://github.com/Stratio/crossdata
 3. Start Cassandra Connector as it is explained before
 4. In crossdata-shell:
    
    Add a data store. We need to specified the XML manifest that defines the data store. The XML manifest can be found
    in the path of the Cassandra Connector in target/stratio-connector-cassandra-0.1.0/conf/CassandraDataStore.xml
    
    
        xdsh:user>  ADD DATASTORE <Absolute path to Cassandra Datastore manifest>;
    
    
    Attach cluster on that data store. The data store name must be the same as the defined in the data store manifest.
    
    
        xdsh:user>  ATTACH CLUSTER <cluster_name> ON DATASTORE <datastore_name> WITH OPTIONS {'Hosts': '[<ipHost_1, ipHost_2,...ipHost_n>]', 'Port': <cassandra_port>};
    
    
    Add the connector manifest. The XML with the manifest can be found in the path of the Cassandra Connector in
    target/stratio-connector-cassandra-0.1.0/conf/CassandraConnector.xml
    
    
        xdsh:user>  ADD CONNECTOR <Path to Cassandra Connector Manifest>
    
    
    Attach the connector to the previously defined cluster. The connector name must match the one defined in the
    Connector Manifest, and the cluster name must match with the previously defined in the ATTACH CLUSTER command.
    
    
        xdsh:user>  ATTACH CONNECTOR <connector name> TO <cluster name> WITH OPTIONS {'DefaultLimit': '1000'};
    
    
    At this point, we can start to send queries, that Crossdata execute with the connector specified.    
    
        xdsh:user> CREATE CATALOG catalogTest;
    
        xdsh:user> USE catalogTest;
    
        xdsh:user> CREATE TABLE tableTest ON CLUSTER cassandra_prod (id int PRIMARY KEY, name text);
    
        xdsh:user> INSERT INTO tableTest(id, name) VALUES (1, 'stratio');
    
        xdsh:user> SELECT * FROM tableTest;


# License #

Stratio Crossdata is licensed as [Apache2](http://www.apache.org/licenses/LICENSE-2.0.txt)

Licensed to STRATIO (C) under one or more contributor license agreements.  
See the NOTICE file distributed with this work for additional information 
regarding copyright ownership.  The STRATIO (C) licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
