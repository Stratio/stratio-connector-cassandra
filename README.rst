About
******

Native connector for Cassandra using Crossdata.

Requirements
=============
Get `Cassandra Lucene Index plugin <https://github.com/Stratio/cassandra-lucene-index/tree/2.1.8.4>`_ version 2.1.8 and put the jar generated into cassandra lib directory

`Cassandra <http://cassandra.apache.org/download/>`_ version 2.1.8 must be installed and started.

`Crossdata <https://github.com/Stratio/crossdata>`_ version 0.4.1 is needed to interact with this
connector.

Compiling Stratio Connector Cassandra
======================================
Cassandra must be running in order to execute ITs. Otherwise, use -DskipITs
To automatically build execute the following command::


    > mvn clean compile install


Running the Stratio Connector Cassandra
========================================
::

    > mvn exec:java -pl cassandra-connector -Dexec.mainClass="com.stratio.connector.cassandra.CassandraConnector"



Build an executable Connector Cassandra
========================================
To generate the executable execute the following command::

    > cd cassandra-connector
    > mvn package -Ppackage


To run Connector Cassandra execute::

    > target/stratio-connector-cassandra-0.4.1/bin/stratio-connector-cassandra-0.4.1 start


To stop the connector execute::


    > target/stratio-connector-cassandra-0.4.1/bin/stratio-connector-cassandra-0.4.1 stop


Build a redistributable package
================================
It is possible too, to create a RPM or DEB redistributable package.

RPM and DEB Package::

    > cd cassandra-connector
    > mvn package -Ppackage


Once the package it's created, execute this commands to install:

RPM Package::

    > rpm -i cassandra-connector/target/cassandra-connector-0.4.1.noarch.rpm

DEB Package::

    > dpkg -i cassandra-connector/target/cassandra-connector-0.4.1.all.deb

Now to start/stop the connector::

    > service connector_cassandra start
    > service connector_cassandra stop

How to use Cassandra Connector
===============================
1. Start `crossdata-server and then crossdata-shell <https://github.com/Stratio/crossdata>`_.
2. Start Cassandra Connector as it is explained before
3. In crossdata-shell ...

Attach cluster on a data store. The data store name must be the same as the defined in the data store manifest ::

    xdsh:user>  ATTACH CLUSTER <cluster_name> ON DATASTORE <datastore_name> WITH OPTIONS {'Hosts': '[<ipHost_1, ipHost_2,...ipHost_n>]', 'Port': <cassandra_port>};


Attach the connector to the previously defined cluster. The connector name must match the one defined in the  Connector Manifest, and the cluster name must match with the previously defined in the ATTACH CLUSTER command ::

    xdsh:user>  ATTACH CONNECTOR <connector name> TO <cluster name> WITH OPTIONS {'DefaultLimit': '1000'};
    
    
At this point, we can start to send queries, that Crossdata execute with the connector specified  ::

    xdsh:user> CREATE CATALOG catalogTest;
    
    xdsh:user> USE catalogTest;
    
    xdsh:user> CREATE TABLE tableTest ON CLUSTER cassandra_prod (id int PRIMARY KEY, name text);
    
    xdsh:user> INSERT INTO tableTest(id, name) VALUES (1, 'stratio');
    
    xdsh:user> SELECT * FROM tableTest;


License
========
Stratio Crossdata is licensed as `Apache2 <http://www.apache.org/licenses/LICENSE-2.0.txt>`_

Licensed to STRATIO (C) under one or more contributor license agreements. See the NOTICE file distributed with this
work for additional information regarding copyright ownership.
The STRATIO (C) licenses this fileto you under the Apache License, Version 2.0 (the"License"); you may not use this
file except in compliancewith the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the
specific language governing permissions and limitations under the License.
