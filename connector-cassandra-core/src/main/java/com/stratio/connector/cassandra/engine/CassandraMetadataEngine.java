/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.cassandra.engine;


import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.CassandraExecutor;
import com.stratio.connector.cassandra.statements.*;
import com.stratio.connector.cassandra.utils.ValueProperty;
import com.stratio.meta.common.connector.IMetadataEngine;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta2.common.data.CatalogName;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.*;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jjlopez on 26/08/14.
 */
public class CassandraMetadataEngine implements IMetadataEngine {

    private Map<String, Session> sessions;
    private Session session = null;
    private static final int PRIMARY_SINGLE = 1;
    private static final int PRIMARY_COMPOSED = 2;
    private static final int PRIMARY_AND_CLUSTERING_SPECIFIED = 3;

    public CassandraMetadataEngine(Map<String, Session> sessions) {
        this.sessions = sessions;
    }

    @Override
    public void createCatalog(ClusterName targetCluster, CatalogMetadata catalogMetadata)
        throws UnsupportedException, ExecutionException {
        session = sessions.get(targetCluster.getName());
        String catalogName = catalogMetadata.getName().getQualifiedName();
        Map<Selector, Selector> catalogOptions = catalogMetadata.getOptions();

        StringBuilder createCatalogOptions=new StringBuilder();
        if (catalogOptions.size()>0) {
            int i=0;
            for (Selector options : catalogOptions.values()) {
                if (i!=0)
                    createCatalogOptions.append(",");

                StringSelector str=(StringSelector)options;
                createCatalogOptions.append(str.getValue());
            }
        }

        CreateCatalogStatement catalogStatement =
            new CreateCatalogStatement(catalogName, true, catalogOptions.size()==0?null:createCatalogOptions.toString());
        CassandraExecutor.execute(catalogStatement.toString(), session);

    }

    @Override
    public void createTable(ClusterName targetCluster, TableMetadata tableMetadata)
        throws UnsupportedException, ExecutionException {
        session = sessions.get(targetCluster.getName());
        String tableName = tableMetadata.getName().getQualifiedName();
        Map<Selector, Selector> tableOptions = tableMetadata.getOptions();
        List<ColumnName> primaryKey = tableMetadata.getPrimaryKey();
        List<ColumnName> clusterKey = tableMetadata.getClusterKey();

        int primaryKeyType;
        if (primaryKey.size()<=1){
            primaryKeyType=PRIMARY_SINGLE;
        }else{
            if(clusterKey.size()>0) {
                primaryKeyType = PRIMARY_AND_CLUSTERING_SPECIFIED;
            }else {
                primaryKeyType = PRIMARY_COMPOSED;
            }
        }

        Map<ColumnName, com.stratio.meta2.common.metadata.ColumnMetadata> tableColumns =
            tableMetadata.getColumns();

        CreateTableStatement tableStatement =
            new CreateTableStatement(tableName, tableColumns, primaryKey, clusterKey, primaryKeyType, true);
        CassandraExecutor.execute(tableStatement.toString(), session);

    }

    @Override
    public void dropCatalog(ClusterName targetCluster, CatalogName name)
        throws UnsupportedException, ExecutionException {
        session = sessions.get(targetCluster.getName());
        DropCatalogStatement catalogStatement = new DropCatalogStatement(name.getName(), true);
        CassandraExecutor.execute(catalogStatement.toString(), session);
    }

    @Override
    public void dropTable(ClusterName targetCluster, TableName name)
        throws UnsupportedException, ExecutionException {
        session = sessions.get(targetCluster.getName());
        DropTableStatement tableStatement = new DropTableStatement(name.getQualifiedName(), true);
        CassandraExecutor.execute(tableStatement.toString(), session);

    }

    @Override
    public void createIndex(ClusterName targetCluster, IndexMetadata indexMetadata) throws UnsupportedException, ExecutionException{
        session = sessions.get(targetCluster.getName());
        CreateIndexStatement indexStatement = new CreateIndexStatement(indexMetadata, true, session );
        CassandraExecutor.execute(indexStatement.toString(), session);
    }

    @Override
    public void dropIndex(ClusterName targetCluster, IndexMetadata indexName) throws UnsupportedException, ExecutionException{
        session = sessions.get(targetCluster.getName());
        DropIndexStatement indexStatement=new DropIndexStatement(indexName, true);
        CassandraExecutor.execute(indexStatement.toString(), session);

    }
}
