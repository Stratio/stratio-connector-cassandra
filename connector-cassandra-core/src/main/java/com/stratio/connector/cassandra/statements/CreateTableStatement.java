/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.cassandra.statements;



import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class that models a {@code CREATE TABLE} statement of the META language.
 */
public class CreateTableStatement {

    private static final int PRIMARY_SINGLE = 1;
    private static final int PRIMARY_COMPOSED = 2;
    private static final int PRIMARY_AND_CLUSTERING_SPECIFIED = 3;
    /**
     * The name of the target table.
     */
    private String tableName;
    /**
     * A map with the name of the columns in the table and the associated data type.
     */
    private Map<ColumnName, ColumnMetadata> tableColumns;
    /**
     * The list of columns that are part of the primary key.
     */
    private List<ColumnName> primaryKey;
    /**
     * The list of columns that are part of the clustering key.
     */
    private List<ColumnName> clusterKey;
    /**
     * The list of properties of the table
     */
    private String properties;
    /**
     * The type of primary key. Accepted values are:
     * <ul>
     * <li>1: If the primary key contains a single column.</li>
     * <li>2: If the primary key is composed of several columns but it does not contain a clustering
     * key.</li>
     * <li>3: If both the primary key and clustering key are specified.</li>
     * </ul>
     */
    private int primaryKeyType;
    /**
     * Whether the table should be created only if not exists.
     */
    private boolean ifNotExists;


    /**
     * Whether the table should be created with a set of properties.
     */
    private boolean withProperties = false;

    /**
     * Catalog
     */
    private String catalog;

    /**
     * Indicates if there is a catalog specified in the table name
     */
    private boolean catalogInc;



    /**
     * Class constructor.
     *
     * @param tableName      The name of the table.
     * @param tableColumns   A map with the name of the columns in the table and the associated data type.
     * @param primaryKey     The list of columns that are part of the primary key.
     * @param clusterKey     The list of columns that are part of the clustering key.
     * @param primaryKeyType The type of primary key.
     */
    public CreateTableStatement(String tableName, Map<ColumnName, ColumnMetadata> tableColumns,
        List<ColumnName> primaryKey, List<ColumnName> clusterKey, int primaryKeyType,
        String properties, boolean ifNotExists)
        throws ExecutionException {

        if (tableName.contains(".")) {
            String[] ksAndTablename = tableName.split("\\.");
            catalog = ksAndTablename[0];
            this.tableName = ksAndTablename[1];
            catalogInc = true;
        } else {
            this.tableName = tableName;
        }
        this.tableColumns = tableColumns;
        this.primaryKey = primaryKey;
        this.clusterKey = clusterKey;
        this.primaryKeyType = primaryKeyType;
        this.ifNotExists = ifNotExists;


        if (properties.length() > 0) {
            this.withProperties = true;
            this.properties = properties;
        }

        if (primaryKey == null || primaryKey.size() == 0) {
            throw new ExecutionException("PrimaryKey must exist");
        } else if (clusterKey == null && primaryKeyType == PRIMARY_AND_CLUSTERING_SPECIFIED) {
            throw new ExecutionException(
                "ClusterKey must exist with the type og primaryKey specified");
        }
    }

    private String getStringProperties(Map<Selector, Selector> properties) {
        StringBuilder str = new StringBuilder();

        int i = 0;
        for (Selector s : properties.values()) {
            if (i != 0) {
                str.append(" AND ");
            }
            StringSelector sselector = (StringSelector) s;
            str.append(sselector.getValue());
        }
        return str.toString();
    }


    public String getSinglePKString() {
        StringBuilder sb = new StringBuilder(" (");
        Set<ColumnName> keySet = tableColumns.keySet();
        int i = 0;
        for (ColumnName column : keySet) {
            if (i != 0) {
                sb.append(", ");
            }
            i = 1;
            String key = column.getName().substring(column.getName().lastIndexOf(".") + 1);
            String vp = tableColumns.get(column).getColumnType().toString();
            sb.append(key).append(" ").append(vp);

            if (key.equals(primaryKey.get(0).getName())) {
                sb.append(" PRIMARY KEY");
            }

        }
        sb.append(")");
        return sb.toString();
    }

    public String getCompositePKString() {
        StringBuilder sb = new StringBuilder("PRIMARY KEY");
        if (primaryKeyType == PRIMARY_COMPOSED) {
            sb.append("(");


            Iterator<ColumnName> pks = primaryKey.iterator();
            while (pks.hasNext()) {
                sb.append(pks.next().getName());
                if (pks.hasNext()) {
                    sb.append(", ");
                }
            }
            sb.append(")");
        }

        if (primaryKeyType == PRIMARY_AND_CLUSTERING_SPECIFIED) {
            sb.append("((");
            Iterator<ColumnName> pks = primaryKey.iterator();
            while (pks.hasNext()) {
                sb.append(pks.next().getName());
                if (pks.hasNext()) {
                    sb.append(", ");
                }
            }
            sb.append(")");

            for (ColumnName key : clusterKey) {
                sb.append(", ").append(key.getName());
            }
            sb.append(")");

        }
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Create table ");
        if (ifNotExists) {
            sb.append("IF NOT EXISTS ");
        }

        if (catalogInc) {
            sb.append(catalog).append(".");
        }
        sb.append(tableName);

        if (primaryKeyType == PRIMARY_SINGLE) {
            sb.append(getSinglePKString());
        } else {
            Set<ColumnName> keySet = tableColumns.keySet();
            sb.append(" (");
            for (ColumnName key : keySet) {
                String vp = tableColumns.get(key).getColumnType().toString();
                sb.append(key.getName()).append(" ").append(vp).append(", ");
            }
            sb.append(getCompositePKString()).append(")");
        }

        if (withProperties) {
            sb.append(" WITH ").append(properties);
        }
        return sb.toString();
    }



}
