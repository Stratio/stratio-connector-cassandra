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


import com.datastax.driver.core.DataType;

import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.utils.IdentifierProperty;
import com.stratio.connector.cassandra.utils.ValueProperty;
import com.stratio.meta.common.metadata.structures.ColumnType;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.metadata.ColumnMetadata;
import com.stratio.meta2.common.metadata.IndexMetadata;
import com.stratio.meta2.common.metadata.IndexType;
import com.stratio.meta2.common.metadata.TableMetadata;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

import java.util.*;
import java.util.Map.Entry;

/**
 * Class that models a {@code CREATE INDEX} statement of the META language to CQL native. T
 */
public class CreateIndexStatement {




    private String keyspace = null;
    private boolean keyspaceIncluded =false;

    private IndexType type = null;

    /**
     * Whether the index should be created only if not exists.
     */
    private boolean createIfNotExists = false;



    /**
     * The name of the index.
     */
    private String name = null;

    /**
     * The name of the target table.
     */
    private String tableName = null;

    /**
     * The tableMetadata of the target table.
     */
    private TableMetadata tableMetadata = null;
    /**
     * The list of columns covered by the index. Only one column is allowed for {@code DEFAULT}
     * indexes.
     */
    private List<ColumnMetadata> targetColumns = null;

    /**
     * The name of the class that implements the secondary index.
     */
    private String usingClass = null;

    /**
     * The map of parameters passed to the index during its creation.
     */
    private Map<Selector, Selector> parameters = new LinkedHashMap<>();

    /**
     * The map of options passed to the index during its creation.
     */
    private Map<ValueProperty, ValueProperty> options = new LinkedHashMap<>();

    /**
     * Map of lucene types associated with Cassandra data types.
     */
    private static Map<String, String> luceneTypes = new HashMap<>();

    /**
     * Table metadata cached on the validate function.
     */
    private transient TableMetadata metadata = null;

    static {
        luceneTypes.put("SQL_VARCHAR", "{type:\"string\"}");
        luceneTypes.put(DataType.varchar().toString(), "{type:\"string\"}");
        luceneTypes.put(DataType.inet().toString(), "{type:\"string\"}");
        luceneTypes.put(DataType.ascii().toString(), "{type:\"string\"}");
        luceneTypes.put(DataType.bigint().toString(), "{type:\"long\"}");
        luceneTypes.put(DataType.counter().toString(), "{type:\"long\"}");
        luceneTypes.put("BOOLEAN", "{type:\"boolean\"}");
        luceneTypes.put("SQL_DOUBLE", "{type:\"double\"}");
        luceneTypes.put("SQL_FLOAT", "{type:\"float\"}");
        luceneTypes.put("SQL_INTEGER", "{type:\"integer\"}");
        luceneTypes.put(DataType.uuid().toString(), "{type:\"uuid\"}");
    }


    public CreateIndexStatement(IndexMetadata indexMetadata, boolean createIfNotExists, Session session) {
        targetColumns = new ArrayList<>();
        this.parameters = indexMetadata.getOptions();
        this.targetColumns=indexMetadata.getColumns();
        this.createIfNotExists=createIfNotExists;
        this.type=indexMetadata.getType();
        this.tableName=targetColumns.get(0).getName().getTableName().getName();
        this.keyspace=targetColumns.get(0).getName().getTableName().getCatalogName().getName();
        if (keyspace!=null)
            this.keyspaceIncluded=true;
        this.name=indexMetadata.getName().getName();

        if (type==IndexType.FULL_TEXT) {
            usingClass = "'com.stratio.cassandra.index.RowIndex'";
            options=generateLuceneOptions();
            //Create the new column for the Lucene Index
            try {
                session.execute(
                    "ALTER TABLE " + this.tableName + " ADD " + getIndexName() + " varchar");
            }catch (Exception e){

            }
        }
    }



    /**
     * Get the name of the index. If a LUCENE index is to be created, the name of the index is
     * prepended with {@code stratio_lucene_}. If a name for the index is not specified, the index
     * will be named using the concatenation of the target column names.
     *
     * @return The name of the index.
     */
    protected String getIndexName() {
        String result = null;
        if (name == null) {
            StringBuilder sb = new StringBuilder();
            if (IndexType.FULL_TEXT.equals(type)) {
                sb.append("stratio_lucene_");
                sb.append(tableName);
            } else {
                sb.append(tableName);
                for (ColumnMetadata c : targetColumns) {
                    sb.append("_");
                    sb.append(c.getName());
                }
                sb.append("_idx");
            }
            result = sb.toString();
        } else {
            result = name;
            if (IndexType.FULL_TEXT.equals(type)) {
                result = "stratio_lucene_" + name;
            }
        }
        return result;
    }


    public String toString() {
        StringBuilder sb = new StringBuilder("CREATE ");
        if (type==(IndexType.FULL_TEXT))
            sb.append("CUSTOM");
        sb.append(" INDEX ");
        if (createIfNotExists) {
            sb.append("IF NOT EXISTS ");
        }

        if (name != null) {
            sb.append(getIndexName()).append(" ");
        }
        sb.append("ON ");
        if (keyspaceIncluded) {
            sb.append(keyspace).append(".");
        }
        sb.append(tableName);
        sb.append(" (");
        int i=0;
        for(ColumnMetadata columnName:targetColumns){
            if (i!=0)
                sb.append(",");
            sb.append(columnName.getName().getName());
            i=1;
        }
        sb.append(")");

        if (usingClass != null) {
            sb.append(" USING ");
            sb.append(usingClass);
        }
        if (!options.isEmpty()) {
            sb.append(" WITH OPTIONS = {");
            Iterator<Entry<ValueProperty, ValueProperty>> entryIt = options.entrySet().iterator();
            Entry<ValueProperty, ValueProperty> e;
            while (entryIt.hasNext()) {
                e = entryIt.next();
                sb.append(e.getKey()).append(": ").append(e.getValue());
                if (entryIt.hasNext()) {
                    sb.append(", ");
                }
            }
            sb.append("}");
        }

        return sb.toString();
    }



    /**
     * Generate the set of Lucene options required to create an index.
     *
     * @return The set of options.
     */

    protected Map<ValueProperty, ValueProperty> generateLuceneOptions() {
        Map<ValueProperty, ValueProperty> result = new HashMap<>();

        // TODO: Read parameters from default configuration and merge with the user specification.
        result.put(new IdentifierProperty("'refresh_seconds'"), new IdentifierProperty("'1'"));
        result.put(new IdentifierProperty("'num_cached_filters'"), new IdentifierProperty("'1'"));
        result.put(new IdentifierProperty("'ram_buffer_mb'"), new IdentifierProperty("'32'"));
        result.put(new IdentifierProperty("'max_merge_mb'"), new IdentifierProperty("'5'"));
        result.put(new IdentifierProperty("'max_cached_mb'"), new IdentifierProperty("'30'"));
        result.put(new IdentifierProperty("'schema'"), new IdentifierProperty("'"
            + generateLuceneSchema() + "'"));

        return result;
    }

    /**
     * Generate the Lucene options schema that corresponds with the selected column.
     *
     * @return The JSON representation of the Lucene schema.
     */

    protected String generateLuceneSchema() {
        StringBuilder sb = new StringBuilder();
        sb.append("{default_analyzer:\"org.apache.lucene.analysis.standard.StandardAnalyzer\",");
        sb.append("fields:{");


        // Iterate throught the columns.
        for (ColumnMetadata column : targetColumns) {
            sb.append(column.getName().getName());
            sb.append(":");
            sb.append(luceneTypes.get(column.getColumnType().getStandardType()));
            sb.append(",");
        }

        sb.append("}}");
        return sb.toString().replace(",}}", "}}");
    }



}
