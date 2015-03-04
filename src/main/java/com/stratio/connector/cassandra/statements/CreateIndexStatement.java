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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.stratio.connector.cassandra.utils.IdentifierProperty;
import com.stratio.connector.cassandra.utils.Utils;
import com.stratio.connector.cassandra.utils.ValueProperty;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.IndexMetadata;
import com.stratio.crossdata.common.metadata.IndexType;

/**
 * Class that models a {@code CREATE INDEX} statement of the META language to CQL native. T
 */
public class CreateIndexStatement {

    /**
     * Map of lucene types associated with Cassandra data types.
     */
    private static Map<com.stratio.crossdata.common.metadata.DataType, String> luceneTypes = new HashMap<>();

    static {
        luceneTypes.put(com.stratio.crossdata.common.metadata.DataType.VARCHAR, "{type:\"string\"}");
        luceneTypes.put(com.stratio.crossdata.common.metadata.DataType.TEXT, "{type:\"string\"}");
        luceneTypes.put(com.stratio.crossdata.common.metadata.DataType.BOOLEAN, "{type:\"boolean\"}");
        luceneTypes.put(com.stratio.crossdata.common.metadata.DataType.DOUBLE, "{type:\"double\"}");
        luceneTypes.put(com.stratio.crossdata.common.metadata.DataType.BIGINT, "{type:\"long\"}");
        luceneTypes.put(com.stratio.crossdata.common.metadata.DataType.FLOAT, "{type:\"float\"}");
        luceneTypes.put(com.stratio.crossdata.common.metadata.DataType.INT, "{type:\"integer\"}");
        //luceneTypes.put(com.stratio.crossdata.common.metadata.DataType.UUID, "{type:\"uuid\"}");
    }

    private String columnForIndex;

    private String keyspace = null;
    private boolean keyspaceIncluded = false;
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
     * The list of columns covered by the index. Only one column is allowed for {@code DEFAULT}
     * indexes.
     */
    private Map<ColumnName, ColumnMetadata> targetColumns = null;
    /**
     * The name of the class that implements the secondary index.
     */
    private String usingClass = null;
    /**
     * The map of options passed to the index during its creation.
     */
    private Map<ValueProperty, ValueProperty> options = new LinkedHashMap<>();

    /**
     * Basic Constructor.
     *
     * @param indexMetadata     Index metadata information .
     * @param createIfNotExists Condition of creation of the index.
     * @param session           Session that the Index affect.
     * @throws ExecutionException
     */
    public CreateIndexStatement(IndexMetadata indexMetadata, boolean createIfNotExists,
            Session session)
            throws ExecutionException {
        this.targetColumns = indexMetadata.getColumns();
        this.createIfNotExists = createIfNotExists;
        this.type = indexMetadata.getType();
        this.tableName = indexMetadata.getName().getTableName().getName();
        this.keyspace = indexMetadata.getName().getTableName().getCatalogName().getName();
        if (keyspace != null) {
            this.keyspaceIncluded = true;
        }
        this.name = indexMetadata.getName().getName();

        if (type == IndexType.FULL_TEXT) {
            usingClass = "'com.stratio.cassandra.index.RowIndex'";

            //Create the new column for the Lucene Index
            try {
                columnForIndex = indexMetadata.getName().getName();
                String catalog= Utils.toCaseSensitive(indexMetadata.getName().getTableName().getCatalogName()
                        .getName());
                String table=Utils.toCaseSensitive(indexMetadata.getName().getTableName().getName());
                session.execute(
                        "ALTER TABLE " + catalog + '.' + table  +  " ADD "
                                + Utils.toCaseSensitive(columnForIndex) + " varchar;");
            } catch (Exception e) {
                throw new ExecutionException(
                        "Cannot generate a new Column to insert the Lucene Index. " + e.getMessage(),
                        e);
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
    private String getIndexName() {
        String result = null;
        if (name == null) {
            StringBuilder sb = new StringBuilder();
            if (IndexType.FULL_TEXT.equals(type)) {
                sb.append("stratio_fulltext");
                for (ColumnMetadata columnMetadata : targetColumns.values()) {
                    sb.append("_");
                    sb.append(columnMetadata.getName().getName());
                }
                sb.append(tableName);
            } else {
                sb.append(tableName);

                for (Map.Entry<ColumnName, ColumnMetadata> entry : targetColumns.entrySet()) {
                    sb.append("_");
                    sb.append(entry.getValue());
                }

                sb.append("_idx");
            }
            result = sb.toString();
        } else {
            result = name;
            if (IndexType.FULL_TEXT.equals(type)) {
                result = keyspace + "_" + tableName + "_" + name;
            }
        }
        return result;
    }

    /**
     * Get the query in a String in CQL language.
     *
     * @return String with the query
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("CREATE ");
        if (type == (IndexType.FULL_TEXT)) {
            options = generateLuceneOptions();
            sb.append("CUSTOM");
        }
        sb.append(" INDEX ");
        if (createIfNotExists) {
            sb.append("IF NOT EXISTS ");
        }

        if (name != null) {
            sb.append((getIndexName())).append(" ");
        }
        sb.append("ON ");
        if (keyspaceIncluded) {
            sb.append(Utils.toCaseSensitive(keyspace)).append(".");
        }
        sb.append(Utils.toCaseSensitive(tableName));
        sb.append(" (");
        if (type!=IndexType.FULL_TEXT) {
            int i = 0;
            for (Map.Entry<ColumnName, ColumnMetadata> entry : targetColumns.entrySet()) {
                if (i != 0) {
                    sb.append(",");
                }
                sb.append(Utils.toCaseSensitive(entry.getValue().getName().getName()));
                i = 1;
            }
        } else {
            sb.append(Utils.toCaseSensitive(columnForIndex));
        }
        sb.append(")");

        if (usingClass != null) {
            sb.append(" USING ");
            sb.append(usingClass);
        }
        if (!options.isEmpty()) {
            sb.append(getOptionsString());
        }

        return sb.toString();
    }

    private String getOptionsString() {
        StringBuilder sb = new StringBuilder();

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

        return sb.toString();

    }

    /**
     * Generate the set of Lucene options required to create an index.
     *
     * @return The set of options.
     */

    private Map<ValueProperty, ValueProperty> generateLuceneOptions() {
        Map<ValueProperty, ValueProperty> result = new HashMap<>();

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
        for (Map.Entry<ColumnName, ColumnMetadata> entry : targetColumns.entrySet()) {
            sb.append(Utils.toCaseSensitive(entry.getValue().getName().getName()));
            sb.append(":");
            if (entry.getValue().getColumnType().getDataType()==com.stratio.crossdata.common.metadata.DataType.NATIVE){
                if (entry.getValue().getColumnType().getDbType().equals("timestamp")){
                    sb.append("{type:\"date\", pattern:\"yyyy-MM-dd HH:mm:ss\"}");
                }else {
                    sb.append("{type:\"").append(entry.getValue().getColumnType().getDbType()).append("\"}");
                }
            }else {
                sb.append(luceneTypes.get(entry.getValue().getColumnType().getDataType()));
            }
            sb.append(",");
        }

        sb.append("}}");
        return sb.toString().replace(",}}", "}}");
    }

}
