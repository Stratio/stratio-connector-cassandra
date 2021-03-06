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

package com.stratio.connector.cassandra.utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Selector;

/**
 * Utils Class that implements a utility helper for Cassandra Connector.
 */
public class Utils {

    /**
     * Class logger.
     */
    private static final Logger LOG = Logger.getLogger(Utils.class);
    /**
     * Map of methods required to transform a {@link com.datastax.driver.core.DataType} into the
     * corresponding object.
     */
    private static Map<String, Method> transformations = new HashMap<>();

    static {
        try {
            transformations
                    .put(DataType.ascii().toString(), Row.class.getMethod("getString", String.class));
            transformations
                    .put(DataType.text().toString(), Row.class.getMethod("getString", String.class));
            transformations
                    .put(DataType.varchar().toString(), Row.class.getMethod("getString", String.class));
            transformations
                    .put(DataType.bigint().toString(), Row.class.getMethod("getLong", String.class));
            transformations
                    .put(DataType.counter().toString(), Row.class.getMethod("getLong", String.class));
            transformations
                    .put(DataType.cboolean().toString(), Row.class.getMethod("getBool", String.class));
            transformations
                    .put(DataType.blob().toString(), Row.class.getMethod("getBytes", String.class));
            transformations
                    .put(DataType.decimal().toString(),
                            Row.class.getMethod("getDecimal", String.class));
            transformations
                    .put(DataType.cdouble().toString(), Row.class.getMethod("getDouble", String.class));
            transformations
                    .put(DataType.cfloat().toString(), Row.class.getMethod("getFloat", String.class));
            transformations
                    .put(DataType.inet().toString(), Row.class.getMethod("getInet", String.class));
            transformations
                    .put(DataType.cint().toString(), Row.class.getMethod("getInt", String.class));
            transformations
                    .put(DataType.timestamp().toString(), Row.class.getMethod("getDate", String.class));
            transformations
                    .put(DataType.uuid().toString(), Row.class.getMethod("getUUID", String.class));
            transformations
                    .put(DataType.timeuuid().toString(), Row.class.getMethod("getUUID", String.class));
            transformations
                    .put(DataType.varint().toString(), Row.class.getMethod("getVarint", String.class));

        } catch (NoSuchMethodException e) {
            LOG.error("Cannot create transformation map", e);
        }
    }

    /**
     * Get a {@link com.stratio.crossdata.common.data.Cell} with the column contents of a Row.
     *
     * @param type       The {@link com.datastax.driver.core.DataType} of the column.
     * @param r          The row that contains the column.
     * @param columnName The column name.
     * @return A {@link com.stratio.crossdata.common.data.Cell} with the contents.
     * @throws java.lang.reflect.InvocationTargetException If the required method cannot be invoked.
     * @throws IllegalAccessException                      If the method cannot be accessed.
     */
    protected Cell getCell(DataType type, Row r, String columnName)
            throws InvocationTargetException, IllegalAccessException {
        Method m = transformations.get(type.toString());
        Object value = m.invoke(r, columnName);
        return new Cell(value);
    }

    /**
     * Transforms a Cassandra {@link com.datastax.driver.core.ResultSet} into a {@link
     * com.stratio.crossdata.common.data.ResultSet}.
     *
     * @param resultSet The input Cassandra result set.
     * @param alias     The map with the relations between ColumnName and Alias.
     * @return An equivalent Meta ResultSet.
     */
    public com.stratio.crossdata.common.data.ResultSet transformToMetaResultSet(
            com.datastax.driver.core.ResultSet resultSet, Map<Selector, String> alias) {
        ResultSet crs = new ResultSet();

        CassandraMetadataHelper helper = new CassandraMetadataHelper();

        //Get the columns in order
        List<ColumnDefinitions.Definition> definitions = resultSet.getColumnDefinitions().asList();
        List<ColumnMetadata> columnList =
                new ArrayList<>();
        ColumnMetadata columnMetadata = null;
        //Obtain the metadata associated with the columns.
        for (ColumnDefinitions.Definition def : definitions) {
            //Insert the alias if exists
            ColumnName columnName = new ColumnName(def.getKeyspace(), def.getTable(), def.getName());
            ColumnType type;
            if (def.getType().getName().toString().equalsIgnoreCase("list")){
                String innerType=def.getType().getTypeArguments().get(0).getName().toString();
                type = helper.toColumnType(def.getType().getName().toString(), innerType);
            }else{
                type = helper.toColumnType(def.getType().getName().toString());
            }

            ColumnName cassandraColumnName = new ColumnName(def.getKeyspace(), def.getTable(), def.getName());
            for (Map.Entry<Selector, String> entry : alias.entrySet()) {
                if (entry.getKey().getColumnName().getQualifiedName().equals(cassandraColumnName.getQualifiedName())) {
                    columnMetadata = new ColumnMetadata(columnName, null, type);
                    columnMetadata.getName().setAlias(entry.getValue());
                    break;
                } else {
                    columnMetadata = new ColumnMetadata(columnName, null, type);
                }
            }
            columnList.add(columnMetadata);
        }
        crs.setColumnMetadata(columnList);

        try {
            for (Row row : resultSet.all()) {
                com.stratio.crossdata.common.data.Row metaRow = new com.stratio.crossdata.common.data.Row();
                for (ColumnDefinitions.Definition def : definitions) {
                    if (def.getName().toLowerCase().startsWith("stratio")) {
                        continue;
                    }
                    Cell metaCell;
                    if (def.getType().getName().toString().equalsIgnoreCase("list")){
                        metaCell=new Cell(row.getList(def.getName(),def.getType().getTypeArguments().get(0)
                                .asJavaClass())
                                .toString());
                    }else{
                        metaCell = getCell(def.getType(), row, def.getName());
                    }
                    ColumnName cassandraColumnName = new ColumnName(def.getKeyspace(), def.getTable(), def.getName());
                    boolean findIt = false;
                    for (Map.Entry<Selector, String> entry : alias.entrySet()) {
                        if (entry.getKey().getColumnName().getQualifiedName()
                                .equals(cassandraColumnName.getQualifiedName())) {
                            metaRow.addCell(entry.getValue(), metaCell);
                            findIt = true;
                            break;
                        }
                    }
                    if (!findIt) {
                        metaRow.addCell(def.getName(), metaCell);
                    }
                }
                crs.add(metaRow);
            }
        } catch (InvocationTargetException | IllegalAccessException e) {
            LOG.error("Cannot transform result set", e);
            crs = new ResultSet();
        }
        return crs;
    }

    /**
     * Transforms a Cassandra {@link com.datastax.driver.core.ResultSet} into a {@link
     * com.stratio.crossdata.common.data.ResultSet}.
     *
     * @param alias The map with the relations between ColumnName and Alias.
     * @return An equivalent Meta ResultSet.
     */
    public com.stratio.crossdata.common.data.ResultSet transformPagingToMetaResultSet(
            List<ColumnDefinitions.Definition> definitions, List<Row> rows, Map<Selector, String> alias) {
        ResultSet crs = new ResultSet();

        CassandraMetadataHelper helper = new CassandraMetadataHelper();

        List<ColumnMetadata> columnList = new ArrayList<>();
        ColumnMetadata columnMetadata = null;
        //Obtain the metadata associated with the columns.
        for (ColumnDefinitions.Definition def : definitions) {
            //Insert the alias if exists

            ColumnName columnName = new ColumnName(def.getKeyspace(), def.getTable(), def.getName());
            ColumnType type = helper.toColumnType(def.getType().getName().toString());
            ColumnName cassandraColumnName = new ColumnName(def.getKeyspace(), def.getTable(), def.getName());
            for (Map.Entry<Selector, String> entry : alias.entrySet()) {
                if (entry.getKey().getColumnName().getQualifiedName().equals(cassandraColumnName.getQualifiedName())) {
                    columnMetadata = new ColumnMetadata(columnName, null, type);
                    columnMetadata.getName().setAlias(entry.getValue());
                    break;
                } else {
                    columnMetadata = new ColumnMetadata(columnName, null, type);
                }
            }
            columnList.add(columnMetadata);
        }
        crs.setColumnMetadata(columnList);

        try {
            for (Row row : rows) {
                com.stratio.crossdata.common.data.Row metaRow = new com.stratio.crossdata.common.data.Row();
                for (ColumnDefinitions.Definition def : definitions) {
                    if (def.getName().toLowerCase().startsWith("stratio")) {
                        continue;
                    }
                    Cell metaCell = getCell(def.getType(), row, def.getName());
                    ColumnName cassandraColumnName = new ColumnName(def.getKeyspace(), def.getTable(), def.getName());
                    boolean findIt = false;
                    for (Map.Entry<Selector, String> entry : alias.entrySet()) {
                        if (entry.getKey().getColumnName().getQualifiedName()
                                .equals(cassandraColumnName.getQualifiedName())) {
                            metaRow.addCell(entry.getValue(), metaCell);
                            findIt = true;
                            break;
                        }
                    }
                    if (!findIt) {
                        metaRow.addCell(def.getName(), metaCell);
                    }
                }
                crs.add(metaRow);
            }
        } catch (InvocationTargetException | IllegalAccessException e) {
            LOG.error("Cannot transform result set", e);
            crs = new ResultSet();
        }
        return crs;
    }

    /**
     * Get the Crossdata ColumnType from Cassandra DataType
     *
     * @param type The {@link com.datastax.driver.core.DataType} of the column.
     * @return A {@link com.stratio.crossdata.common.metadata.ColumnType}
     */
    public ColumnType getCrossdataColumn(DataType type) {
        ColumnType columnType;
        switch (type.getName()) {
        case ASCII:
        case BLOB:
        case COUNTER:
        case INET:
        case TIMESTAMP:
        case UUID:
        case TIMEUUID:
        case UDT:
        case TUPLE:
        case CUSTOM:
            columnType = new ColumnType(com.stratio.crossdata.common.metadata.DataType.NATIVE);
            break;
        case BIGINT:
            columnType = new ColumnType(com.stratio.crossdata.common.metadata.DataType.BIGINT);
            break;
        case BOOLEAN:
            columnType = new ColumnType(com.stratio.crossdata.common.metadata.DataType.BOOLEAN);
            break;
        case DECIMAL:
        case FLOAT:
            columnType = new ColumnType(com.stratio.crossdata.common.metadata.DataType.FLOAT);
            break;
        case DOUBLE:
            columnType = new ColumnType(com.stratio.crossdata.common.metadata.DataType.DOUBLE);
            break;
        case INT:
        case VARINT:
            columnType = new ColumnType(com.stratio.crossdata.common.metadata.DataType.INT);
            break;
        case TEXT:
        case VARCHAR:
            columnType = new ColumnType(com.stratio.crossdata.common.metadata.DataType.TEXT);
            break;
        case LIST:
            columnType = new ColumnType(com.stratio.crossdata.common.metadata.DataType.LIST);
            break;
        case SET:
            columnType = new ColumnType(com.stratio.crossdata.common.metadata.DataType.SET);
            break;
        case MAP:
            columnType = new ColumnType(com.stratio.crossdata.common.metadata.DataType.MAP);
            break;
        default:
            columnType = new ColumnType(com.stratio.crossdata.common.metadata.DataType.TEXT);
        }
        columnType.setDbType(type.getName().toString());
        return columnType;
    }

    public static String toCaseSensitive(String s) {
        return "\"" + s + "\"";
    }

    public static String getFormatType(ColumnSelector left, Selector right, Session session) {

        switch (session.getCluster().getMetadata().getKeyspace(Utils.toCaseSensitive(left.getColumnName().getTableName()
                .getCatalogName().getName())).getTable(
                Utils.toCaseSensitive(left.getColumnName().getTableName().getName())).getColumn
                (Utils.toCaseSensitive(left.getColumnName().getName())).getType().getName()) {

        case ASCII:
        case BIGINT:
        case BLOB:
        case BOOLEAN:
        case COUNTER:
        case DECIMAL:
        case DOUBLE:
        case FLOAT:
        case INET:
        case INT:
        case TEXT:
        case TIMESTAMP:
        case UUID:
        case VARCHAR:
        case VARINT:
        case LIST:
        case SET:
        case MAP:
        case UDT:
        case TUPLE:
        case CUSTOM:
            return right.toString();
        case TIMEUUID:
            return right.getStringValue();
        }
        return null;
    }

    public static com.stratio.crossdata.common.metadata.DataType getDataTypeFromString(String fieldType) {
        switch (fieldType.toUpperCase()) {
        case "ASCII":
            return com.stratio.crossdata.common.metadata.DataType.TEXT;
        case "BIGINT":
            return com.stratio.crossdata.common.metadata.DataType.BIGINT;
        case "BOOLEAN":
            return com.stratio.crossdata.common.metadata.DataType.BOOLEAN;
        case "DOUBLE":
            return com.stratio.crossdata.common.metadata.DataType.DOUBLE;
        case "FLOAT":
            return com.stratio.crossdata.common.metadata.DataType.FLOAT;
        case "INT":
            return com.stratio.crossdata.common.metadata.DataType.INT;
        case "TEXT":
        case "STRING":
            return com.stratio.crossdata.common.metadata.DataType.TEXT;
        case "VARCHAR":
            return com.stratio.crossdata.common.metadata.DataType.VARCHAR;
        case "VARINT":
            return com.stratio.crossdata.common.metadata.DataType.INT;
        default:
            return com.stratio.crossdata.common.metadata.DataType.NATIVE;
        }

    }
}
