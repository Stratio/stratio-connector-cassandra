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

import java.util.Map;

import com.stratio.connector.cassandra.utils.Utils;
import com.stratio.crossdata.common.data.AlterOperation;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.utils.StringUtils;


/**
 * Class that models an {@code ALTER TABLE} statement from the META language.
 */
public class AlterTableStatement {

    /**
     * The target table.
     */
    private TableName tableName;

    /**
     * Type of alter.
     */
    private AlterOperation option;

    /**
     * Target column name.
     */
    private ColumnName column;

    /**
     * Target column datatype used with {@code ALTER} or {@code ADD}.
     */
    private ColumnType type;

    /**
     * The list of {@link com.stratio.crossdata.core.structures.Property} of the table.
     */
    private Map<Selector, Selector> properties = null;

    /**
     * Class constructor.
     *
     * @param tableName  The name of the table.
     * @param column     The name of the column.
     * @param type       The data type of the column.
     * @param properties The type of modification.
     * @param option     The map of options.
     */
    public AlterTableStatement(TableName tableName, ColumnName column, ColumnType type,
            String properties, AlterOperation option) {

        this.tableName = tableName;
        this.column = column;
        this.type = type;
        this.properties = StringUtils.convertJsonToOptions(tableName,properties);
        this.option = option;
    }

    /**
     *
     * @param tableName  The name of the table.
     * @param column     The name of the column.
     * @param type       The data type of the column.
     * @param properties The type of modification.
     * @param option     The map of options.
     */
    public AlterTableStatement(TableName tableName, ColumnName column, ColumnType type,
            Map<Selector, Selector> properties, AlterOperation option) {

        this.tableName = tableName;
        this.column = column;
        this.properties = properties;
        this.option = option;
        this.type=type;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ALTER TABLE ");
        sb.append(Utils.toCaseSensitive(tableName.getCatalogName().getName())).append('.').append(Utils.toCaseSensitive
                (tableName.getName()));
        switch (option) {
        case ALTER_COLUMN:
            sb.append(" ALTER ").append(Utils.toCaseSensitive(column.getName()));
            sb.append(" TYPE ").append(type);
            break;
        case ADD_COLUMN:
            sb.append(" ADD ");
            sb.append(Utils.toCaseSensitive(column.getName())).append(" ");
            sb.append(type);
            break;
        case DROP_COLUMN:
            sb.append(" DROP ");
            sb.append(Utils.toCaseSensitive(column.getName()));
            break;
        case ALTER_OPTIONS:
            sb.append(" WITH ");
            for(Map.Entry<Selector,Selector> entry: properties.entrySet()) {
                sb.append(entry.getKey().getStringValue()).append(" = ").append(entry.getValue().toString()).append(" AND ");
            }
            sb.delete(sb.lastIndexOf(" AND"), sb.length());
            break;
        default:
            sb.append("BAD OPTION");
            break;
        }

        return sb.toString();
    }



    public TableName getTableName() {
        return tableName;
    }

    public void setTableName(TableName tableName) {
        this.tableName = tableName;
    }



    public ColumnName getColumn() {
        return column;
    }

    public ColumnType getType() {
        return type;
    }

    public AlterOperation getOption() {
        return option;
    }

    public Map<Selector, Selector> getProperties() {
        return properties;
    }
}
