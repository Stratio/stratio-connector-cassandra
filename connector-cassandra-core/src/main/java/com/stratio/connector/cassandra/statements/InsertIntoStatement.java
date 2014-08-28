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


import com.stratio.meta.common.utils.StringUtils;
import com.stratio.meta2.common.statements.structures.terms.GenericTerm;
import org.apache.log4j.Logger;

import java.util.List;


/**
 * Class that models an {@code INSERT INTO} statement from the META language.
 */
public class InsertIntoStatement{


  /**
   * The name of the target table.
   */
  private String tableName;

  /**
   * The list of columns to be assigned.
   */
  private List<String> ids;


  /**
   * A list of {@link com.stratio.meta2.common.statements.structures.terms.GenericTerm} with the literal values to be
   * assigned if the insert type matches {@code TYPE_VALUES_CLAUSE}.
   */
  private List<GenericTerm> cellValues;

  /**
   * Indicates if exists "IF NOT EXISTS" clause.
   */
  private boolean ifNotExists;


    /**
     * Catalog
     */
  private String catalog;

    /**
     * Indicates if there is a catalog specified in the table name
     */
    private boolean catalogInc;

  /**
   * Class logger.
   */
  private static final Logger LOG = Logger.getLogger(InsertIntoStatement.class);

  /**
   * InsertIntoStatement general constructor.
   * 
   * @param tableName Tablename target.
   * @param ids List of name of fields in the table.
   * @param cellValues List of {@link com.stratio.meta2.common.statements.structures.terms.GenericTerm} to insert.
   * @param ifNotExists Boolean that indicates if IF NOT EXISTS clause is included in the query.
   */
  public InsertIntoStatement(String tableName, List<String> ids, List<GenericTerm> cellValues, boolean ifNotExists) {

    this.tableName = tableName;
    if (tableName.contains(".")) {
      String[] ksAndTableName = tableName.split("\\.");
      catalog = ksAndTableName[0];
      this.tableName = ksAndTableName[1];
      catalogInc = true;
    }
    this.ids = ids;
    this.cellValues = cellValues;
    this.ifNotExists = ifNotExists;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("INSERT INTO ");
    if (catalogInc) {
      sb.append(catalog).append(".");
    }
    sb.append(tableName).append(" (");
    sb.append(StringUtils.stringList(ids, ", ")).append(") ");

    sb.append("VALUES (");
    sb.append(StringUtils.stringList(cellValues, ", "));
    sb.append(")");

    if (ifNotExists) {
      sb.append(" IF NOT EXISTS");
    }

    return sb.toString();
  }





}
