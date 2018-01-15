/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.openddal.command.ddl;

import java.util.ArrayList;

import com.openddal.command.expression.Expression;
import com.openddal.dbobject.schema.Schema;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.Table;
import com.openddal.engine.Session;

/**
 * This class represents the statements
 * ALTER TABLE ADD,
 * ALTER TABLE ADD IF NOT EXISTS,
 * ALTER TABLE ALTER COLUMN,
 * ALTER TABLE ALTER COLUMN RESTART,
 * ALTER TABLE ALTER COLUMN SELECTIVITY,
 * ALTER TABLE ALTER COLUMN SET DEFAULT,
 * ALTER TABLE ALTER COLUMN SET NOT NULL,
 * ALTER TABLE ALTER COLUMN SET NULL,
 * ALTER TABLE DROP COLUMN
 */
public class AlterTableAlterColumn extends SchemaCommand {

    private Table table;
    private Column oldColumn;
    private Column newColumn;
    private int type;
    private Expression defaultExpression;
    private Expression newSelectivity;
    private String addBefore;
    private String addAfter;
    private boolean ifNotExists;
    private ArrayList<Column> columnsToAdd;
    

    public AlterTableAlterColumn(Session session, Schema schema) {
        super(session, schema);
    }

    public void setSelectivity(Expression selectivity) {
        newSelectivity = selectivity;
    }

    @Override
    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public void setNewColumns(ArrayList<Column> columnsToAdd) {
        this.columnsToAdd = columnsToAdd;
    }

    public ArrayList<Column> getColumnsToAdd() {
        return columnsToAdd;
    }

    public void setColumnsToAdd(ArrayList<Column> columnsToAdd) {
        this.columnsToAdd = columnsToAdd;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Column getOldColumn() {
        return oldColumn;
    }

    public void setOldColumn(Column oldColumn) {
        this.oldColumn = oldColumn;
    }

    public Column getNewColumn() {
        return newColumn;
    }

    public void setNewColumn(Column newColumn) {
        this.newColumn = newColumn;
    }

    public Expression getDefaultExpression() {
        return defaultExpression;
    }

    public void setDefaultExpression(Expression defaultExpression) {
        this.defaultExpression = defaultExpression;
    }

    public Expression getNewSelectivity() {
        return newSelectivity;
    }

    public String getAddBefore() {
        return addBefore;
    }

    public void setAddBefore(String before) {
        this.addBefore = before;
    }

    public String getAddAfter() {
        return addAfter;
    }

    public void setAddAfter(String after) {
        this.addAfter = after;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }
}
