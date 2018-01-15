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

import com.openddal.command.CommandInterface;
import com.openddal.dbobject.schema.Schema;
import com.openddal.dbobject.table.IndexColumn;
import com.openddal.engine.Session;

/**
 * This class represents the statement
 * CREATE INDEX
 */
public class CreateIndex extends SchemaCommand {

    private String tableName;
    private String indexName;
    private IndexColumn[] indexColumns;
    private boolean primaryKey, unique, hash, spatial;
    private boolean ifNotExists;
    private String comment;

    public CreateIndex(Session session, Schema schema) {
        super(session, schema);
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public IndexColumn[] getIndexColumns() {
        return indexColumns;
    }

    public void setIndexColumns(IndexColumn[] columns) {
        this.indexColumns = columns;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(boolean b) {
        this.primaryKey = b;
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean b) {
        this.unique = b;
    }

    public boolean isHash() {
        return hash;
    }

    public void setHash(boolean b) {
        this.hash = b;
    }

    public boolean isSpatial() {
        return spatial;
    }

    public void setSpatial(boolean b) {
        this.spatial = b;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_INDEX;
    }
}
