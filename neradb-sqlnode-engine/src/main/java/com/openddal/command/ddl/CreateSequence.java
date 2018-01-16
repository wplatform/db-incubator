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
import com.openddal.command.expression.Expression;
import com.openddal.dbobject.schema.Schema;
import com.openddal.engine.Session;
import com.openddal.message.DbException;

/**
 * This class represents the statement
 * CREATE SEQUENCE
 */
public class CreateSequence extends SchemaCommand {

    private String sequenceName;
    private boolean ifNotExists;
    private boolean cycle;
    private Expression minValue;
    private Expression maxValue;
    private Expression start;
    private Expression increment;
    private Expression cacheSize;
    private boolean belongsToTable;

    public CreateSequence(Session session, Schema schema) {
        super(session, schema);
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    public void setStartWith(Expression start) {
        this.start = start;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_SEQUENCE;
    }

    public String getSequenceName() {
        return sequenceName;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public boolean isCycle() {
        return cycle;
    }

    public void setCycle(boolean cycle) {
        this.cycle = cycle;
    }

    public Expression getMinValue() {
        return minValue;
    }

    public void setMinValue(Expression minValue) {
        this.minValue = minValue;
    }

    public Expression getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(Expression maxValue) {
        this.maxValue = maxValue;
    }

    public Expression getStart() {
        return start;
    }

    public Expression getIncrement() {
        return increment;
    }

    public void setIncrement(Expression increment) {
        this.increment = increment;
    }

    public Expression getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(Expression cacheSize) {
        this.cacheSize = cacheSize;
    }

    public boolean isBelongsToTable() {
        return belongsToTable;
    }

    public void setBelongsToTable(boolean belongsToTable) {
        this.belongsToTable = belongsToTable;
    }


}
