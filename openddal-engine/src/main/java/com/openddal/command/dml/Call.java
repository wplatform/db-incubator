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
package com.openddal.command.dml;

import com.openddal.command.CommandInterface;
import com.openddal.command.Prepared;
import com.openddal.command.expression.Expression;
import com.openddal.command.expression.ExpressionVisitor;
import com.openddal.engine.Session;
import com.openddal.message.DbException;
import com.openddal.result.LocalResult;
import com.openddal.result.ResultInterface;
import com.openddal.value.Value;

import java.sql.ResultSet;

/**
 * This class represents the statement
 * CALL.
 */
public class Call extends Prepared {

    private boolean isResultSet;
    private Expression expression;
    private Expression[] expressions;

    public Call(Session session) {
        super(session);
    }

    @Override
    public ResultInterface queryMeta() {
        LocalResult result;
        if (isResultSet) {
            Expression[] expr = expression.getExpressionColumns(session);
            result = new LocalResult(session, expr, expr.length);
        } else {
            result = new LocalResult(session, expressions, 1);
        }
        result.done();
        return result;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    @Override
    public ResultInterface query(int maxrows) {
        setCurrentRowNumber(1);
        Value v = expression.getValue(session);
        if (isResultSet) {
            v = v.convertTo(Value.RESULT_SET);
            ResultSet rs = v.getResultSet();
            return LocalResult.read(session, rs, maxrows);
        }
        LocalResult result = new LocalResult(session, expressions, 1);
        Value[] row = {v};
        result.addRow(row);
        result.done();
        return result;
    }

    @Override
    public void prepare() {
        expression = expression.optimize(session);
        expressions = new Expression[]{expression};
        isResultSet = expression.getType() == Value.RESULT_SET;
        if (isResultSet) {
            prepareAlways = true;
        }
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    @Override
    public boolean isQuery() {
        return true;
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public boolean isReadOnly() {
        return expression.isEverything(ExpressionVisitor.READONLY_VISITOR);

    }

    @Override
    public int getType() {
        return CommandInterface.CALL;
    }

    @Override
    public boolean isCacheable() {
        return !isResultSet;
    }

}
