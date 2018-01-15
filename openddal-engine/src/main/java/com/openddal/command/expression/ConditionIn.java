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
package com.openddal.command.expression;

import java.util.ArrayList;
import java.util.List;

import com.openddal.dbobject.index.IndexCondition;
import com.openddal.dbobject.table.ColumnResolver;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.engine.Database;
import com.openddal.engine.Session;
import com.openddal.util.StatementBuilder;
import com.openddal.value.Value;
import com.openddal.value.ValueBoolean;
import com.openddal.value.ValueNull;

/**
 * An 'in' condition with a list of values, as in WHERE NAME IN(...)
 */
public class ConditionIn extends Condition {

    private final Database database;
    private final ArrayList<Expression> valueList;
    private Expression left;
    private int queryLevel;

    /**
     * Create a new IN(..) condition.
     *
     * @param database the database
     * @param left     the expression before IN
     * @param values   the value list (at least one element)
     */
    public ConditionIn(Database database, Expression left,
                       ArrayList<Expression> values) {
        this.database = database;
        this.left = left;
        this.valueList = values;
    }

    @Override
    public Value getValue(Session session) {
        Value l = left.getValue(session);
        if (l == ValueNull.INSTANCE) {
            return l;
        }
        boolean result = false;
        boolean hasNull = false;
        for (Expression e : valueList) {
            Value r = e.getValue(session);
            if (r == ValueNull.INSTANCE) {
                hasNull = true;
            } else {
                r = r.convertTo(l.getType());
                result = Comparison.compareNotNull(database, l, r, Comparison.EQUAL);
                if (result) {
                    break;
                }
            }
        }
        if (!result && hasNull) {
            return ValueNull.INSTANCE;
        }
        return ValueBoolean.get(result);
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        left.mapColumns(resolver, level);
        for (Expression e : valueList) {
            e.mapColumns(resolver, level);
        }
        this.queryLevel = Math.max(level, this.queryLevel);
    }

    @Override
    public Expression optimize(Session session) {
        left = left.optimize(session);
        boolean constant = left.isConstant();
        if (constant && left == ValueExpression.getNull()) {
            return left;
        }
        boolean allValuesConstant = true;
        boolean allValuesNull = true;
        int size = valueList.size();
        for (int i = 0; i < size; i++) {
            Expression e = valueList.get(i);
            e = e.optimize(session);
            if (e.isConstant() && e.getValue(session) != ValueNull.INSTANCE) {
                allValuesNull = false;
            }
            if (allValuesConstant && !e.isConstant()) {
                allValuesConstant = false;
            }
            valueList.set(i, e);
        }
        if (constant && allValuesConstant) {
            return ValueExpression.get(getValue(session));
        }
        if (size == 1) {
            Expression right = valueList.get(0);
            Expression expr = new Comparison(session, Comparison.EQUAL, left, right);
            expr = expr.optimize(session);
            return expr;
        }
        if (allValuesConstant && !allValuesNull) {
            int leftType = left.getType();
            if (leftType == Value.UNKNOWN) {
                return this;
            }
            Expression expr = new ConditionInConstantSet(session, left, valueList);
            expr = expr.optimize(session);
            return expr;
        }
        return this;
    }

    @Override
    public void createIndexConditions(Session session, TableFilter filter) {
        if (!(left instanceof ExpressionColumn)) {
            return;
        }
        ExpressionColumn l = (ExpressionColumn) left;
        if (filter != l.getTableFilter()) {
            return;
        }
        if (session.getDatabase().getSettings().optimizeInList) {
            ExpressionVisitor visitor = ExpressionVisitor.getNotFromResolverVisitor(filter);
            for (Expression e : valueList) {
                if (!e.isEverything(visitor)) {
                    return;
                }
            }
            filter.addIndexCondition(IndexCondition.getInList(l, valueList));
            return;
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        left.setEvaluatable(tableFilter, b);
        for (Expression e : valueList) {
            e.setEvaluatable(tableFilter, b);
        }
    }

    @Override
    public String getSQL() {
        StatementBuilder buff = new StatementBuilder("(");
        buff.append(left.getSQL()).append(" IN(");
        for (Expression e : valueList) {
            buff.appendExceptFirst(", ");
            buff.append(e.getSQL());
        }
        return buff.append("))").toString();
    }

    @Override
    public void updateAggregate(Session session) {
        left.updateAggregate(session);
        for (Expression e : valueList) {
            e.updateAggregate(session);
        }
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        if (!left.isEverything(visitor)) {
            return false;
        }
        return areAllValues(visitor);
    }

    private boolean areAllValues(ExpressionVisitor visitor) {
        for (Expression e : valueList) {
            if (!e.isEverything(visitor)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int getCost() {
        int cost = left.getCost();
        for (Expression e : valueList) {
            cost += e.getCost();
        }
        return cost;
    }

    /**
     * Add an additional element if possible. Example: given two conditions
     * A IN(1, 2) OR A=3, the constant 3 is added: A IN(1, 2, 3).
     *
     * @param other the second condition
     * @return null if the condition was not added, or the new condition
     */
    Expression getAdditional(Comparison other) {
        Expression add = other.getIfEquals(left);
        if (add != null) {
            valueList.add(add);
            return this;
        }
        return null;
    }


    @Override
    public String getPreparedSQL(Session session, List<Value> parameters) {
        StatementBuilder buff = new StatementBuilder("(");
        buff.append(left.getPreparedSQL(session, parameters)).append(" IN(");
        for (Expression e : valueList) {
            buff.appendExceptFirst(", ");
            buff.append(e.getPreparedSQL(session, parameters));
        }
        return buff.append("))").toString();
    }
}
