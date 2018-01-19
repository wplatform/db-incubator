/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.neradb.dbobject;

import java.sql.Connection;
import java.sql.SQLException;

import com.neradb.command.Parser;
import com.neradb.common.DbException;
import com.neradb.dbobject.table.Table;
import com.neradb.engine.Session;
import com.neradb.engine.spi.Aggregate;
import com.neradb.engine.spi.AggregateFunction;
import com.neradb.message.Trace;
import com.neradb.util.JdbcUtils;
import com.neradb.value.DataType;

/**
 * Represents a user-defined aggregate function.
 */
public class UserAggregate extends DbObjectBase {

    private String className;
    private Class<?> javaClass;

    public UserAggregate(Database db, int id, String name, String className,
            boolean force) {
        initDbObjectBase(db, id, name, Trace.FUNCTION);
        this.className = className;
        if (!force) {
            getInstance();
        }
    }

    public Aggregate getInstance() {
        if (javaClass == null) {
            javaClass = JdbcUtils.loadUserClass(className);
        }
        Object obj;
        try {
            obj = javaClass.newInstance();
            Aggregate agg;
            if (obj instanceof Aggregate) {
                agg = (Aggregate) obj;
            } else {
                agg = new AggregateWrapper((AggregateFunction) obj);
            }
            return agg;
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public String getCreateSQLForCopy(Table table, String quotedName) {
        throw DbException.throwInternalError(toString());
    }

    @Override
    public String getDropSQL() {
        return "DROP AGGREGATE IF EXISTS " + getSQL();
    }

    @Override
    public String getCreateSQL() {
        return "CREATE FORCE AGGREGATE " + getSQL() +
                " FOR " + Parser.quoteIdentifier(className);
    }

    @Override
    public int getType() {
        return DbObject.AGGREGATE;
    }

    @Override
    public synchronized void removeChildrenAndResources(Session session) {
        database.removeMeta(session, getId());
        className = null;
        javaClass = null;
        invalidate();
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("AGGREGATE");
    }

    public String getJavaClassName() {
        return this.className;
    }

    /**
     * Wrap {@link AggregateFunction} in order to behave as
     * {@link com.neradb.engine.spi.Aggregate}
     **/
    private static class AggregateWrapper implements Aggregate {
        private final AggregateFunction aggregateFunction;

        AggregateWrapper(AggregateFunction aggregateFunction) {
            this.aggregateFunction = aggregateFunction;
        }

        @Override
        public void init(Connection conn) throws SQLException {
            aggregateFunction.init(conn);
        }

        @Override
        public int getInternalType(int[] inputTypes) throws SQLException {
            int[] sqlTypes = new int[inputTypes.length];
            for (int i = 0; i < inputTypes.length; i++) {
                sqlTypes[i] = DataType.convertTypeToSQLType(inputTypes[i]);
            }
            return  DataType.convertSQLTypeToValueType(aggregateFunction.getType(sqlTypes));
        }

        @Override
        public void add(Object value) throws SQLException {
            aggregateFunction.add(value);
        }

        @Override
        public Object getResult() throws SQLException {
            return aggregateFunction.getResult();
        }
    }

}
