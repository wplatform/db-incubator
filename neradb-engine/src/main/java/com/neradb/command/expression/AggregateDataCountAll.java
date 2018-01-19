/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.neradb.command.expression;

import com.neradb.dbobject.Database;
import com.neradb.message.DbException;
import com.neradb.value.Value;
import com.neradb.value.ValueLong;
import com.neradb.value.ValueNull;

/**
 * Data stored while calculating a COUNT(*) aggregate.
 */
class AggregateDataCountAll extends AggregateData {
    private long count;

    @Override
    void add(Database database, int dataType, boolean distinct, Value v) {
        if (distinct) {
            throw DbException.throwInternalError();
        }
        count++;
    }

    @Override
    Value getValue(Database database, int dataType, boolean distinct) {
        if (distinct) {
            throw DbException.throwInternalError();
        }
        Value v = ValueLong.get(count);
        return v == null ? ValueNull.INSTANCE : v.convertTo(dataType);
    }

}
