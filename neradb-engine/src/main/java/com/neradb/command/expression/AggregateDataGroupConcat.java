/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.neradb.command.expression;

import java.util.ArrayList;

import com.neradb.common.utils.New;
import com.neradb.dbobject.Database;
import com.neradb.util.ValueHashMap;
import com.neradb.value.Value;
import com.neradb.value.ValueNull;

/**
 * Data stored while calculating a GROUP_CONCAT aggregate.
 */
class AggregateDataGroupConcat extends AggregateData {
    private ArrayList<Value> list;
    private ValueHashMap<AggregateDataGroupConcat> distinctValues;

    @Override
    void add(Database database, int dataType, boolean distinct, Value v) {
        if (v == ValueNull.INSTANCE) {
            return;
        }
        if (distinct) {
            if (distinctValues == null) {
                distinctValues = ValueHashMap.newInstance();
            }
            distinctValues.put(v, this);
            return;
        }
        if (list == null) {
            list = New.arrayList();
        }
        list.add(v);
    }

    @Override
    Value getValue(Database database, int dataType, boolean distinct) {
        if (distinct) {
            groupDistinct(database, dataType);
        }
        return null;
    }

    ArrayList<Value> getList() {
        return list;
    }

    private void groupDistinct(Database database, int dataType) {
        if (distinctValues == null) {
            return;
        }
        for (Value v : distinctValues.keys()) {
            add(database, dataType, false, v);
        }
    }
}
