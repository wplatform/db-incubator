/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.neradb.engine.spi;

import com.neradb.command.ddl.CreateTableData;
import com.neradb.dbobject.table.Table;

/**
 * A class that implements this interface can create custom table
 * implementations.
 *
 * @author Sergi Vladykin
 */
public interface TableEngine {

    /**
     * Create new table.
     *
     * @param data the data to construct the table
     * @return the created table
     */
    Table createTable(CreateTableData data);

}
