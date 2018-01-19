/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.neradb.command.ddl;

import com.neradb.api.ErrorCode;
import com.neradb.command.CommandInterface;
import com.neradb.dbobject.Database;
import com.neradb.dbobject.schema.Constant;
import com.neradb.dbobject.schema.Schema;
import com.neradb.engine.Session;
import com.neradb.message.DbException;

/**
 * This class represents the statement
 * DROP CONSTANT
 */
public class DropConstant extends SchemaCommand {

    private String constantName;
    private boolean ifExists;

    public DropConstant(Session session, Schema schema) {
        super(session, schema);
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setConstantName(String constantName) {
        this.constantName = constantName;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        Constant constant = getSchema().findConstant(constantName);
        if (constant == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.CONSTANT_NOT_FOUND_1, constantName);
            }
        } else {
            db.removeSchemaObject(session, constant);
        }
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_CONSTANT;
    }

}
