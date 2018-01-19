/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.neradb.command.ddl;

import com.neradb.command.CommandInterface;
import com.neradb.common.DbException;
import com.neradb.common.ErrorCode;
import com.neradb.dbobject.Database;
import com.neradb.dbobject.UserDataType;
import com.neradb.engine.Session;

/**
 * This class represents the statement
 * DROP DOMAIN
 */
public class DropUserDataType extends DefineCommand {

    private String typeName;
    private boolean ifExists;

    public DropUserDataType(Session session) {
        super(session);
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        UserDataType type = db.findUserDataType(typeName);
        if (type == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.USER_DATA_TYPE_NOT_FOUND_1, typeName);
            }
        } else {
            db.removeDatabaseObject(session, type);
        }
        return 0;
    }

    public void setTypeName(String name) {
        this.typeName = name;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_DOMAIN;
    }

}
