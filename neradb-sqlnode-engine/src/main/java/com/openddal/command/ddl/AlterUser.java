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

import com.openddal.command.expression.Expression;
import com.openddal.dbobject.User;
import com.openddal.engine.Session;
import com.openddal.message.DbException;

/**
 * This class represents the statements
 * ALTER USER ADMIN,
 * ALTER USER RENAME,
 * ALTER USER SET PASSWORD
 */
public class AlterUser extends DefineCommand {

    private int type;
    private User user;
    private String newName;
    private Expression password;
    private Expression salt;
    private Expression hash;
    private boolean admin;

    public AlterUser(Session session) {
        super(session);
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    @Override
    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public String getNewName() {
        return newName;
    }

    public void setNewName(String newName) {
        this.newName = newName;
    }

    public Expression getPassword() {
        return password;
    }

    public void setPassword(Expression password) {
        this.password = password;
    }

    public Expression getSalt() {
        return salt;
    }

    public void setSalt(Expression e) {
        salt = e;
    }

    public Expression getHash() {
        return hash;
    }

    public void setHash(Expression e) {
        hash = e;
    }

    public boolean isAdmin() {
        return admin;
    }

    public void setAdmin(boolean admin) {
        this.admin = admin;
    }


}
