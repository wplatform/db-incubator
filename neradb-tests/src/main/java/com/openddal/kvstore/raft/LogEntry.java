/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  The ASF licenses 
 * this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.openddal.kvstore.raft;

/**
 * Log entry, which represents an entry stored in the sequential log store
 * replication is done through LogEntry objects
 * @author Data Technology LLC
 *
 */
public class LogEntry {

    private byte[] value;
    private long term;
    private LogValueType vaueType;

    public LogEntry(){
        this(0, null);
    }

    public LogEntry(long term, byte[] value){
        this(term, value, LogValueType.Application);
    }

    public LogEntry(long term, byte[] value, LogValueType valueType){
        this.term = term;
        this.value = value;
        this.vaueType = valueType;
    }

    public long getTerm(){
        return this.term;
    }

    public byte[] getValue(){
        return this.value;
    }

    public LogValueType getValueType(){
        return this.vaueType;
    }
}
