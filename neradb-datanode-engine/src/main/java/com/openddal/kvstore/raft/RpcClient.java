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

import java.util.concurrent.CompletableFuture;

public interface RpcClient {

    /**
     * Sends a RaftRequestMessage to peer and read a response from peer
     * this will not be called concurrently
     * @param request Raft rpc request message
     * @return Raft rpc response
     */
    public CompletableFuture<RaftResponseMessage> send(RaftRequestMessage request);
}
