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

package extensions;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.logging.LogManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.kvstore.raft.RpcClient;
import com.openddal.kvstore.raft.RpcClientFactory;

public class RpcTcpClientFactory implements RpcClientFactory {
	
	private final static Logger logger = LoggerFactory.getLogger(BinaryUtils.class);

	
    private ExecutorService executorService;

    public RpcTcpClientFactory(ExecutorService executorService){
        this.executorService = executorService;
    }

    @Override
    public RpcClient createRpcClient(String endpoint) {
        try {
            URI uri = new URI(endpoint);
            return new RpcTcpClient(new InetSocketAddress(uri.getHost(), uri.getPort()), this.executorService);
        } catch (URISyntaxException e) {
            logger.error(String.format("%s is not a valid uri", endpoint));
            throw new IllegalArgumentException("invalid uri for endpoint");
        }
    }

}
