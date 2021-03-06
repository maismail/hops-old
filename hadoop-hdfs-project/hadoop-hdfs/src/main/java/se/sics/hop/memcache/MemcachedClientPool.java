/*
 * Copyright 2014 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package se.sics.hop.memcache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class MemcachedClientPool {

  private final List<MemcachedClient> clients;
  private int index;

  public MemcachedClientPool(int poolSize, String server) throws IOException {
    this.index = 0;
    this.clients = new ArrayList<MemcachedClient>();
    for (int i = 0; i < poolSize; i++) {
      this.clients.add(new MemcachedClient(new BinaryConnectionFactory(), AddrUtil.getAddresses(server.trim())));
    }
  }

  public MemcachedClient poll() {
    int i = index % clients.size();
    index++;
    return clients.get(i);
  }

  public void shutdown() {
    for (MemcachedClient client : clients) {
      client.shutdown();
    }
  }
}
