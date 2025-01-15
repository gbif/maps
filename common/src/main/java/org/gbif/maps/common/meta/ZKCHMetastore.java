/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.maps.common.meta;

import com.google.common.io.Closeables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.EnsurePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * An implementation of the MapMetastore backed by Zookeeper.
 *
 * This uses a Zookeeper Path Cache pattern to watch for changes of the ZK node.
 */
class ZKCHMetastore implements CHMetastore, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ZKCHMetastore.class);

  private final CuratorFramework client;
  private final NodeCache zkNodeCache;
  private final String zkNodePath;
  private String clickhouseDB;

  ZKCHMetastore(String zkEnsemble, int retryIntervalMs, String zkNodePath) throws Exception {
    this.zkNodePath = zkNodePath;
    client = CuratorFrameworkFactory.newClient(zkEnsemble, new RetryNTimes(Integer.MAX_VALUE, retryIntervalMs));
    client.start();

    // Ensure node exists so data can be set
    EnsurePath path = new EnsurePath(zkNodePath);
    path.ensure(client.getZookeeperClient());

    // start a node cache, watching for changes and initialised with the current state
    zkNodeCache = new NodeCache(client, zkNodePath, false);

    zkNodeCache.getListenable().addListener(new NodeCacheListener() {
      @Override
      public void nodeChanged() throws Exception {
        updateInternal(zkNodeCache);
      }
    });

    LOG.info("Starting watcher for {}", zkNodePath);
    zkNodeCache.start(true);

    updateInternal(zkNodeCache); // set the initial state
  }


  @Override
  public String getClickhouseDB() {
    return clickhouseDB;
  }

  @Override
  public void setClickhouseDB(String db) throws Exception {
    LOG.info("Updating ClickhouseDB[{}] with: {}", zkNodePath, db);
    client.setData().forPath(zkNodePath, db.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void close() throws IOException {
    Closeables.close(zkNodeCache, true);
    Closeables.close(client, true);
  }

  /**
   * Reads ZK node data and if not null sets the internal object.
   */
  void updateInternal(NodeCache cache) {
    try {

      ChildData child = cache.getCurrentData();
      clickhouseDB = new String(child.getData(), StandardCharsets.UTF_8);
      LOG.info("Clickhouse DB for {} updated {}", zkNodePath, clickhouseDB);
    } catch(Exception e) {
      LOG.error("Unable to update Clickhouse from ZooKeeper (MapMeta may return state data until recovered).", e);
    }
  }
}
