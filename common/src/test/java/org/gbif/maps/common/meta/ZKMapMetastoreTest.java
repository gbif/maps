package org.gbif.maps.common.meta;

import java.io.File;
import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

public class ZKMapMetastoreTest {
  private TestingServer zkTestServer;
  private CuratorFramework client;
  private MapMetastore metastore;
  private String zk;

  @Before
  public void startZookeeper() throws Exception {
    String portAsString = System.getProperty("ZK_PORT") == null ? "2181" : System.getProperty("ZK_PORT");

    zkTestServer = new TestingServer(Integer.parseInt(portAsString));
    zk = zkTestServer.getConnectString();
    metastore = Metastores.newZookeeperMapsMeta(zk, 1000, "/gbif-map/test-dev");
    client = CuratorFrameworkFactory.newClient(zk, new RetryNTimes(3, 1000));
    client.start();
  }

  @After
  public void stopZookeeper() throws IOException {
    client.close();
    metastore.close();
    zkTestServer.stop();
  }

  @Test
  public void testLifeCycle() {
    try {
      MapTables initial = metastore.read();
      assertNull(initial);

      MapTables t1 = new MapTables("prod_tile", "prod_point");
      metastore.update(t1);

      // Double check: read ZK data directly forcing a sync to avoid latencies
      client.sync().forPath("/gbif-map/test-dev");
      byte[] raw = client.getData().forPath("/gbif-map/test-dev");
      MapTables t2 = MapTables.deserialize(raw);
      assertEquals("ZooKeeper native read returns different value", t1, metastore.read());

      Thread.sleep(1000); // not receiving a watch update within 1 sec on a local test means something is suspicious
      assertEquals("Tables not the same after a write", t1, metastore.read());

    } catch (Exception e) {
      fail("Unexpected exception occurred: " + e.getMessage());
    }
  }
}
