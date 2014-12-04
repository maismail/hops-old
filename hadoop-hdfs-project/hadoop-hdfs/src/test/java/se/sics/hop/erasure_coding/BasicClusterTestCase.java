package se.sics.hop.erasure_coding;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

public abstract class BasicClusterTestCase extends TestCase {

  protected static final int DFS_TEST_BLOCK_SIZE = 4 * 1024;

  private MiniDFSCluster cluster;
  private DistributedFileSystem dfs;
  private Configuration conf;

  protected BasicClusterTestCase() {
    this(new HdfsConfiguration());
    conf.setLong(DFS_BLOCK_SIZE_KEY, DFS_TEST_BLOCK_SIZE);
    conf.setInt(DFS_REPLICATION_KEY, DFS_REPLICATION_DEFAULT);
    conf.setBoolean(DFSConfigKeys.ERASURE_CODING_ENABLED_KEY, true);
    conf.setInt(DFSConfigKeys.RECHECK_INTERVAL_KEY, 10000);
  }

  protected BasicClusterTestCase(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setUp() throws Exception {
    cluster = new MiniDFSCluster.Builder(getConf())
        .numDataNodes(getConf().getInt(DFSConfigKeys.DFS_REPLICATION_KEY, DFSConfigKeys.DFS_REPLICATION_DEFAULT))
        .build();
    cluster.waitActive();

    dfs = cluster.getFileSystem();
  }

  @Override
  public void tearDown() throws Exception {
    FileStatus[] files = dfs.globStatus(new Path("/*"));
    for (FileStatus file: files) {
      dfs.delete(file.getPath(), true);
    }
    dfs.close();
    cluster.shutdown();
  }

  public DistributedFileSystem getDfs() {
    return dfs;
  }

  public MiniDFSCluster getCluster() {
    return cluster;
  }
}

