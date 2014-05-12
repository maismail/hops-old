package org.apache.hadoop.fs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.TestDfsClient;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.datanode.DataNodeUtil;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.junit.Test;
import se.sics.hop.erasure_coding.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

public class TestErasureCodingFileSystem extends BasicClusterTestCase {

  public static final Log LOG = LogFactory.getLog(TestErasureCodingFileSystem.class);

  private static final int TEST_BLOCK_COUNT = 10;

  private final long seed = 0xDEADBEEFL;
  private final Path testFile = new Path("/test_file");
  private final Path parityFile = new Path("/raidsrc/test_file");

  @Test
  public void testBlockRepair() throws IOException, InterruptedException {
    DistributedFileSystem dfs = getDfs();
    TestDfsClient testDfsClient = new TestDfsClient(getConf());
    testDfsClient.injectIntoDfs(dfs);

    EncodingPolicy policy = new EncodingPolicy("src", 1);
    TestUtil.createRandomFile(dfs, testFile, seed, TEST_BLOCK_COUNT, DFS_TEST_BLOCK_SIZE, policy);
//    Codec.initializeCodecs(conf);
    FileStatus testFileStatus = dfs.getFileStatus(testFile);
//    encodingManager.encodeFile("src", testFile);

    // Busy waiting until the encoding is done
    while(!dfs.getEncodingStatus(testFile.toUri().getPath()).isEncoded()) {
      Thread.sleep(1000);
    }
    FileStatus parityFileStatus = dfs.getFileStatus(parityFile);

    String path = testFileStatus.getPath().toUri().getPath();
    int blockToLoose = new Random(seed).nextInt((int) (testFileStatus.getLen() / testFileStatus.getBlockSize()));
    LocatedBlock lb = dfs.getClient().getLocatedBlocks(path, 0, Long.MAX_VALUE).get(blockToLoose);
    DataNodeUtil.looseBlock(getCluster(), lb);
    List<LocatedBlock> lostBlocks = new ArrayList<LocatedBlock>();
    lostBlocks.add(lb);
    LocatedBlocks locatedBlocks = new LocatedBlocks(0, false, lostBlocks, null, true);
    testDfsClient.setMissingLocatedBlocks(locatedBlocks);
    LOG.info("Loosing block " + lb.toString());
    getCluster().triggerBlockReports();

    DistributedRaidFileSystem drfs = new DistributedRaidFileSystem();
    NameNode nameNode = getCluster().getNameNode();
    drfs.initialize(nameNode.getUri(nameNode.getServiceRpcAddress()), getConf());
    try {
      FSDataInputStream in = drfs.open(testFile);
      byte[] buff = new byte[TEST_BLOCK_COUNT * DFS_TEST_BLOCK_SIZE];
      in.readFully(0, buff);
    } catch (BlockMissingException e) {
      fail("Repair failed. Missing a block.");
    }
  }
}