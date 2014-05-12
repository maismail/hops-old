package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

public class TestDfsClient extends DFSClient {

  private LocatedBlocks missingLocatedBlocks;

  public TestDfsClient(Configuration conf) throws IOException {
    super(conf);
  }

  public TestDfsClient(InetSocketAddress address, Configuration conf) throws IOException {
    super(address, conf);
  }

  public TestDfsClient(URI nameNodeUri, Configuration conf) throws IOException {
    super(nameNodeUri, conf);
  }

  public TestDfsClient(URI nameNodeUri, Configuration conf, FileSystem.Statistics stats) throws IOException {
    super(nameNodeUri, conf, stats);
  }

  @Override
  public LocatedBlocks getMissingLocatedBlocks(String src) throws IOException {
    return missingLocatedBlocks;
  }

  @Override
  public LocatedBlock getRepairedBlockLocations(String path, long blockId) throws IOException {
    for (LocatedBlock locatedBlock : missingLocatedBlocks.getLocatedBlocks()) {
      if (locatedBlock.getBlock().getBlockId() == blockId) {
        return locatedBlock;
      }
    }
    throw new IOException("Block not found");
  }

  public void setMissingLocatedBlocks(LocatedBlocks missingLocatedBlocks) {
    this.missingLocatedBlocks = missingLocatedBlocks;
  }

  public void injectIntoDfs(DistributedFileSystem dfs) {
    dfs.dfs = this;
  }
}
