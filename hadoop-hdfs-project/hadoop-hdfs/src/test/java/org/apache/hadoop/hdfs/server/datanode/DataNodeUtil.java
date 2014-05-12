package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import java.io.IOException;
import java.util.List;

public class DataNodeUtil {

  public static void looseBlock(MiniDFSCluster cluster, LocatedBlock lb) throws IOException {
    List<DataNode> dataNodes =  cluster.getDataNodes();
    String blockPoolId = lb.getBlock().getBlockPoolId();
    Block block = lb.getBlock().getLocalBlock();
    Block[] toDelete =  new Block[]{block};
    for (DataNode dataNode: dataNodes) {
      if (dataNode.blockScanner != null) {
        dataNode.blockScanner.deleteBlocks(blockPoolId, toDelete);
      }
      try {
        dataNode.data.invalidate(blockPoolId, toDelete);
      } catch (IOException e) {
        // Thrown because not all DataNodes have all blocks
      }
    }
    waitForDelete(cluster, lb);
    assert(0 == cluster.getAllBlockFiles(lb.getBlock()).length);
  }

  public static void waitForDelete(MiniDFSCluster cluster, LocatedBlock lb) {
    // Deleting of blocks is asynchronous. Bussy waiting is my best approach for now
    while (cluster.getAllBlockFiles(lb.getBlock()).length > 0);
  }
}
