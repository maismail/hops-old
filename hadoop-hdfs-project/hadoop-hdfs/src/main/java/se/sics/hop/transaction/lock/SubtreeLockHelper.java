package se.sics.hop.transaction.lock;

import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.ActiveNamenode;

import java.util.Collection;

public final class SubtreeLockHelper {

  public static boolean isSubtreeLocked(boolean subtreeLocked, long nameNodeId, Collection<ActiveNamenode> activeNamenodes) {
    return subtreeLocked && NameNode.isNameNodeAlive(activeNamenodes, nameNodeId);
  }
}
