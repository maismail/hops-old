package se.sics.hop.transaction.lock;

import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.util.Collection;
import se.sics.hop.leaderElection.node.ActiveNode;

public final class SubtreeLockHelper {

  public static boolean isSubtreeLocked(boolean subtreeLocked, long nameNodeId, Collection<ActiveNode> activeNamenodes) {
    return subtreeLocked && NameNode.isNameNodeAlive(activeNamenodes, nameNodeId);
  }
}
