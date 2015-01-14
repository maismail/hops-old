package se.sics.hop.transaction.lock;

import se.sics.hop.exception.TransientStorageException;

import java.util.Collection;
import se.sics.hop.leaderElection.node.ActiveNode;

public class SubtreeLockedException extends TransientStorageException {



  public SubtreeLockedException(Collection<ActiveNode> namenodes) {
    super(createMessage(namenodes));
  }

  private static String createMessage(Collection<ActiveNode> namenodes) {
    StringBuilder builder = new StringBuilder();
    builder.append("Active NameNode were ");
    for (ActiveNode namenode : namenodes) {
      builder.append(namenode.toString() + ", ");
    }
    return builder.toString();
  }
}
