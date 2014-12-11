package se.sics.hop.transaction.lock;

import org.apache.hadoop.hdfs.server.protocol.ActiveNamenode;
import se.sics.hop.exception.TransientStorageException;

import java.util.Collection;

public class SubtreeLockedException extends TransientStorageException {



  public SubtreeLockedException(Collection<ActiveNamenode> namenodes) {
    super(createMessage(namenodes));
  }

  private static String createMessage(Collection<ActiveNamenode> namenodes) {
    StringBuilder builder = new StringBuilder();
    builder.append("Active NameNode were ");
    for (ActiveNamenode namenode : namenodes) {
      builder.append(namenode.toString() + ", ");
    }
    return builder.toString();
  }
}
