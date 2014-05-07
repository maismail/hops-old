package se.sics.hop.metadata.lock;

import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.LinkedList;

public class ErasureCodingTransactionLocks extends HDFSTransactionLocks {

  private TransactionLockTypes.LockType encodingStatusLock;
  private long inodeId;

  public ErasureCodingTransactionLocks() {

  }

  public ErasureCodingTransactionLocks(LinkedList<INode> resolvedInodes, boolean preTxPathFullyResolved) {
    super(resolvedInodes, preTxPathFullyResolved);
  }

  public ErasureCodingTransactionLocks addEncodingStatusLock(TransactionLockTypes.LockType lock, long inodeId) {
    this.encodingStatusLock = lock;
    this.inodeId = inodeId;

    return this;
  }

  public TransactionLockTypes.LockType getEncodingStatusLock() {
    return encodingStatusLock;
  }

  public long getInodeId() {
    return inodeId;
  }
}
