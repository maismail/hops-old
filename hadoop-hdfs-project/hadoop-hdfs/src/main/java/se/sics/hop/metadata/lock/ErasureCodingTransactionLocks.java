package se.sics.hop.metadata.lock;

import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.LinkedList;

public class ErasureCodingTransactionLocks extends HDFSTransactionLocks {

  private TransactionLockTypes.LockType encodingStatusLock;
  private int inodeId;

  public ErasureCodingTransactionLocks() {

  }

  public ErasureCodingTransactionLocks(LinkedList<INode> resolvedInodes, boolean preTxPathFullyResolved) {
    super(resolvedInodes, preTxPathFullyResolved);
  }

  public ErasureCodingTransactionLocks addEncodingStatusLock(int inodeId) {
    this.encodingStatusLock = TransactionLockTypes.LockType.READ_COMMITTED;;
    this.inodeId = inodeId;

    return this;
  }

  public TransactionLockTypes.LockType getEncodingStatusLock() {
    return encodingStatusLock;
  }

  public int getInodeId() {
    return inodeId;
  }
}
