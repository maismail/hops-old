package se.sics.hop.metadata.lock;

import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.transaction.lock.TransactionLockTypes;

import java.util.LinkedList;

public class ErasureCodingTransactionLocks extends HDFSTransactionLocks {

  private TransactionLockTypes.LockType encodingStatusLock;
  private int inodeId;
  private TransactionLockTypes.LockType encodingStatusLockOnPathLeaf;
  private TransactionLockTypes.LockType encodingStatusLockByParityFilePathLeaf;

  public ErasureCodingTransactionLocks() {

  }

  public ErasureCodingTransactionLocks(LinkedList<INode> resolvedInodes, boolean preTxPathFullyResolved) {
    super(resolvedInodes, preTxPathFullyResolved);
  }

  public ErasureCodingTransactionLocks addEncodingStatusLockOnPathLeave() {
    this.encodingStatusLockOnPathLeaf = TransactionLockTypes.LockType.READ_COMMITTED;
    return this;
  }

  public ErasureCodingTransactionLocks addEncodingStatusLockByParityFilePathLeaf() {
    this.encodingStatusLockByParityFilePathLeaf = TransactionLockTypes.LockType.READ_COMMITTED;
    return this;
  }

  public ErasureCodingTransactionLocks addEncodingStatusLock(int inodeId) {
    this.encodingStatusLock = TransactionLockTypes.LockType.READ_COMMITTED;
    this.inodeId = inodeId;

    return this;
  }

  public TransactionLockTypes.LockType getEncodingStatusLock() {
    return encodingStatusLock;
  }

  public Integer getEncodingStatusInodeId() {
    return inodeId;
  }

  public TransactionLockTypes.LockType getEncodingStatusLockOnPathLeaf() {
    return encodingStatusLockOnPathLeaf;
  }

  public TransactionLockTypes.LockType getEncodingStatusLockByParityFilePathLeaf() {
    return encodingStatusLockByParityFilePathLeaf;
  }
}
