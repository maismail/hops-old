package se.sics.hop.metadata.lock;

import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.transaction.lock.TransactionLockTypes;

import java.util.LinkedList;

public class ErasureCodingTransactionLocks extends HDFSTransactionLocks {

  private TransactionLockTypes.LockType encodingStatusLock;
  private int encodingStatusInodeId;

  private TransactionLockTypes.LockType encodingStatusLockOnPathLeaf;
  private TransactionLockTypes.LockType encodingStatusLockByParityFilePathLeaf;

  private TransactionLockTypes.LockType blockChecksumLockOnTargets;

  private TransactionLockTypes.LockType blockChecksumLockByKeyTuple;
  private Integer[] blockChecksumBlockIndexes;

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
    this.encodingStatusInodeId = inodeId;

    return this;
  }

  public ErasureCodingTransactionLocks addBlockChecksumLockOnTargets() {
    this.blockChecksumLockOnTargets = TransactionLockTypes.LockType.READ_COMMITTED;
    return this;
  }

  public ErasureCodingTransactionLocks addBlockChecksumLockByBlockIndex(Integer... indexes) {
    this.blockChecksumLockByKeyTuple = TransactionLockTypes.LockType.READ_COMMITTED;
    blockChecksumBlockIndexes = indexes;
    return this;
  }

  public TransactionLockTypes.LockType getEncodingStatusLock() {
    return encodingStatusLock;
  }

  public Integer getEncodingStatusInodeId() {
    return encodingStatusInodeId;
  }

  public TransactionLockTypes.LockType getEncodingStatusLockOnPathLeaf() {
    return encodingStatusLockOnPathLeaf;
  }

  public TransactionLockTypes.LockType getEncodingStatusLockByParityFilePathLeaf() {
    return encodingStatusLockByParityFilePathLeaf;
  }

  public TransactionLockTypes.LockType getBlockChecksumLockOnTargets() {
    return blockChecksumLockOnTargets;
  }

  public TransactionLockTypes.LockType getBlockChecksumLockByKeyTuple() {
    return blockChecksumLockByKeyTuple;
  }

  public Integer[] getBlockChecksumBlockIndexes() {

    return blockChecksumBlockIndexes;
  }
}
