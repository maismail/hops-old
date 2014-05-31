package se.sics.hop.metadata.lock;

import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeIdentifier;
import se.sics.hop.erasure_coding.EncodingStatus;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.LinkedList;

public class ErasureCodingTransactionLockAcquirer extends HDFSTransactionLockAcquirer {

  public ErasureCodingTransactionLockAcquirer() {
    super(new ErasureCodingTransactionLocks());
  }

  public ErasureCodingTransactionLockAcquirer(LinkedList<INode> resolvedInodes, boolean preTxPathFullyResolved) {
    super(new ErasureCodingTransactionLocks(resolvedInodes, preTxPathFullyResolved));
  }

  @Override
  public TransactionLocks acquire() throws PersistanceException, UnresolvedPathException {
    super.acquire();
    acquireEncodingLock();
    return getLocks();
  }

  private void acquireEncodingLock() throws PersistanceException {
    ErasureCodingTransactionLocks locks = getLocks();
    if (locks.getEncodingStatusLock() != null) {
      acquireLock(locks.getEncodingStatusLock(), EncodingStatus.Finder.ByInodeId, locks.getInodeId());
      acquireLock(locks.getEncodingStatusLock(), EncodingStatus.Finder.ByParityInodeId, locks.getInodeId());
    }
  }

  @Override
  public HDFSTransactionLocks acquireByBlock(INodeIdentifier iNodeIdentifier) throws PersistanceException, UnresolvedPathException {
    super.acquireByBlock(iNodeIdentifier);
    acquireEncodingLock();
    return getLocks();
  }

  @Override
  public ErasureCodingTransactionLocks getLocks() {
    return (ErasureCodingTransactionLocks) super.getLocks();
  }
}
