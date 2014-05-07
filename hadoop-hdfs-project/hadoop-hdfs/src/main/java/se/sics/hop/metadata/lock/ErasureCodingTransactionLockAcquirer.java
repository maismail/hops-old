package se.sics.hop.metadata.lock;

import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.erasure_coding.EncodingStatus;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.transaction.lock.TransactionLockAcquirer;
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

    ErasureCodingTransactionLocks locks = getLocks();
    if (locks.getEncodingStatusLock() != null) {
      acquireLock(locks.getEncodingStatusLock(), EncodingStatus.Finder.ByInodeId, locks.getInodeId());
    }
    return locks;
  }

  @Override
  public ErasureCodingTransactionLocks getLocks() {
    return (ErasureCodingTransactionLocks) super.getLocks();
  }
}
