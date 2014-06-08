package se.sics.hop.metadata.lock;

import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeIdentifier;
import se.sics.hop.erasure_coding.EncodingStatus;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.LinkedList;
import java.util.List;

public class ErasureCodingTransactionLockAcquirer extends HDFSTransactionLockAcquirer {

  public ErasureCodingTransactionLockAcquirer() {
    super(new ErasureCodingTransactionLocks());
  }

  public ErasureCodingTransactionLockAcquirer(LinkedList<INode> resolvedInodes, boolean preTxPathFullyResolved) {
    super(new ErasureCodingTransactionLocks(resolvedInodes, preTxPathFullyResolved));
  }

  public TransactionLocks acquireForDelete(boolean isErasureCodingEnabled)
        throws PersistanceException, UnresolvedPathException {
    super.acquire();
    if (isErasureCodingEnabled) {
      for (List<INode> list : allResolvedINodes) {
        for (INode iNode : list) {
          acquireLock(TransactionLockTypes.LockType.READ_COMMITTED, EncodingStatus.Finder.ByInodeId, iNode.getId());
        }
      }
    }
    return getLocks();
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
      // TODO STEFFEN - Should only acquire the locks if we know it has a status and also not twice.
      // Maybe add a flag to iNode specifying whether it's encoded or a parity file
      if (acquireLock(locks.getEncodingStatusLock(), EncodingStatus.Finder.ByInodeId, locks.getInodeId()) != null) {
        // Cannot be both
        return;
      }
      EncodingStatus status = acquireLock(TransactionLockTypes.LockType.READ, EncodingStatus.Finder.ByParityInodeId,
          locks.getInodeId());
      if (status == null) {
        return;
      }
      // The inode was not locked as the locked inode is form the parity file. Lock the proper one.
      iNodeScanLookUpByID(TransactionLockTypes.INodeLockType.WRITE, status.getInodeId(), getLocks());
      // We didn't have a lock on it when reading it. So read it again.
      acquireLock(locks.getEncodingStatusLock(), EncodingStatus.Finder.ByParityInodeId, locks.getInodeId());
    }
  }

  @Override
  public ErasureCodingTransactionLocks acquireByBlock(INodeIdentifier iNodeIdentifier) throws PersistanceException, UnresolvedPathException {
    super.acquireByBlock(iNodeIdentifier);
    acquireEncodingLock();
    return getLocks();
  }

  @Override
  public ErasureCodingTransactionLocks getLocks() {
    return (ErasureCodingTransactionLocks) super.getLocks();
  }
}
