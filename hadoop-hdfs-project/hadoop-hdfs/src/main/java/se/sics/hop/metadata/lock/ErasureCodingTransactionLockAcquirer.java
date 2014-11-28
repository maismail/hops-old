package se.sics.hop.metadata.lock;

import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.erasure_coding.EncodingStatus;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.metadata.INodeIdentifier;
import se.sics.hop.metadata.hdfs.entity.hop.BlockChecksum;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.transaction.lock.OldTransactionLocks;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static se.sics.hop.metadata.hdfs.dal.BlockChecksumDataAccess.KeyTuple;

public class ErasureCodingTransactionLockAcquirer extends HDFSTransactionLockAcquirer {

  public ErasureCodingTransactionLockAcquirer() {
    super(new ErasureCodingTransactionLocks());
  }

  public ErasureCodingTransactionLockAcquirer(LinkedList<INode> resolvedInodes, boolean preTxPathFullyResolved) {
    super(new ErasureCodingTransactionLocks(resolvedInodes, preTxPathFullyResolved));
  }

  public OldTransactionLocks acquireForDelete(boolean isErasureCodingEnabled)
      throws PersistanceException, UnresolvedPathException, ExecutionException, SubtreeLockedException {
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
  public OldTransactionLocks acquire() throws PersistanceException, UnresolvedPathException, ExecutionException, SubtreeLockedException {
    super.acquire();
    acquireEncodingLock();
    acquireEncodingLockOnPathLeaf();
    acquireEncodingLockByParityPathLeaf();
    acquireBlockChecksum();
    return getLocks();
  }

  private void acquireEncodingLock() throws PersistanceException {
    ErasureCodingTransactionLocks locks = getLocks();
    if (locks.getEncodingStatusLock() != null) {
      // TODO STEFFEN - Should only acquire the locks if we know it has a status and also not twice.
      // Maybe add a flag to iNode specifying whether it's encoded or a parity file
      if (acquireLock(locks.getEncodingStatusLock(), EncodingStatus.Finder.ByInodeId, locks.getEncodingStatusInodeId()) != null) {
        // Cannot be both
        return;
      }
      EncodingStatus status = acquireLock(TransactionLockTypes.LockType.READ, EncodingStatus.Finder.ByParityInodeId,
          locks.getEncodingStatusInodeId());
      if (status == null) {
        return;
      }
      // The inode was not locked as the locked inode is form the parity file. Lock the proper one.
      iNodeScanLookUpByID(TransactionLockTypes.INodeLockType.WRITE, status.getInodeId(), getLocks());
      // We didn't have a lock on it when reading it. So read it again.
      acquireLock(locks.getEncodingStatusLock(), EncodingStatus.Finder.ByParityInodeId, locks.getEncodingStatusInodeId());
    }
  }

  private void acquireEncodingLockOnPathLeaf() throws PersistanceException {
    ErasureCodingTransactionLocks locks = getLocks();
    if (locks.getEncodingStatusLockOnPathLeaf() != null) {
      INode target = allResolvedINodes.getFirst().getLast();
      acquireLock(locks.getEncodingStatusLockOnPathLeaf(), EncodingStatus.Finder.ByInodeId, target.getId());
    }
  }

  private void acquireEncodingLockByParityPathLeaf() throws PersistanceException {
    ErasureCodingTransactionLocks locks = getLocks();
    if (locks.getEncodingStatusLockByParityFilePathLeaf() != null) {
      INode target = allResolvedINodes.getFirst().getLast();
      acquireLock(locks.getEncodingStatusLockByParityFilePathLeaf(), EncodingStatus.Finder.ByParityInodeId, target.getId());
    }
  }

  private void acquireBlockChecksum() throws PersistanceException {
    ErasureCodingTransactionLocks locks = getLocks();
    if (locks.getBlockChecksumLockByKeyTuple() != null) {
      int i = 0;
      for (LinkedList<INode> path : allResolvedINodes) {
        INode target = path.getLast();
        KeyTuple key = new KeyTuple(target.getId(), locks.getBlockChecksumBlockIndexes()[i++]);
        acquireLock(locks.getBlockChecksumLockByKeyTuple(), BlockChecksum.Finder.ByKeyTuple, key);
      }
    }
  }

  @Override
  public ErasureCodingTransactionLocks acquireByBlock(INodeIdentifier iNodeIdentifier) throws PersistanceException, UnresolvedPathException, ExecutionException {
    super.acquireByBlock(iNodeIdentifier);
    acquireEncodingLock();
    return getLocks();
  }

  @Override
  public ErasureCodingTransactionLocks getLocks() {
    return (ErasureCodingTransactionLocks) super.getLocks();
  }
}
