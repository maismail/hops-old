package se.sics.hop.metadata.persistence.lock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.CorruptReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.ExcessReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.IndexedReplica;
import org.apache.hadoop.hdfs.server.blockmanagement.InvalidatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.UnderReplicatedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import se.sics.hop.metadata.persistence.FinderType;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryWithQuota;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileUnderConstruction;
import se.sics.hop.metadata.persistence.entity.HopLeader;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import se.sics.hop.metadata.persistence.entity.HopLeasePath;
import se.sics.hop.metadata.persistence.exceptions.PersistanceException;
import se.sics.hop.metadata.persistence.lock.TransactionLockTypes.INodeLockType;
import static se.sics.hop.metadata.persistence.lock.TransactionLockTypes.INodeLockType.READ_COMMITED;
import static se.sics.hop.metadata.persistence.lock.TransactionLockTypes.INodeLockType.WRITE_ON_PARENT;
import se.sics.hop.metadata.persistence.lock.TransactionLockTypes.LockType;
import se.sics.hop.metadata.persistence.lock.TransactionLockTypes.INodeResolveType;
import static se.sics.hop.metadata.persistence.lock.TransactionLockTypes.LockType.READ;
import static se.sics.hop.metadata.persistence.lock.TransactionLockTypes.LockType.READ_COMMITTED;
import static se.sics.hop.metadata.persistence.lock.TransactionLockTypes.LockType.WRITE;
import se.sics.hop.transcation.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.Variable;

/**
 *
 * @author Hooman <hooman@sics.se>
 * @author Salman <salman@sics.se>
 */
public class TransactionLockAcquirer {

  private final static Log LOG = LogFactory.getLog(TransactionLockAcquirer.class);
  private final TransactionLocks locks;
  private LinkedList<LinkedList<INode>> allResolvedINodes = new LinkedList<LinkedList<INode>>(); //linked lists are important. we need to perserv insertion order
  private LinkedList<Lease> leaseResults = new LinkedList<Lease>();
  private LinkedList<BlockInfo> blockResults = new LinkedList<BlockInfo>();

  public TransactionLockAcquirer() {
    this.locks = new TransactionLocks();
  }

  public TransactionLockAcquirer(LinkedList<INode> resolvedInodes, boolean preTxPathFullyResolved) {
    this.locks = new TransactionLocks(resolvedInodes, preTxPathFullyResolved);
  }

  public TransactionLocks getLocks() {
    return this.locks;
  }

  public TransactionLocks acquire() throws PersistanceException, UnresolvedPathException {
    // acuires lock in order
    if (locks.getInodeLock() != null && locks.getInodeParam() != null && locks.getInodeParam().length > 0) {
      acquireInodeLocks(locks.getInodeParam());
    }

    if (locks.getBlockLock() != null) {
      if (locks.getInodeLock() != null && locks.getBlockParam() != null) {
        throw new RuntimeException("Acquiring locks on block-infos using inode-id and block-id concurrently is not allowed!");
      }
      blockResults.addAll(acquireBlockLock());
    }

    acquireLeaseAndLpathLockNormal();
    acquireBlockRelatedLocksNormal();
    readINodeAttributes();
    return locks;
  }

  /**
   * This method acquires lockk on the inode starting with a block-id. The
   * lock-types should be set before using add* methods. Otherwise, no lock
   * would be acquired.
   *
   * @throws PersistanceException
   */
  public TransactionLocks acquireByBlock(long inodeId) throws PersistanceException, UnresolvedPathException {
    if (locks.getInodeLock() == null || locks.getBlockParam() == null) {// inodelock must be set before.
      return locks;
    }
    INode inode = null;
    if (locks.getInodeResolveType() == INodeResolveType.PATH) {
      checkPathIsResolved();
      if (!locks.getPreTxResolvedInodes().isEmpty()) {
        inode = takeLocksFromRootToLeaf(locks.getPreTxResolvedInodes(), locks.getInodeLock());
      }
    } else {
      inode = acquireINodeLockById(locks.getInodeLock(), inodeId, locks);
    }

    if (inode != null) {
      if (!(inode instanceof INodeFile)) {
        return locks; //it should abort the transaction and retry at this stage. Cause something is changed in the storage.
      }
      LinkedList<INode> resolvedINodeForBlk = new LinkedList<INode>();
      resolvedINodeForBlk.add(inode);
      allResolvedINodes.add(resolvedINodeForBlk);
    }

    blockResults.addAll(acquireLockList(locks.getBlockLock(), BlockInfo.Finder.ByInodeId, inodeId));
    // sort the blocks. it is important as the ndb returns the blocks in random order and two
    // txs trying to take locks on the blocks of a file will end up in dead lock 
    Collections.sort((List<BlockInfo>) blockResults, BlockInfo.Order.ByBlockId);

    if (blockResults.isEmpty()) {
      BlockInfo block = acquireLock(locks.getBlockLock(), BlockInfo.Finder.ById, locks.getBlockParam());
      if (block != null) {
        blockResults.add(block);
      }
    }

    // read-committed block is the same as block found by inode-file so everything is fine and continue the rest.
    acquireLeaseAndLpathLockNormal();
    acquireBlockRelatedLocksNormal();
    readINodeAttributes();
    return locks;
  }

  public TransactionLocks acquireByLease(SortedSet<String> sortedPaths) throws PersistanceException, UnresolvedPathException {
    if (locks.getLeaseParam() == null) {
      return locks;
    }

    acquireInodeLocks(sortedPaths.toArray(new String[sortedPaths.size()]));

    blockResults.addAll(acquireBlockLock());

    Lease nnLease = acquireNameNodeLease(); // NameNode lease is always acquired first.
    if (nnLease != null) {
      leaseResults.add(nnLease);
    }
    Lease lease = acquireLock(locks.getLeaseLock(), Lease.Finder.ByPKey, locks.getLeaseParam());
    if (lease == null) {
      return locks; // Lease does not exist anymore.
    }
    leaseResults.add(lease);

    List<HopLeasePath> lpResults = acquireLeasePathsLock();
    if (lpResults.size() > sortedPaths.size()) {
      return locks; // TODO: It should retry again, cause there are new lease-paths for this lease which we have not acquired their inodes locks.
    }

    acquireBlockRelatedLocksNormal();
    readINodeAttributes();
    return locks;
  }

  /**
   * Acquires lock on lease path and lease having leasepath. This is used by the
   * test cases.
   *
   * @param leasePath
   */
  public TransactionLocks acquireByLeasePath(String leasePath, TransactionLockTypes.LockType leasePathLock, TransactionLockTypes.LockType leaseLock) throws PersistanceException {
    HopLeasePath lp = acquireLock(leasePathLock, HopLeasePath.Finder.ByPKey, leasePath);
    if (lp != null) {
      acquireLock(leaseLock, Lease.Finder.ByHolderId, lp.getHolderId());
    }
    return locks;
  }

  public TransactionLocks acquireForRename() throws PersistanceException, UnresolvedPathException {
    return acquireForRename(false);
  }

  public TransactionLocks acquireForRename(boolean allowExistingDir) throws PersistanceException, UnresolvedPathException {
    byte[][] srcComponents = INode.getPathComponents(locks.getInodeParam()[0]);
    byte[][] dstComponents = INode.getPathComponents(locks.getInodeParam()[1]);

    if (locks.getInodeLock() != null && locks.getInodeParam() != null && locks.getInodeParam().length > 0) {
      //[S] consider src = /a/b/c and dst = /d
      //during the acquire lock of src write locks will be acquired on parent of c and c
      //during the acquire lock of dst write lock on the root will be acquired but the snapshot 
      //layer will not let the request go to the db as it has already cached the root inode
      //one simple solution is that to acquire lock on the short path first
      if (srcComponents.length <= dstComponents.length) {
        acquireInodeLocks(locks.getInodeParam()[0]);
        acquireInodeLocks(locks.getInodeParam()[1]);
      } else {
        acquireInodeLocks(locks.getInodeParam()[1]);
        acquireInodeLocks(locks.getInodeParam()[0]);
      }

      if (allowExistingDir) // In deprecated rename, it allows to move a dir to an existing destination.
      {
        LinkedList<INode> dstINodes = acquireInodeLockByPath(locks, locks.getInodeParam()[1]); // reads from snapshot.  
        if (dstINodes.size() == dstComponents.length && dstINodes.getLast().isDirectory()) {
          //the dst exist and is a directory.
          INode existingInode = acquireINodeLockByNameAndParentId(
                  locks.getInodeLock(),
                  DFSUtil.bytes2String(srcComponents[srcComponents.length - 1]),
                  dstINodes.getLast().getId(), locks);
//        inodeResult = new INode[inodeResult1.length + inodeResult2.length + 1];
//        if (existingInode != null & !existingInode.isDirectory()) {
//          inodeResult[inodeResult.length - 1] = existingInode;
//        }
        }
      }
    }

    if (locks.getBlockLock() != null) {
      if (locks.getInodeLock() != null && locks.getBlockParam() != null) {
        throw new RuntimeException("Acquiring locks on block-infos using inode-id and block-id concurrently is not allowed!");
      }
      blockResults.addAll(acquireBlockLock());
    }

    acquireLeaseAndLpathLockNormal();
    acquireBlockRelatedLocksNormal();
    readINodeAttributes();
    return locks;
  }

  private LinkedList<Lease> acquireLeaseLock() throws PersistanceException {

    checkStringParam(locks.getLeaseParam());
    SortedSet<String> holders = new TreeSet<String>();
    if (locks.getLeaseParam() != null) {
      holders.add((String) locks.getLeaseParam());
    }

    for (LinkedList<INode> resolvedINodes : allResolvedINodes) {
      for (INode f : resolvedINodes) {
        if (f instanceof INodeFileUnderConstruction) {
          holders.add(((INodeFileUnderConstruction) f).getClientName());
        }
      }
    }

    LinkedList<Lease> leases = new LinkedList<Lease>();
    for (String h : holders) {
      Lease lease = acquireLock(locks.getLeaseLock(), Lease.Finder.ByPKey, h);
      if (lease != null) {
        leases.add(lease);
      }
    }

    return leases;
  }

  private void checkStringParam(Object param) {
    if (param != null && !(param instanceof String)) {
      throw new IllegalArgumentException("Param is expected to be a String but is " + param.getClass().getName());
    }
  }

  private List<HopLeasePath> acquireLeasePathsLock() throws PersistanceException {
    List<HopLeasePath> lPaths = new LinkedList<HopLeasePath>();
    if (leaseResults != null) {
      for (Lease l : leaseResults) {
        Collection<HopLeasePath> result = acquireLockList(locks.getLpLock(), HopLeasePath.Finder.ByHolderId, l.getHolderID());
        if (!l.getHolder().equals(HdfsServerConstants.NAMENODE_LEASE_HOLDER)) { // We don't need to keep the lps result for namenode-lease. 
          lPaths.addAll(result);
        }
      }
    }

    return lPaths;
  }

  private void acquireReplicasLock(LockType lock, FinderType finder) throws PersistanceException {
    if (blockResults != null && !blockResults.isEmpty()) {
      for (Block b : blockResults) {
        acquireLockList(lock, finder, b.getBlockId());
      }
    } else // if blockResults is null then we can safely bring null in to cache
    {
      if (locks.getBlockParam() != null) {
        acquireLockList(lock, finder, locks.getBlockParam()/*id*/);
      }
    }
  }

  private void acquireBlockRelatedLock(LockType lock, FinderType finder) throws PersistanceException {
    if (blockResults != null && !blockResults.isEmpty()) {
      for (Block b : blockResults) {
        acquireLock(lock, finder, b.getBlockId());
      }
    } else // if blockResults is null then we can safely bring null in to cache
    {
      if (locks.getBlockParam() != null) {
        acquireLock(lock, finder, locks.getBlockParam()/*id*/);
      }
    }
  }

  private LinkedList<INode> findImmediateChildren(INode lastINode) throws PersistanceException {
    LinkedList<INode> children = new LinkedList<INode>();
    if (lastINode != null) {
      if (lastINode instanceof INodeDirectory) {
        lockINode(locks.getInodeLock());
        children.addAll(((INodeDirectory) lastINode).getChildren());
      }
    }
    return children;
  }

  private LinkedList<INode> findChildrenRecursively(INode lastINode) throws PersistanceException {
    LinkedList<INode> children = new LinkedList<INode>();
    LinkedList<INode> unCheckedDirs = new LinkedList<INode>();
    if (lastINode != null) {
      if (lastINode instanceof INodeDirectory) {
        unCheckedDirs.add(lastINode);
      }
    }

    // Find all the children in the sub-directories.
    while (!unCheckedDirs.isEmpty()) {
      INode next = unCheckedDirs.poll();
      if (next instanceof INodeDirectory) {
        lockINode(locks.getInodeLock());
        List<INode> clist = ((INodeDirectory) next).getChildren();
        unCheckedDirs.addAll(clist);
        children.addAll(clist);
      } 
    }
    LOG.debug("Added "+children.size()+" childern.");
    return children;
  }

  private String buildPath(String path, int size) {
    StringBuilder builder = new StringBuilder();
    byte[][] components = INode.getPathComponents(path);

    for (int i = 0; i < Math.min(components.length, size); i++) {
      if (i == 0) {
        builder.append("/");
      } else {
        if (i != 1) {
          builder.append("/");
        }
        builder.append(DFSUtil.bytes2String(components[i]));
      }
    }

    return builder.toString();
  }

  private Lease acquireNameNodeLease() throws PersistanceException {
    if (locks.getNnLeaseLock() != null) {
      return acquireLock(locks.getNnLeaseLock(), Lease.Finder.ByPKey, HdfsServerConstants.NAMENODE_LEASE_HOLDER);
    }
    return null;
  }

  public TransactionLocks acquireLeaderLock() throws PersistanceException {
    if (locks.getLeaderLock() != null) {
      acquireLockList(locks.getLeaderLock(), HopLeader.Finder.All);
    }
    return locks;
  }

  private void acquireLeaseAndLpathLockNormal() throws PersistanceException {
    if (locks.getLeaseLock() != null) {
      leaseResults.addAll(acquireLeaseLock());
    }

    if (locks.getLpLock() != null) {
      acquireLeasePathsLock();
    }
  }

  /**
   * Acquires lock on the lease, lease-path, replicas, excess, corrupt,
   * invalidated, under-replicated and pending blocks.
   *
   * @throws PersistanceException
   */
  private void acquireBlockRelatedLocksNormal() throws PersistanceException {

//HOP if (blockResults != null && !blockResults.isEmpty()) //[S] commented this to bring null in to the cache for invalid/deleted blocks
//    {
    if (locks.getReplicaLock() != null) {
      acquireReplicasLock(locks.getReplicaLock(), IndexedReplica.Finder.ByBlockId);
    }

    if (locks.getCrLock() != null) {
      acquireReplicasLock(locks.getCrLock(), CorruptReplica.Finder.ByBlockId);
    }

    if (locks.getErLock() != null) {
      acquireReplicasLock(locks.getErLock(), ExcessReplica.Finder.ByBlockId);
    }

    if (locks.getRucLock() != null) {
      acquireReplicasLock(locks.getRucLock(), ReplicaUnderConstruction.Finder.ByBlockId);
    }

    if (locks.getInvLocks() != null) {
      acquireReplicasLock(locks.getInvLocks(), InvalidatedBlock.Finder.ByBlockId);
    }

    if (locks.getUrbLock() != null) {
      acquireBlockRelatedLock(locks.getUrbLock(), UnderReplicatedBlock.Finder.ByBlockId);
    }

    if (locks.getPbLock() != null) {
      acquireBlockRelatedLock(locks.getPbLock(), PendingBlockInfo.Finder.ByPKey);
    }
//    }

    if (locks.getBlockKeyLock() != null) {
      if (locks.getBlockKeyIds() != null) {
        for (int id : locks.getBlockKeyIds()) {
          acquireLock(locks.getBlockKeyLock(), BlockKey.Finder.ById, id);
        }
      }
      if (locks.getBlockKeyTypes() != null) {
        for (short type : locks.getBlockKeyTypes()) {
          acquireLock(locks.getBlockKeyLock(), BlockKey.Finder.ByType, type);
        }
      }
    }

    if (locks.getGenerationStampLock() != null) {
      acquireLock(locks.getGenerationStampLock(), Variable.Finder.GenerationStamp);
    }

    if (locks.getBlockIdCounterLock() != null) {
      acquireLock(locks.getBlockIdCounterLock(), Variable.Finder.BlockID);
    }
  }

  private void acquireInodeLocks(String... params) throws UnresolvedPathException, PersistanceException {
    switch (locks.getInodeResolveType()) {
      case PATH: // Only use memcached for this case.
      case PATH_AND_IMMEDIATE_CHILDREN: // Memcached not applicable for delete of a dir (and its children)
      case PATH_AND_ALL_CHILDREN_RECURESIVELY:
        for (int i = 0; i < params.length; i++) {
          LinkedList<INode> resolvedInodes = acquireInodeLockByPath(locks, params[i]);
          if (resolvedInodes.size() > 0) {
            INode lastINode = resolvedInodes.peekLast();
            if (locks.getInodeResolveType() == INodeResolveType.PATH_AND_IMMEDIATE_CHILDREN) {
              resolvedInodes.addAll(findImmediateChildren(lastINode));
            } else if (locks.getInodeResolveType() == INodeResolveType.PATH_AND_ALL_CHILDREN_RECURESIVELY) {
              resolvedInodes.addAll(findChildrenRecursively(lastINode));
            }
          }
          allResolvedINodes.add(resolvedInodes);
        }
        break;
      // e.g. mkdir -d /opt/long/path which creates subdirs.
      // That is, the HEAD and some ancestor inodes might not exist yet.
      case PATH_WITH_UNKNOWN_HEAD: // Can try and use memcached for this case.
        for (int i = 0; i < params.length; i++) {
          String fullPath = params[i];
          checkPathIsResolved();
          int resolvedSize = locks.getPreTxResolvedInodes().size();
          String existingPath = buildPath(fullPath, resolvedSize);
          acquireInodeLocksByPreTxResolvedIDs(locks);
          INode baseDir = locks.getPreTxResolvedInodes().peekLast();
          LinkedList<INode> rest = acquireLockOnRestOfPath(locks.getInodeLock(), baseDir,
                  fullPath, existingPath, locks.isResolveLink());
          locks.getPreTxResolvedInodes().addAll(rest);
          allResolvedINodes.add(locks.getPreTxResolvedInodes());
        }
        break;

      default:
        throw new IllegalArgumentException("Unknown type " + locks.getInodeLock().name());
    }
  }

  private void checkPathIsResolved() throws INodeResolveException {
    if (locks.getPreTxResolvedInodes() == null) {
      throw new INodeResolveException(String.format(
              "Requires to have inode-id(s) in order to do this operation. "
              + "ResolvedInodes is null."));
    }
  }

  private LinkedList<BlockInfo> acquireBlockLock() throws PersistanceException {

    LinkedList<BlockInfo> blocks = new LinkedList<BlockInfo>();

    if (locks.getBlockParam() != null) {
      long bid = (Long) locks.getBlockParam();

      BlockInfo result = acquireLock(locks.getBlockLock(), BlockInfo.Finder.ById, bid);
      if (result != null) {
        blocks.add(result);
      }
    } else {
      for (LinkedList<INode> resolvedINodes : allResolvedINodes) {
        for (INode inode : resolvedINodes) {
          if (inode instanceof INodeFile) {
            blocks.addAll(acquireLockList(locks.getBlockLock(), BlockInfo.Finder.ByInodeId, inode.getId()));
          }
        }
      }
    }

    // sort the blocks. it is important as the ndb returns the blocks in random order and two
    // txs trying to take locks on the blocks of a file will end up in dead lock 
    Collections.sort(blocks, BlockInfo.Order.ByBlockId);

    return blocks;
  }

  private INode takeLocksFromRootToLeaf(LinkedList<INode> inodes, INodeLockType inodeLock) throws PersistanceException {

    StringBuilder msg = new StringBuilder();
    msg.append("Took Lock on the entire path ");
    INode lockedLeafINode = null;
    for (int i = 0; i < inodes.size(); i++) {
      if (i == (inodes.size() - 1)) // take specified lock
      {
        lockedLeafINode = acquireINodeLockById(inodeLock, inodes.get(i).getId(), locks);
      } else // take read commited lock
      {
        lockedLeafINode = acquireINodeLockById(INodeLockType.READ_COMMITED, inodes.get(i).getId(), locks);
      }

      if (!lockedLeafINode.getLocalName().equals("")) {
        msg.append("/");
        msg.append(lockedLeafINode.getLocalName());
      }
    }
    LOG.debug(msg.toString());
    return lockedLeafINode;
  }

  //TransacationLockAcquirer Code
  private static <T> Collection<T> acquireLockList(LockType lock, FinderType<T> finder, Object... param) throws PersistanceException {
    setLockMode(lock);
    if (param == null) {
      return EntityManager.findList(finder);
    } else {
      return EntityManager.findList(finder, param);
    }
  }

  private static <T> T acquireLock(LockType lock, FinderType<T> finder, Object... param) throws PersistanceException {
    setLockMode(lock);
    if (param == null) {
      return null;
    }
    return EntityManager.find(finder, param);
  }

  private static LinkedList<INode> acquireLockOnRestOfPath(INodeLockType lock, INode baseInode,
          String fullPath, String prefix, boolean resolveLink) throws PersistanceException, UnresolvedPathException {
    LinkedList<INode> resolved = new LinkedList<INode>();
    byte[][] fullComps = INode.getPathComponents(fullPath);
    byte[][] prefixComps = INode.getPathComponents(prefix);
    int[] count = new int[]{prefixComps.length - 1};
    boolean lastComp;
    lockINode(lock);
    INode[] curInode = new INode[]{baseInode};
    while (count[0] < fullComps.length && curInode[0] != null) {
      lastComp = INodeUtil.getNextChild(
              curInode,
              fullComps,
              count,
              resolved,
              resolveLink,
              true);
      if (lastComp) {
        break;
      }
    }

    return resolved;
  }

  private static LinkedList<INode> acquireInodeLockByPath(TransactionLocks locks, String path) throws UnresolvedPathException, PersistanceException {
    LinkedList<INode> resolvedInodes = new LinkedList<INode>();

    if (path == null) {
      return resolvedInodes;
    }

    byte[][] components = INode.getPathComponents(path);
    INode[] curNode = new INode[1];

    int[] count = new int[]{0};
    boolean lastComp = (count[0] == components.length - 1);
    if (lastComp) // if root is the last directory, we should acquire the write lock over the root
    {
      resolvedInodes.add(acquireLockOnRoot(locks.getInodeLock(), locks));
      return resolvedInodes;
    } else if ((count[0] == components.length - 2) && locks.getInodeLock() == INodeLockType.WRITE_ON_PARENT) // if Root is the parent
    {
      curNode[0] = acquireLockOnRoot(locks.getInodeLock(), locks);
    } else {
      curNode[0] = acquireLockOnRoot(INodeLockType.READ_COMMITED, locks);
    }
    resolvedInodes.add(curNode[0]);
    
    while (count[0] < components.length && curNode[0] != null) {

      INodeLockType curInodeLock = null;
      // TODO - memcached - primary key lookup for the row.
      if (((locks.getInodeLock() == INodeLockType.WRITE || locks.getInodeLock() == INodeLockType.WRITE_ON_PARENT) && (count[0] + 1 == components.length - 1))
              || (locks.getInodeLock() == INodeLockType.WRITE_ON_PARENT && (count[0] + 1 == components.length - 2))) {
        EntityManager.writeLock(); // if the next p-component is the last one or is the parent (in case of write on parent), acquire the write lock
        curInodeLock = INodeLockType.WRITE;
      } else if (locks.getInodeLock() == INodeLockType.READ_COMMITED) {
        EntityManager.readCommited();
        curInodeLock = INodeLockType.READ_COMMITED;
      } else {
        EntityManager.readLock();
        curInodeLock = INodeLockType.READ;
      }

      lastComp = INodeUtil.getNextChild(
              curNode,
              components,
              count,
              resolvedInodes,
              locks.isResolveLink(),
              true);
      if (curNode[0] != null) {
        locks.addLockedINodes(curNode[0], curInodeLock);
      }
      if (lastComp) {
        break;
      }
    }

    // TODO - put invalidated cache values in memcached.

    return resolvedInodes;
  }

  // TODO - use this method when there's a hit in memcached
  // Jude's verification function
  private static INode acquireINodeLockById(INodeLockType lock, long id, TransactionLocks locks) throws PersistanceException {
    lockINode(lock);
    INode inode = EntityManager.find(INode.Finder.ByPKey, id);
    locks.addLockedINodes(inode, lock);
    return inode;
  }

  private static INode acquireINodeLockByNameAndParentId(
          INodeLockType lock,
          String name,
          long parentId,
          TransactionLocks locks)
          throws PersistanceException {
    lockINode(lock);
    INode inode = EntityManager.find(INode.Finder.ByNameAndParentId, name, parentId);
    locks.addLockedINodes(inode, lock);
    return inode;
  }

  private static void lockINode(INodeLockType lock) {
    switch (lock) {
      case WRITE:
      case WRITE_ON_PARENT:
        EntityManager.writeLock();
        break;
      case READ:
        EntityManager.readLock();
        break;
      case READ_COMMITED:
        EntityManager.readCommited();
        break;
    }
  }

  private static INode acquireLockOnRoot(INodeLockType lock, TransactionLocks locks) throws PersistanceException {

    lockINode(lock);
    INode inode = EntityManager.find(INode.Finder.ByPKey, 0L);
    LOG.debug("Acquired " + lock + " on the root node");
    locks.addLockedINodes(inode, lock);
    return inode;
  }

  private static void setLockMode(LockType mode) {
    switch (mode) {
      case WRITE:
        EntityManager.writeLock();
        break;
      case READ:
        EntityManager.readLock();
        break;
      case READ_COMMITTED:
        EntityManager.readCommited();
        break;
    }
  }

  //if path is already resolved then take locks based on primarny keys
  private static void acquireInodeLocksByPreTxResolvedIDs(TransactionLocks locks) throws PersistanceException {
    LinkedList<INode> resolvedInodes = locks.getPreTxResolvedInodes();
    int palthLength = resolvedInodes.size();
    int count = 0;
    boolean lastComp = (count == palthLength - 1);

    if (lastComp) { // if root is the last directory, we should acquire the write lock over the root
      acquireLockOnRoot(locks.getInodeLock(), locks);
      return;
    }

    boolean canTakeParentLock = locks.isPreTxPathFullyResolved(); //if the path is not fully resolved then there is no point in taking strong lock on the penultimate inode
    while (count < palthLength) {
      if ( // take write lock on the element if needed
              ((count == (palthLength - 1)) && (locks.getInodeLock() == INodeLockType.WRITE || locks.getInodeLock() == INodeLockType.WRITE_ON_PARENT))
              || ((count == (palthLength - 2)) && (locks.getInodeLock() == INodeLockType.WRITE_ON_PARENT) && canTakeParentLock)) {
        acquireINodeLockById(INodeLockType.WRITE, resolvedInodes.get(count).getId(), locks);
      } else if (locks.getInodeLock() == INodeLockType.READ_COMMITED) {
        acquireINodeLockById(INodeLockType.READ_COMMITED, resolvedInodes.get(count).getId(), locks);
      } else {
        acquireINodeLockById(INodeLockType.READ, resolvedInodes.get(count).getId(), locks);
      }

      lastComp = (count == (palthLength - 1));
      count++;
      if (lastComp) {
        break;
      }
    }
  }

  private void readINodeAttributes() throws PersistanceException {
    for (LinkedList<INode> resolvedINodes : allResolvedINodes) {
      for (INode inode : resolvedINodes) {
        if (inode instanceof INodeDirectoryWithQuota) {
          acquireLock(LockType.READ_COMMITTED, INodeAttributes.Finder.ByPKey, inode.getId());
        }
      }
    }   
  }
}
