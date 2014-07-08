package se.sics.hop.metadata.context;

import se.sics.hop.metadata.hdfs.entity.EntityContext;
import java.util.*;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.dal.PendingBlockDataAccess;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.EntityContextStat;
import se.sics.hop.metadata.hdfs.entity.TransactionContextMaintenanceCmds;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINodeCandidatePK;
import se.sics.hop.transaction.lock.TransactionLocks;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class PendingBlockContext extends EntityContext<PendingBlockInfo> {

  private Map<Long, PendingBlockInfo> pendings = new HashMap<Long, PendingBlockInfo>();
  private Map<Long, PendingBlockInfo> newPendings = new HashMap<Long, PendingBlockInfo>();
  private Map<Long, PendingBlockInfo> modifiedPendings = new HashMap<Long, PendingBlockInfo>();
  private Map<Long, PendingBlockInfo> removedPendings = new HashMap<Long, PendingBlockInfo>();
  private Set<Integer> inodesRead = new HashSet<Integer>();
  private boolean allPendingRead = false;
  private PendingBlockDataAccess<PendingBlockInfo> dataAccess;

  public PendingBlockContext(PendingBlockDataAccess<PendingBlockInfo>  dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(PendingBlockInfo pendingBlock) throws PersistanceException {
    if (removedPendings.containsKey(pendingBlock.getBlockId()) || modifiedPendings.containsKey(pendingBlock.getBlockId())) {
      throw new TransactionContextException("Removed/Modified pending-block passed to be persisted");
    }

    pendings.put(pendingBlock.getBlockId(), pendingBlock);
    newPendings.put(pendingBlock.getBlockId(), pendingBlock);
    log("added-pending", CacheHitState.NA,
            new String[]{"bid", Long.toString(pendingBlock.getBlockId()),
              "numInProgress", Integer.toString(pendingBlock.getNumReplicas())});
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    pendings.clear();
    newPendings.clear();
    modifiedPendings.clear();
    removedPendings.clear();
    inodesRead.clear();
    allPendingRead = false;
  }

  @Override
  public int count(CounterType counter, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public List<PendingBlockInfo> findList(FinderType<PendingBlockInfo> finder, Object... params) throws PersistanceException {
    PendingBlockInfo.Finder pFinder = (PendingBlockInfo.Finder) finder;
    List<PendingBlockInfo> result = null;
    switch (pFinder) {
      case ByTimeLimit:
        long timeLimit = (Long) params[0];
        log("find-pendings-by-timelimit", CacheHitState.NA, new String[]{"timelimit", Long.toString(timeLimit)});
        aboutToAccessStorage();
        return syncInstances(dataAccess.findByTimeLimitLessThan(timeLimit));
      case All:
        if (allPendingRead) {
          log("find-all-pendings", CacheHitState.HIT);
        } else {
          log("find-all-pendings", CacheHitState.LOSS);
          aboutToAccessStorage();
          syncInstances(dataAccess.findAll());
          allPendingRead = true;
        }
        result = new ArrayList();
        for (PendingBlockInfo pendingBlockInfo : pendings.values()) {
          if (pendingBlockInfo != null) {
            result.add(pendingBlockInfo);
          }
        }
        return result;
      case ByInodeId:
        Integer inodeId = (Integer) params[0];
        if(inodesRead.contains(inodeId)){
          log("find-pendings-by-inode-id", CacheHitState.HIT, new String[]{"inode_id", Integer.toString(inodeId)});
          return getPendingReplicasForINode(inodeId);
        }else{
          log("find-pendings-by-inode-id", CacheHitState.LOSS, new String[]{"inode_id", Integer.toString(inodeId)});
          aboutToAccessStorage();
          result = dataAccess.findByINodeId(inodeId);
          inodesRead.add(inodeId);
          if(result != null){
            saveLists(result);
          }
          return result;
        }       
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public PendingBlockInfo find(FinderType<PendingBlockInfo> finder, Object... params) throws PersistanceException {
    PendingBlockInfo.Finder pFinder = (PendingBlockInfo.Finder) finder;
    PendingBlockInfo result = null;
    switch (pFinder) {
      case ByBlockId:
        long blockId = (Long) params[0];
        Integer inodeId = (Integer) params[1];
        if (this.pendings.containsKey(blockId)) {
          log("find-pending-by-pk", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId),"inode_id", Integer.toString(inodeId)});
          result = this.pendings.get(blockId);
        } else if (inodesRead.contains(inodeId) /*|| inodeId == INode.NON_EXISTING_ID*/){
          return null;
        }
        else if (!this.removedPendings.containsKey(blockId)) {
          log("find-pending-by-pk", CacheHitState.LOSS, new String[]{"bid", Long.toString(blockId),"inode_id", Integer.toString(inodeId)});
          aboutToAccessStorage();
          result = dataAccess.findByPKey(blockId,inodeId);
          this.pendings.put(blockId, result);
        } 
        return result;
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }
  
  private List<PendingBlockInfo> getPendingReplicasForINode(int inodeId){
    List<PendingBlockInfo>  list = new ArrayList<PendingBlockInfo>();
    for(PendingBlockInfo pbi: pendings.values()){
      if(pbi.getInodeId() == inodeId){
        list.add(pbi);
      }
    }
    return list;
  }
  
   private void saveLists(List<PendingBlockInfo> list){
     for(PendingBlockInfo pbi : list){
       pendings.put(pbi.getBlockId(), pbi);
     }
  }

    @Override
    public void prepare(TransactionLocks lks) throws StorageException {
        // if the list is not empty then check for the lock types
        // lock type is checked after when list lenght is checked 
        // because some times in the tx handler the acquire lock 
        // function is empty and in that case tlm will throw 
        // null pointer exceptions
//        HDFSTransactionLocks hlks = (HDFSTransactionLocks)lks;
//        if ((!removedPendings.values().isEmpty()
//                || !modifiedPendings.values().isEmpty())
//                && hlks.getPbLock()!= TransactionLockTypes.LockType.WRITE) {
//            throw new LockUpgradeException("Trying to upgrade pending block locks");
//        }
        dataAccess.prepare(removedPendings.values(), newPendings.values(), modifiedPendings.values());
    }

  @Override
  public void remove(PendingBlockInfo pendingBlock) throws PersistanceException {
    if (!pendings.containsKey(pendingBlock.getBlockId())) {  
      throw new TransactionContextException("Unattached pending-block passed to be removed id "+pendingBlock.getBlockId());
    }
    pendings.remove(pendingBlock.getBlockId());
    newPendings.remove(pendingBlock.getBlockId());
    modifiedPendings.remove(pendingBlock.getBlockId());
    removedPendings.put(pendingBlock.getBlockId(), pendingBlock);
    log("removed-pending", CacheHitState.NA, new String[]{"bid", Long.toString(pendingBlock.getBlockId())});
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(PendingBlockInfo pendingBlock) throws PersistanceException {
    if (removedPendings.containsKey(pendingBlock.getBlockId())) {
      throw new TransactionContextException("Removed pending-block passed to be persisted");
    }

    pendings.put(pendingBlock.getBlockId(), pendingBlock);
    if(newPendings.containsKey(pendingBlock.getBlockId())){
      newPendings.put(pendingBlock.getBlockId(), pendingBlock);
    }
    else{
      modifiedPendings.put(pendingBlock.getBlockId(), pendingBlock);
    }
    
    log("updated-pending", CacheHitState.NA,
            new String[]{"bid", Long.toString(pendingBlock.getBlockId()),
              "numInProgress", Integer.toString(pendingBlock.getNumReplicas())});
  }

  /**
   * This method only returns the result fetched from the storage not those in
   * the memory.
   *
   * @param pendingTables
   * @return
   */
  private List<PendingBlockInfo> syncInstances(Collection<PendingBlockInfo> pendingTables) {
    List<PendingBlockInfo> newPBlocks = new ArrayList<PendingBlockInfo>();
    for (PendingBlockInfo p : pendingTables) {
      if (pendings.containsKey(p.getBlockId())) {
        if (pendings.get(p.getBlockId()) == null) {
          pendings.put(p.getBlockId(), p);
        }
        newPBlocks.add(pendings.get(p.getBlockId()));
      } else if (!removedPendings.containsKey(p.getBlockId())) {
        pendings.put(p.getBlockId(), p);
        newPBlocks.add(p);
      }
    }

    return newPBlocks;
  }
  
  @Override
  public EntityContextStat collectSnapshotStat() throws PersistanceException {
    EntityContextStat stat = new EntityContextStat("Pending Blocks",newPendings.size(),modifiedPendings.size(),removedPendings.size());
    return stat;
  }

  @Override
  public void snapshotMaintenance(TransactionContextMaintenanceCmds cmds, Object... params) throws PersistanceException {
    HOPTransactionContextMaintenanceCmds hopCmds = (HOPTransactionContextMaintenanceCmds) cmds;
    switch (hopCmds) {
      case INodePKChanged:
          // need to update the rows with updated inodeId or partKey
        checkForSnapshotChange();        
        INode inodeBeforeChange = (INode) params[0];
        INode inodeAfterChange  = (INode) params[1];
        break;
      case Concat:
        checkForSnapshotChange();
        HopINodeCandidatePK trg_param = (HopINodeCandidatePK)params[0];
        List<HopINodeCandidatePK> srcs_param = (List<HopINodeCandidatePK>)params[1];
        List<BlockInfo> oldBlks  = (List<BlockInfo>)params[2];
        updatePendingReplicas(trg_param, srcs_param);
        break;
    }
  }
  
  private void checkForSnapshotChange(){
     if (!newPendings.isEmpty() || !removedPendings.isEmpty() || !modifiedPendings.isEmpty()) // during the tx no replica should have been changed
        {
          throw new IllegalStateException("No pending replicas row should have been changed during the Tx");
        }
  }
  
  private void updatePendingReplicas(HopINodeCandidatePK trg_param, List<HopINodeCandidatePK> toBeDeletedSrcs){
    
    
      for(PendingBlockInfo pending : pendings.values()){
        HopINodeCandidatePK pk = new HopINodeCandidatePK(pending.getInodeId());
        if(!trg_param.equals(pk) && toBeDeletedSrcs.contains(pk)){
          PendingBlockInfo toBeDeleted = clonePendingReplicaObj(pending);
          PendingBlockInfo toBeAdded = clonePendingReplicaObj(pending);
          
          removedPendings.put(toBeDeleted.getBlockId(), toBeDeleted);
          log("snapshot-maintenance-removed-pending",CacheHitState.NA, new String[]{"bid", Long.toString(toBeDeleted.getBlockId()),"inodeId", Integer.toString(toBeDeleted.getInodeId())});
          
          //both inode id and partKey has changed
          toBeAdded.setInodeId(trg_param.getInodeId());
          newPendings.put(toBeAdded.getBlockId(), toBeAdded);
          log("snapshot-maintenance-added-pending",CacheHitState.NA, new String[]{"bid", Long.toString(toBeAdded.getBlockId()),"inodeId", Integer.toString(toBeAdded.getInodeId())});
        }
      }
    
  }
  
  private PendingBlockInfo clonePendingReplicaObj(PendingBlockInfo src){
    return new PendingBlockInfo(src.getBlockId(),src.getInodeId(),src.getTimeStamp(),src.getNumReplicas());
  }
}
