package se.sics.hop.metadata.context;

import se.sics.hop.metadata.hdfs.entity.EntityContext;
import java.util.ArrayList;
import se.sics.hop.metadata.hdfs.dal.ReplicaUnderConstructionDataAccess;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.EntityContextStat;
import se.sics.hop.metadata.hdfs.entity.TransactionContextMaintenanceCmds;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINodeCandidatePK;
import se.sics.hop.transaction.lock.TransactionLocks;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ReplicaUnderConstructionContext extends EntityContext<ReplicaUnderConstruction> {

  /**
   * Mappings
   */
  private Map<ReplicaUnderConstruction, ReplicaUnderConstruction> newReplicasUc = new HashMap<ReplicaUnderConstruction, ReplicaUnderConstruction>();
  private Map<ReplicaUnderConstruction, ReplicaUnderConstruction> removedReplicasUc = new HashMap<ReplicaUnderConstruction, ReplicaUnderConstruction>();
  private Map<Long, List<ReplicaUnderConstruction>> blockReplicasUCAll = new HashMap<Long, List<ReplicaUnderConstruction>>();
  private Set<Integer> inodesRead = new HashSet<Integer>();
  private ReplicaUnderConstructionDataAccess<ReplicaUnderConstruction> dataAccess;

  public ReplicaUnderConstructionContext(ReplicaUnderConstructionDataAccess<ReplicaUnderConstruction> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(ReplicaUnderConstruction replica) throws PersistanceException {
    if (removedReplicasUc.containsKey(replica)) {
      throw new TransactionContextException("Removed  under constructionreplica passed to be persisted");
    }

    newReplicasUc.put(replica, replica);
    if (blockReplicasUCAll.get(replica.getBlockId()) == null) {
      blockReplicasUCAll.put(replica.getBlockId(), new ArrayList<ReplicaUnderConstruction>());
    }
    blockReplicasUCAll.get(replica.getBlockId()).add(replica);

    log("added-replicauc", CacheHitState.NA,
            new String[]{"bid", Long.toString(replica.getBlockId()),
      "sid", Integer.toString(replica.getStorageId()), "state", replica.getState().name()});
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    newReplicasUc.clear();
    removedReplicasUc.clear();
    blockReplicasUCAll.clear();
    inodesRead.clear();
  }

  @Override
  public int count(CounterType counter, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public List<ReplicaUnderConstruction> findList(FinderType<ReplicaUnderConstruction> finder, Object... params) throws PersistanceException {

    ReplicaUnderConstruction.Finder rFinder = (ReplicaUnderConstruction.Finder) finder;
    List<ReplicaUnderConstruction> result = null;
    switch (rFinder) {
      case ByBlockId:
        long blockId = (Long) params[0];
        Integer partKey = (Integer) params[1];
        Integer inodeId = (Integer) params[2];
        if (blockReplicasUCAll.containsKey(blockId)) {
          log("find-replicaucs-by-bid", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId),"part_key", partKey!=null?Integer.toString(partKey):"NULL"});
          result = blockReplicasUCAll.get(blockId);
        } else if (inodesRead.contains(inodeId) /*|| inodeId == INode.NON_EXISTING_ID*/){
          return null;
        }
        else {
          log("find-replicaucs-by-bid", CacheHitState.LOSS, new String[]{"bid", Long.toString(blockId),"part_key", partKey!=null?Integer.toString(partKey):"NULL"});
          aboutToAccessStorage();
          result = dataAccess.findReplicaUnderConstructionByBlockId(blockId, partKey);
          blockReplicasUCAll.put(blockId, result);
        }
        break;
     case ByINodeId:
        inodeId = (Integer) params[0];
        partKey = (Integer) params[1];
        
        if(inodesRead.contains(inodeId)){
          log("find-replicaucs-by-inode-id", CacheHitState.HIT, new String[]{"inode_id", Integer.toString(inodeId),"part_key", partKey!=null?Integer.toString(partKey):"NULL"});
          return getReplicasUnderConstructionForFile(inodeId);
        }else{
          log("find-replicaucs-by-inode-id", CacheHitState.LOSS, new String[]{"inode_id", Integer.toString(inodeId),"part_key", partKey!=null?Integer.toString(partKey):"NULL"});
          aboutToAccessStorage();
          result = dataAccess.findReplicaUnderConstructionByINodeId(inodeId, partKey);
          inodesRead.add(inodeId);
          if(result != null){
            saveLists(result);
          }
          return result;
        }       
    }

    return result;
  }

  private List<ReplicaUnderConstruction> getReplicasUnderConstructionForFile(int inodeId){
    List<ReplicaUnderConstruction> tmp = new ArrayList<ReplicaUnderConstruction>();
    for(Long blockId : blockReplicasUCAll.keySet()){
      List<ReplicaUnderConstruction> blockReplicas = blockReplicasUCAll.get(blockId);
      for(ReplicaUnderConstruction replica : blockReplicas){
        if(replica.getInodeId() == inodeId){
          tmp.add(replica);
        }
      }
    }
    return tmp;
  } 
  
  private void saveLists(List<ReplicaUnderConstruction> list){
    for(ReplicaUnderConstruction replica : list){
      List<ReplicaUnderConstruction> blockReplicas = blockReplicasUCAll.get(replica.getBlockId());
      if(blockReplicas == null){
        blockReplicas = new ArrayList<ReplicaUnderConstruction>();
      }
      blockReplicas.add(replica);
      blockReplicasUCAll.put(replica.getBlockId(), blockReplicas);
    }
  }
  
  @Override
  public ReplicaUnderConstruction find(FinderType<ReplicaUnderConstruction> finder, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
  }

  @Override
  public void prepare(TransactionLocks lks) throws StorageException {
    // if the list is not empty then check for the lock types
    // lock type is checked after when list lenght is checked 
    // because some times in the tx handler the acquire lock 
    // function is empty and in that case tlm will throw 
    // null pointer exceptions
//    HDFSTransactionLocks hlks = (HDFSTransactionLocks)lks;
//    if ((!removedReplicasUc.values().isEmpty())
//            && hlks.getRucLock() != TransactionLockTypes.LockType.WRITE) {
//      throw new LockUpgradeException("Trying to upgrade replica under construction locks");
//    }
    dataAccess.prepare(removedReplicasUc.values(), newReplicasUc.values(), null);
  }

  @Override
  public void remove(ReplicaUnderConstruction replica) throws PersistanceException {

    boolean removed = false;
    if (blockReplicasUCAll.containsKey(replica.getBlockId())) {
      List<ReplicaUnderConstruction> urbs = blockReplicasUCAll.get(replica.getBlockId());
      if (urbs.contains(replica)) {
        removedReplicasUc.put(replica, replica);
        blockReplicasUCAll.remove(replica);
        removed = true;
      }
    }
    if (!removed) {

      throw new StorageException("Trying to delete row in ruc table that was not locked. ruc bid " + replica.getBlockId()
              + " sid " + replica.getStorageId());
    }
    newReplicasUc.remove(replica);
    log("removed-replicauc", CacheHitState.NA,
            new String[]{"bid", Long.toString(replica.getBlockId()),
      "sid", Integer.toString(replica.getStorageId()), "state", replica.getState().name(),
      " replicas to be removed", Integer.toString(removedReplicasUc.size()),
      "Storage id", Integer.toString(replica.getStorageId())});
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
  }

  @Override
  public void update(ReplicaUnderConstruction replica) throws PersistanceException {
    throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
  }
  
  @Override
  public EntityContextStat collectSnapshotStat() throws PersistanceException {
    EntityContextStat stat = new EntityContextStat("Replicas Under Construction",newReplicasUc.size(),0,removedReplicasUc.size());
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
        if (inodeBeforeChange.getLocalName().equals(inodeAfterChange.getLocalName()) ==  false){
          log("snapshot-maintenance-replicauc-pk-change", CacheHitState.NA, new String[]{"Before inodeId", Integer.toString(inodeBeforeChange.getId()), "name", inodeBeforeChange.getLocalName(), "pid", Integer.toString(inodeBeforeChange.getParentId()),"After inodeId", Integer.toString(inodeAfterChange.getId()), "name", inodeAfterChange.getLocalName(), "pid", Integer.toString(inodeAfterChange.getParentId()) });
          List<HopINodeCandidatePK> deletedINodesPK = new ArrayList<HopINodeCandidatePK>();
          deletedINodesPK.add(new HopINodeCandidatePK(inodeBeforeChange.getId()));
          updateReplicaUCs(new HopINodeCandidatePK(inodeAfterChange.getId()), deletedINodesPK);
        }
        break;
      case Concat:
        checkForSnapshotChange();
        HopINodeCandidatePK trg_param = (HopINodeCandidatePK)params[0];
        List<HopINodeCandidatePK> srcs_param = (List<HopINodeCandidatePK>)params[1];
        List<BlockInfo> oldBlks  = (List<BlockInfo>)params[2];
        updateReplicaUCs(trg_param, srcs_param);
        break;
    }
  }
  
  private void checkForSnapshotChange(){
     if (newReplicasUc.size() != 0 || removedReplicasUc.size() != 0 ) // during the tx no replica should have been changed
        {
          throw new IllegalStateException("No replica under construction row should have been changed during the Tx");
        }
  }
  
  private void updateReplicaUCs(HopINodeCandidatePK trg_param, List<HopINodeCandidatePK> toBeDeletedSrcs){
    
    for(List<ReplicaUnderConstruction> replicasUC : blockReplicasUCAll.values()){
      for(ReplicaUnderConstruction replicaUC : replicasUC){
        HopINodeCandidatePK pk = new HopINodeCandidatePK(replicaUC.getInodeId());
        if(!trg_param.equals(pk) && toBeDeletedSrcs.contains(pk)){
          ReplicaUnderConstruction toBeDeleted = cloneReplicaUCObj(replicaUC);
          ReplicaUnderConstruction toBeAdded = cloneReplicaUCObj(replicaUC);
          
          removedReplicasUc.put(toBeDeleted, toBeDeleted);
          log("snapshot-maintenance-removed-replicauc",CacheHitState.NA, new String[]{"bid", Long.toString(toBeDeleted.getBlockId()),"inodeId", Integer.toString(toBeDeleted.getInodeId())});
          //both inode id and partKey has changed
          toBeAdded.setInodeId(trg_param.getInodeId());
          newReplicasUc.put(toBeAdded, toBeAdded);
          log("snapshot-maintenance-added-replicauc",CacheHitState.NA, new String[]{"bid", Long.toString(toBeAdded.getBlockId()),"inodeId", Integer.toString(toBeAdded.getInodeId())});
        }
      }
    }
  }
  
  private ReplicaUnderConstruction cloneReplicaUCObj(ReplicaUnderConstruction src){
    return new ReplicaUnderConstruction(src.getState(),src.getStorageId(),src.getBlockId(),src.getInodeId(),src.getPartKey(),src.getIndex());
  }
}
