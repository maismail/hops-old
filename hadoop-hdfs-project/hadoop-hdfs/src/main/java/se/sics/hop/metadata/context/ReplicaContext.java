package se.sics.hop.metadata.context;

import se.sics.hop.metadata.hdfs.entity.EntityContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.metadata.hdfs.entity.hop.HopIndexedReplica;
import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.dal.ReplicaDataAccess;
import se.sics.hop.exception.StorageException;
import static se.sics.hop.metadata.hdfs.entity.EntityContext.log;
import se.sics.hop.metadata.hdfs.entity.EntityContextStat;
import se.sics.hop.metadata.hdfs.entity.TransactionContextMaintenanceCmds;
import se.sics.hop.transaction.lock.TransactionLocks;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ReplicaContext extends EntityContext<HopIndexedReplica> {

  /**
   * Mappings
   */
  private Map<HopIndexedReplica, HopIndexedReplica> removedReplicas = new HashMap<HopIndexedReplica, HopIndexedReplica>();
  private Map<HopIndexedReplica, HopIndexedReplica> newReplicas = new HashMap<HopIndexedReplica, HopIndexedReplica>();
  private Map<HopIndexedReplica, HopIndexedReplica> modifiedReplicas = new HashMap<HopIndexedReplica, HopIndexedReplica>();
  private Map<Long, List<HopIndexedReplica>> blocksReplicas = new HashMap<Long, List<HopIndexedReplica>>();
  private Set<Integer> inodesRead = new HashSet<Integer>();
  private ReplicaDataAccess dataAccess;

  public ReplicaContext(ReplicaDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(HopIndexedReplica replica) throws PersistanceException {
    if (removedReplicas.containsKey(replica)) {
      throw new TransactionContextException("Removed replica passed to be persisted");
    }

    newReplicas.put(replica, replica);
    
    List<HopIndexedReplica> list = blocksReplicas.get(replica.getBlockId());
    if(list == null){
      list = new ArrayList<HopIndexedReplica>();
    }list.add(replica);
    blocksReplicas.put(replica.getBlockId(), list);
    
    log("added-replica", CacheHitState.NA,
            new String[]{"bid", Long.toString(replica.getBlockId()),
      "sid", Integer.toString(replica.getStorageId()), "index", Integer.toString(replica.getIndex())});
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    newReplicas.clear();
    modifiedReplicas.clear();
    removedReplicas.clear();
    blocksReplicas.clear();
    inodesRead.clear();
  }

  @Override
  public int count(CounterType counter, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

    @Override
    public void prepare(TransactionLocks lks) throws StorageException {
        // if the list is not empty then check for the lock types
        // lock type is checked after when list lenght is checked 
        // because some times in the tx handler the acquire lock 
        // function is empty and in that case tlm will throw 
        // null pointer exceptions
//        HDFSTransactionLocks hlks = (HDFSTransactionLocks)lks;
//        if ((!removedReplicas.values().isEmpty()
//                || !modifiedReplicas.values().isEmpty())
//                && hlks.getReplicaLock() != TransactionLockTypes.LockType.WRITE) {
//            throw new LockUpgradeException("Trying to upgrade replica locks");
//        }
      
      log("prepare-replica", CacheHitState.NA, new String[]{"removed size",Integer.toString(removedReplicas.size()),"new Size", Integer.toString(newReplicas.size()), "modified size", Integer.toString(modifiedReplicas.size())});
        dataAccess.prepare(removedReplicas.values(), newReplicas.values(), modifiedReplicas.values());
    }

  @Override
  public void remove(HopIndexedReplica replica) throws PersistanceException {
    modifiedReplicas.remove(replica);
    blocksReplicas.get(replica.getBlockId()).remove(replica);
    if (newReplicas.containsKey(replica)) {
      newReplicas.remove(replica);
    } else {
      removedReplicas.put(replica, replica);
    }
    log("removed-replica", CacheHitState.NA,
            new String[]{"bid", Long.toString(replica.getBlockId()),
      "sid", Integer.toString(replica.getStorageId()), "index", Integer.toString(replica.getIndex())});
  }

  @Override
  public HopIndexedReplica find(FinderType<HopIndexedReplica> finder, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public List<HopIndexedReplica> findList(FinderType<HopIndexedReplica> finder, Object... params) throws PersistanceException {
    HopIndexedReplica.Finder iFinder = (HopIndexedReplica.Finder) finder;
    List<HopIndexedReplica> result = null;
    
    switch (iFinder) {
      case ByBlockId:
        long blockId = (Long) params[0];
        Integer  inodeId = (Integer) params[1];
        Integer  partKey = (Integer) params[2];
        if (blocksReplicas.containsKey(blockId)) {
          log("find-replicas-by-bid", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId),"part_key", partKey!=null?Integer.toString(partKey):"NULL"});
          result = blocksReplicas.get(blockId);
        } else if (inodesRead.contains(inodeId) /*|| inodeId == INode.NON_EXISTING_ID*/){
          return null;
        }
        else {
          log("find-replicas-by-bid", CacheHitState.LOSS, new String[]{"bid", Long.toString(blockId),"part_key", partKey!=null?Integer.toString(partKey):"NULL"});
          aboutToAccessStorage();
          result = dataAccess.findReplicasById(blockId, partKey);
          blocksReplicas.put(blockId, result);
        }
        return new ArrayList<HopIndexedReplica>(result); // Shallow copy
      case ByINodeId:
        inodeId = (Integer) params[0];
        partKey = (Integer) params[1];
        
        if(inodesRead.contains(inodeId)){
          log("find-replicas-by-inode-id", CacheHitState.HIT, new String[]{"inode_id", Integer.toString(inodeId),"part_key", partKey!=null?Integer.toString(partKey):"NULL"});
          return getReplicasForINode(inodeId);
        }else{
          log("find-replicas-by-inode-id", CacheHitState.LOSS, new String[]{"inode_id", Integer.toString(inodeId),"part_key", partKey!=null?Integer.toString(partKey):"NULL"});
          aboutToAccessStorage();
          result = dataAccess.findReplicasByINodeId(inodeId, partKey);
          inodesRead.add(inodeId);
          if(result != null){
            saveLists(result);
          }
          return result;
        }       
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  private List<HopIndexedReplica> getReplicasForINode(int inodeId){
    List<HopIndexedReplica> tmp = new ArrayList<HopIndexedReplica>();
    for(Long blockId : blocksReplicas.keySet()){
      List<HopIndexedReplica> blockReplicas = blocksReplicas.get(blockId);
      for(HopIndexedReplica replica : blockReplicas){
        if(replica.getInodeID() == inodeId){
          tmp.add(replica);
        }
      }
    }
    return tmp;
  } 
  
  private void saveLists(List<HopIndexedReplica> list){
    for(HopIndexedReplica replica : list){
      List<HopIndexedReplica> blockReplicas = blocksReplicas.get(replica.getBlockId());
      if(blockReplicas == null){
        blockReplicas = new ArrayList<HopIndexedReplica>();
      }
      blockReplicas.add(replica);
      blocksReplicas.put(replica.getBlockId(), blockReplicas);
    }
  }
  
  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(HopIndexedReplica replica) throws PersistanceException {
    if (removedReplicas.containsKey(replica)) {
      throw new TransactionContextException("Removed replica passed to be persisted");
    }

    modifiedReplicas.put(replica, replica);
    List<HopIndexedReplica> list = blocksReplicas.get(replica.getBlockId());
    list.remove(replica);
    list.add(replica);
    log("updated-replica", CacheHitState.NA,
            new String[]{"bid", Long.toString(replica.getBlockId()),
      "sid", Integer.toString(replica.getStorageId()), "index", Integer.toString(replica.getIndex())});
  }
  
  @Override
  public EntityContextStat collectSnapshotStat() throws PersistanceException {
    EntityContextStat stat = new EntityContextStat("Replicas",newReplicas.size(),modifiedReplicas.size(),removedReplicas.size());
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
        log("snapshot-maintenance-removed-replicas", CacheHitState.NA, new String[]{"id", Integer.toString(inodeBeforeChange.getId()), "name", inodeBeforeChange.getLocalName(), "pid", Integer.toString(inodeBeforeChange.getParentId()) });
        List<INodePK> deletedINodesPK = new ArrayList<INodePK>();
        deletedINodesPK.add(new INodePK(inodeBeforeChange.getId(), inodeBeforeChange.getPartKey()));
        updateReplicas(new INodePK(inodeAfterChange.getId(), inodeAfterChange.getPartKey()), deletedINodesPK);
        break;
      case Concat:
        checkForSnapshotChange();
        INodePK trg_param = (INodePK)params[0];
        List<INodePK> srcs_param = (List<INodePK>)params[1];
        List<BlockInfo> oldBlks  = (List<BlockInfo>)params[2];
        updateReplicas(trg_param, srcs_param);
        break;
    }
  }
  
  private void checkForSnapshotChange(){
     if (newReplicas.size() != 0 || modifiedReplicas.size() != 0 || removedReplicas.size() != 0) // during the tx no replica should have been changed
        {
          throw new IllegalStateException("No replica should have been changed during the Tx");
        }
  }
  
  private void updateReplicas(INodePK trg_param, List<INodePK> toBeDeletedSrcs){
    
    for(List<HopIndexedReplica> replicas : blocksReplicas.values()){
      for(HopIndexedReplica replica : replicas){
        INodePK pk = new INodePK(replica.getInodeID(), replica.getPartKey());
        if(!trg_param.equals(pk) && toBeDeletedSrcs.contains(pk)){
          HopIndexedReplica toBeDeleted = cloneReplicaObj(replica);
          HopIndexedReplica toBeAdded = cloneReplicaObj(replica);
          
          removedReplicas.put(toBeDeleted, toBeDeleted);
          
          //both inode id and partKey has changed
          toBeAdded.setInodeID(trg_param.id);
          toBeAdded.setPartKey(trg_param.partKey);
          newReplicas.put(toBeAdded, toBeAdded);
        }
      }
    }
  }
  
  private HopIndexedReplica cloneReplicaObj(HopIndexedReplica src){
    return new HopIndexedReplica(src.getBlockId(), src.getStorageId(), src.getInodeID(), src.getPartKey(), src.getIndex());
  }
}

