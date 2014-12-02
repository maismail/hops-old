package se.sics.hop.metadata.context;

import com.google.common.primitives.Ints;
import se.sics.hop.metadata.hdfs.entity.EntityContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINodeCandidatePK;
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
  private Map<Long, Map<Integer, HopIndexedReplica>> blocksReplicas = new HashMap<Long, Map<Integer, HopIndexedReplica>>();
  private Set<Integer> inodesRead = new HashSet<Integer>();
  private ReplicaDataAccess dataAccess;

  public ReplicaContext(ReplicaDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(HopIndexedReplica replica) throws PersistanceException {
    if (removedReplicas.containsKey(replica) || modifiedReplicas.containsKey(replica)) {
      throw new TransactionContextException("Removed/Modified replica passed to be persisted");
    }

    newReplicas.put(replica, replica);
    
    Map<Integer, HopIndexedReplica> map = blocksReplicas.get(replica.getBlockId());
    if(map == null){
      map = new HashMap<Integer, HopIndexedReplica>();
    }
    map.put(replica.getStorageId(), replica);
    blocksReplicas.put(replica.getBlockId(), map);
    
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
      dataAccess.prepare(removedReplicas.values(), newReplicas.values(), modifiedReplicas.values());
    }

  @Override
  public void remove(HopIndexedReplica replica) throws PersistanceException {
    if (!blocksReplicas.get(replica.getBlockId()).get(replica.getStorageId()).equals(replica)) {
      throw new TransactionContextException("Unattached replica passed to be removed, inodeId="+replica.getInodeId()+" bid="+replica.getBlockId()+" sid="+replica.getStorageId()+" index="+replica.getIndex());
    }

    
    modifiedReplicas.remove(replica);
    blocksReplicas.get(replica.getBlockId()).remove(replica.getStorageId());
    if (newReplicas.containsKey(replica)) {    //sometimes you add a replica in a Tx and then in the same tx the replica is removed. 
                                               //in this case simply remove the replica
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
    HopIndexedReplica.Finder iFinder = (HopIndexedReplica.Finder) finder;
    switch (iFinder) {
      case ByPK:
        long id = (Long) params[0];
        int sid = (Integer) params[1];
        if (blocksReplicas.containsKey(id)) {
          if (blocksReplicas.get(id).containsKey(sid)) {
            return blocksReplicas.get(id).get(sid);
          }
        }
        //block doesn't have any replicas at this sid or at all
        return null;
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public List<HopIndexedReplica> findList(FinderType<HopIndexedReplica> finder, Object... params) throws PersistanceException {
    HopIndexedReplica.Finder iFinder = (HopIndexedReplica.Finder) finder;
    
    switch (iFinder) {
      case ByBlockId:
        long blockId = (Long) params[0];
        Integer  inodeId = (Integer) params[1];
        if (blocksReplicas.containsKey(blockId)) {
          log("find-replicas-by-bid", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId)});
          return new ArrayList<HopIndexedReplica>(blocksReplicas.get(blockId).values());
        } else if (inodesRead.contains(inodeId) /*|| inodeId == INode.NON_EXISTING_ID*/){
          return null;
        }
        else {
          log("find-replicas-by-bid", CacheHitState.LOSS, new String[]{"bid", Long.toString(blockId)});
          aboutToAccessStorage();
          return syncInstances(dataAccess.findReplicasById(blockId, inodeId), blockId);
        }
      case ByINodeId:
        inodeId = (Integer) params[0];   
        if(inodesRead.contains(inodeId)){
          log("find-replicas-by-inode-id", CacheHitState.HIT, new String[]{"inode_id", Integer.toString(inodeId)});
          return getReplicasForINode(inodeId);
        }else{
          log("find-replicas-by-inode-id", CacheHitState.LOSS, new String[]{"inode_id", Integer.toString(inodeId)});
          aboutToAccessStorage();
          return syncInstances(dataAccess.findReplicasByINodeId(inodeId), inodeId);
        }
      case ByStorageId:
        long[] blockIds = (long[]) params[0];
        int sid = (Integer) params[1];
        int[] sids = new int[blockIds.length];
        Arrays.fill(sids, sid);
        log("find-replicas-by-sid", CacheHitState.NA, new String[]{"Ids", "" + blockIds, "sid", Integer.toString(sid)});
        //return syncInstances(dataAccess.findReplicasByPKS(blockIds, inodeIds, sids), blockIds, sid);
        return syncInstances(dataAccess.findReplicasByStorageId(sid), blockIds, sid);
      case ByPKS:
        long[] pblockIds = (long[]) params[0];
        int[] pinodeIds = (int[]) params[1];
        int psid = (Integer) params[2];
        int[] psids = new int[pblockIds.length];
        Arrays.fill(psids,psid);
        log("find-replicas-by-pks", CacheHitState.NA, new String[]{"Ids", "" + pblockIds, "InodeIds", "" + pinodeIds, " sid", Integer.toString(psid)});
        return syncInstances(dataAccess.findReplicasByPKS(pblockIds, pinodeIds, psids), pblockIds, psid);
      case ByINodeIds:
        int[] ids = (int[]) params[0];
        log("find-replicas-by-inode-ids", CacheHitState.LOSS, new String[]{"inode_ids", Arrays.toString(ids)});
        aboutToAccessStorage();
        return syncInstances(dataAccess.findReplicasByINodeIds(ids), ids);
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  private List<HopIndexedReplica> syncInstances(List<HopIndexedReplica> list, long blockId) {
    for (HopIndexedReplica r : list) {
      addReplica(blockId, r.getStorageId(), r);
    }
    if (list.isEmpty()) {
      addReplica(blockId, null, null);
    }
    return new ArrayList<HopIndexedReplica>(list);
  }
  
  private List<HopIndexedReplica> syncInstances(List<HopIndexedReplica> list, int inodeId) {
    for (HopIndexedReplica r : list) {
      addReplica(r.getBlockId(), r.getStorageId(), r);
    }
    inodesRead.add(inodeId);
    return new ArrayList<HopIndexedReplica>(list);
  }

  private List<HopIndexedReplica> syncInstances(List<HopIndexedReplica> list, int[] inodeIds) {
    for (HopIndexedReplica r : list) {
      addReplica(r.getBlockId(), r.getStorageId(), r);
    }
    inodesRead.addAll(Ints.asList(inodeIds));
    return new ArrayList<HopIndexedReplica>(list);
  }
   
  private List<HopIndexedReplica> syncInstances(List<HopIndexedReplica> list, long[] blockIds, int sid) {
    Set<Long> nullBlks = new HashSet<Long>();
    for (long id : blockIds) {
      nullBlks.add(id);
    }
    for (HopIndexedReplica replica : list) {
      long blkId = replica.getBlockId();
      if (nullBlks.contains(blkId)) {
        nullBlks.remove(blkId);
      }
      addReplica(blkId, sid, replica);
    }

    for (long blkId : nullBlks) {
      addReplica(blkId, sid, null);
    }

    return new ArrayList<HopIndexedReplica>(list);
  }

  private void addReplica(Long blkId, Integer sid, HopIndexedReplica replica) {
    Map<Integer, HopIndexedReplica> cMap = blocksReplicas.get(blkId);
    if (cMap == null) {
      cMap = new HashMap<Integer, HopIndexedReplica>();
      blocksReplicas.put(blkId, cMap);
    }
    if (sid != null) {
      cMap.put(sid, replica);
    }
  }
  
  private List<HopIndexedReplica> getReplicasForINode(int inodeId){
    List<HopIndexedReplica> tmp = new ArrayList<HopIndexedReplica>();
    for(Long blockId : blocksReplicas.keySet()){
      Collection<HopIndexedReplica> blockReplicas = blocksReplicas.get(blockId).values();
      for(HopIndexedReplica replica : blockReplicas){
        if(replica.getInodeId() == inodeId){
          tmp.add(replica);
        }
      }
    }
    return tmp;
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

    if(newReplicas.containsKey(replica)){
      newReplicas.put(replica, replica);
    }
    else{
      modifiedReplicas.put(replica, replica);
    }
    
    blocksReplicas.get(replica.getBlockId()).put(replica.getStorageId(), replica);
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
        break;
      case Concat:
        checkForSnapshotChange();
        HopINodeCandidatePK trg_param = (HopINodeCandidatePK)params[0];
        List<HopINodeCandidatePK> srcs_param = (List<HopINodeCandidatePK>)params[1];
        List<BlockInfo> oldBlks  = (List<BlockInfo>)params[2];
        updateReplicas(trg_param, srcs_param);
        break;
    }
  }
  
  private void checkForSnapshotChange(){
     if (newReplicas.size() != 0 || modifiedReplicas.size() != 0) // during the tx no replica should have been changed
        {// renaming to existing file will put replicas in the deleted list
          throw new IllegalStateException("No replica should have been changed during the Tx");
        }
  }
  
  private void updateReplicas(HopINodeCandidatePK trg_param, List<HopINodeCandidatePK> toBeDeletedSrcs){
    
    for(Map<Integer, HopIndexedReplica> replicas : blocksReplicas.values()){
      for(HopIndexedReplica replica : replicas.values()){
        HopINodeCandidatePK pk = new HopINodeCandidatePK(replica.getInodeId());
        if(!trg_param.equals(pk) && toBeDeletedSrcs.contains(pk)){
          HopIndexedReplica toBeDeleted = cloneReplicaObj(replica);
          HopIndexedReplica toBeAdded = cloneReplicaObj(replica);
          
          removedReplicas.put(toBeDeleted, toBeDeleted);
          log("snapshot-maintenance-removed-replica",CacheHitState.NA, new String[]{"bid", Long.toString(toBeDeleted.getBlockId()),"inodeId", Integer.toString(toBeDeleted.getInodeId())});
          
          //both inode id and partKey has changed
          toBeAdded.setInodeId(trg_param.getInodeId());
          newReplicas.put(toBeAdded, toBeAdded);
          log("snapshot-maintenance-added-replica",CacheHitState.NA, new String[]{"bid", Long.toString(toBeAdded.getBlockId()),"inodeId", Integer.toString(toBeAdded.getInodeId())});
        }
      }
    }
  }
  
  private HopIndexedReplica cloneReplicaObj(HopIndexedReplica src){
    return new HopIndexedReplica(src.getBlockId(), src.getStorageId(), src.getInodeId(), src.getIndex());
  }
}

