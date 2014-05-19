package se.sics.hop.metadata.context;

import se.sics.hop.metadata.hdfs.entity.EntityContext;
import java.util.*;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.metadata.hdfs.entity.hop.HopExcessReplica;
import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.dal.ExcessReplicaDataAccess;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.EntityContextStat;
import se.sics.hop.metadata.hdfs.entity.TransactionContextMaintenanceCmds;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINodeCandidatePK;
import se.sics.hop.transaction.lock.TransactionLocks;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class ExcessReplicaContext extends EntityContext<HopExcessReplica> {

  private Map<HopExcessReplica, HopExcessReplica> exReplicas = new HashMap<HopExcessReplica, HopExcessReplica>();
  private Map<Long, TreeSet<HopExcessReplica>> blockIdToExReplica = new HashMap<Long, TreeSet<HopExcessReplica>>();
  private Map<HopExcessReplica, HopExcessReplica> newExReplica = new HashMap<HopExcessReplica, HopExcessReplica>(); //TODO use sets
  private Map<HopExcessReplica, HopExcessReplica> removedExReplica = new HashMap<HopExcessReplica, HopExcessReplica>();//TODO use sets
  private Set<Integer> inodesRead = new HashSet<Integer>();
  private ExcessReplicaDataAccess<HopExcessReplica>  dataAccess;
  private int nullCount = 0;

  public ExcessReplicaContext(ExcessReplicaDataAccess<HopExcessReplica>  dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(HopExcessReplica exReplica) throws TransactionContextException {
    if (removedExReplica.containsKey(exReplica)) {
      throw new TransactionContextException("Removed excess-replica passed to be persisted");
    }

    if (exReplicas.containsKey(exReplica) && exReplicas.get(exReplica) == null) {
      nullCount--;
    }
    exReplicas.put(exReplica, exReplica);
    
    TreeSet<HopExcessReplica> set = blockIdToExReplica.get(exReplica.getBlockId());
    if(set == null){
      set = new TreeSet<HopExcessReplica>();
    }
    set.add(exReplica);
    blockIdToExReplica.put(exReplica.getBlockId(), set);
    
    newExReplica.put(exReplica, exReplica);
    log("added-excess", CacheHitState.NA,
            new String[]{"bid", Long.toString(exReplica.getBlockId()), "sid", Integer.toString(exReplica.getStorageId())});
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    exReplicas.clear();
    blockIdToExReplica.clear();
    newExReplica.clear();
    removedExReplica.clear();
    inodesRead.clear();
    nullCount = 0;
  }

  @Override
  public int count(CounterType<HopExcessReplica> counter, Object... params) throws PersistanceException {
//    HopExcessReplica.Counter eCounter = (HopExcessReplica.Counter) counter;
//    switch (eCounter) {
//      case All:
//        log("count-all-excess");
//        return dataAccess.countAll() + newExReplica.size() - removedExReplica.size() - nullCount;
//    }
    throw new RuntimeException(UNSUPPORTED_COUNTER);
  }

  @Override
  public HopExcessReplica find(FinderType<HopExcessReplica> finder,
          Object... params) throws PersistanceException {
    HopExcessReplica.Finder eFinder = (HopExcessReplica.Finder) finder;
    HopExcessReplica result = null;

    switch (eFinder) {
      case ByPKey:
        long blockId = (Long) params[0];
        int storageId = (Integer) params[1];
        int inodeId = (Integer) params[2];
        HopExcessReplica searchKey = new HopExcessReplica(storageId, blockId, inodeId);
        if (blockIdToExReplica.containsKey(blockId) && !blockIdToExReplica.get(blockId).contains(searchKey)) {
          log("find-excess-by-pk-not-exist", CacheHitState.HIT,
                  new String[]{"bid", Long.toString(blockId), "sid", Integer.toString(storageId)});
          return null;
        }
        if (exReplicas.containsKey(searchKey)) {
          log("find-excess-by-pk", CacheHitState.HIT,
                  new String[]{"bid", Long.toString(blockId), "sid", Integer.toString(storageId)});
          result = exReplicas.get(searchKey);
        } 
        else if (inodesRead.contains(inodeId) /*|| inodeId == INode.NON_EXISTING_ID*/) {
           log("find-excess-by-pk", CacheHitState.HIT,
                  new String[]{"bid", Long.toString(blockId), "sid", Integer.toString(storageId)});
          return null;
        }
        else if (removedExReplica.containsKey(searchKey)) {
          log("find-excess-by-pk-removed-item", CacheHitState.HIT,
                  new String[]{"bid", Long.toString(blockId), "sid", Integer.toString(storageId)});
          result = null;
        } 
        
        else {
          log("find-excess-by-pk", CacheHitState.LOSS,
                  new String[]{"bid", Long.toString(blockId), "sid", Integer.toString(storageId)});
          aboutToAccessStorage();
          result = dataAccess.findByPK(blockId, storageId, inodeId);
          if (result == null) {
            this.exReplicas.put(searchKey, null);
            TreeSet<HopExcessReplica> set = blockIdToExReplica.get(searchKey.getBlockId());
            if(set == null){
              set = new TreeSet<HopExcessReplica>();
            }
            blockIdToExReplica.put(searchKey.getBlockId(), set);
            nullCount++;
          }else{
            List<HopExcessReplica> list = new ArrayList<HopExcessReplica>();
            list.add(result);
            syncExcessReplicaInstances(list);
          }
        }
        return result;
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<HopExcessReplica> findList(FinderType<HopExcessReplica> finder, Object... params) throws PersistanceException {
    HopExcessReplica.Finder eFinder = (HopExcessReplica.Finder) finder;

    switch (eFinder) {
      case ByBlockId:
        long bId = (Long) params[0];
        Integer inodeId = (Integer) params[1];
        if (blockIdToExReplica.containsKey(bId)) {
          log("find-excess-by-blockId", CacheHitState.HIT, new String[]{"bid", String.valueOf(bId)});
        } 
        else if (inodesRead.contains(inodeId) /*|| inodeId == INode.NON_EXISTING_ID*/) {
           log("find-excess-by-blockId", CacheHitState.HIT, new String[]{"bid", String.valueOf(bId)});
          return null;
        }
        else {
          log("find-excess-by-blockId", CacheHitState.LOSS, new String[]{"bid", String.valueOf(bId)});
          aboutToAccessStorage();
          List<HopExcessReplica> list = dataAccess.findExcessReplicaByBlockId(bId, inodeId);
          
          TreeSet<HopExcessReplica> set = blockIdToExReplica.get(bId);
          if(set == null){
            set = new TreeSet<HopExcessReplica>();
          }
          blockIdToExReplica.put(bId, set); 
          
          if(list != null && !list.isEmpty()){
            syncExcessReplicaInstances(list);
          }
        }
        return new ArrayList<HopExcessReplica>(this.blockIdToExReplica.get(bId)); //clone the list reference
       case ByINodeId:;
        inodeId = (Integer) params[0];
        if(inodesRead.contains(inodeId)){
          log("find-excess-by-inode-id", CacheHitState.HIT, new String[]{"inode_id", Integer.toString(inodeId)});
          return getExcessReplicasForINode(inodeId);
        }else{
          log("find-excess-by-inode-id", CacheHitState.LOSS, new String[]{"inode_id", Integer.toString(inodeId)});
          aboutToAccessStorage();
          List<HopExcessReplica> list = dataAccess.findExcessReplicaByINodeId(inodeId);
          inodesRead.add(inodeId);
          if(list != null && !list.isEmpty()){
            syncExcessReplicaInstances(list);
          }
          return list;
        }   
    }
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }
  
  private List<HopExcessReplica> getExcessReplicasForINode(int inodeId){
    List<HopExcessReplica>  list = new ArrayList<HopExcessReplica>();
    for(TreeSet<HopExcessReplica> set : blockIdToExReplica.values()){
      for(HopExcessReplica excess: set){
        if(excess.getInodeId() == inodeId){
          list.add(excess);
        }
      }
    }
      
    return list;
  }

    @Override
    public void prepare(TransactionLocks lks) throws StorageException {
        // if the list is not empty then check for the lock types
        // lock type is checked after when list lenght is checked 
        // because some times in the tx handler the acquire lock 
        // function is empty and in that case tlm will throw 
        // null pointer exceptions
//        HDFSTransactionLocks hlks = (HDFSTransactionLocks)lks;
//        if ((!removedExReplica.values().isEmpty())
//                && hlks.getErLock() != TransactionLockTypes.LockType.WRITE) {
//            throw new LockUpgradeException("Trying to upgrade block locks");
//        }
        dataAccess.prepare(removedExReplica.values(), newExReplica.values(), null);
    }

  @Override
  public void remove(HopExcessReplica exReplica) throws PersistanceException {
    if (exReplicas.remove(exReplica) == null) {
      throw new TransactionContextException("Unattached excess-replica passed to be removed");
    }

    newExReplica.remove(exReplica);
    removedExReplica.put(exReplica, exReplica);
    
    if (blockIdToExReplica.containsKey(exReplica.getBlockId())) {
      TreeSet<HopExcessReplica> ibs = blockIdToExReplica.get(exReplica.getBlockId());
      ibs.remove(exReplica);
    }
    log("removed-excess", CacheHitState.NA,
            new String[]{"bid", Long.toString(exReplica.getBlockId()), "sid", Integer.toString(exReplica.getStorageId())});
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(HopExcessReplica entity) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  private void syncExcessReplicaInstances(List<HopExcessReplica> list) {
    for (HopExcessReplica replica : list) {
      if (!removedExReplica.containsKey(replica)) {
        if (exReplicas.containsKey(replica) && exReplicas.get(replica) == null ) {
            nullCount--;
        }
        exReplicas.put(replica, replica);
        
        TreeSet<HopExcessReplica> set = blockIdToExReplica.get(replica.getBlockId());
        if(set == null){
          set = new TreeSet<HopExcessReplica>();
        }
        set.add(replica);
        blockIdToExReplica.put(replica.getBlockId(), set);
      }
    }
  }
  
  @Override
  public EntityContextStat collectSnapshotStat() throws PersistanceException {
    EntityContextStat stat = new EntityContextStat("Excess Replicas",newExReplica.size(),0,removedExReplica.size());
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
          log("snapshot-maintenance-excess-pk-change", CacheHitState.NA, new String[]{"Before inodeId", Integer.toString(inodeBeforeChange.getId()), "name", inodeBeforeChange.getLocalName(), "pid", Integer.toString(inodeBeforeChange.getParentId()),"After inodeId", Integer.toString(inodeAfterChange.getId()), "name", inodeAfterChange.getLocalName(), "pid", Integer.toString(inodeAfterChange.getParentId()) });
          List<HopINodeCandidatePK> deletedINodesPK = new ArrayList<HopINodeCandidatePK>();
          deletedINodesPK.add(new HopINodeCandidatePK(inodeBeforeChange.getId()));
          updateExcessReplicas(new HopINodeCandidatePK(inodeAfterChange.getId()), deletedINodesPK);
        }
        break;
      case Concat:
        checkForSnapshotChange();
        HopINodeCandidatePK trg_param = (HopINodeCandidatePK)params[0];
        List<HopINodeCandidatePK> srcs_param = (List<HopINodeCandidatePK>)params[1];
        List<BlockInfo> oldBlks  = (List<BlockInfo>)params[2];
        updateExcessReplicas(trg_param, srcs_param);
        break;
    }
  }
  
  private void checkForSnapshotChange(){
     if (!newExReplica.isEmpty() || !removedExReplica.isEmpty() ) // during the tx no replica should have been changed
        {
          throw new IllegalStateException("No excess replicas row should have been changed during the Tx");
        }
  }
  
  private void updateExcessReplicas(HopINodeCandidatePK trg_param, List<HopINodeCandidatePK> toBeDeletedSrcs){
    
    
      for(HopExcessReplica exr : exReplicas.values()){
        if(exr == null) continue;
        HopINodeCandidatePK pk = new HopINodeCandidatePK(exr.getInodeId());
        if(!trg_param.equals(pk) && toBeDeletedSrcs.contains(pk)){
          HopExcessReplica toBeDeleted = cloneExcessReplicaObj(exr);
          HopExcessReplica toBeAdded = cloneExcessReplicaObj(exr);
          
          removedExReplica.put(toBeDeleted, toBeDeleted);
          log("snapshot-maintenance-removed-excess",CacheHitState.NA, new String[]{"bid", Long.toString(toBeDeleted.getBlockId()),"inodeId", Integer.toString(toBeDeleted.getInodeId())});
          
          //both inode id and partKey has changed
          toBeAdded.setInodeId(trg_param.getInodeId());
          newExReplica.put(toBeAdded, toBeAdded);
          log("snapshot-maintenance-added-excess",CacheHitState.NA, new String[]{"bid", Long.toString(toBeAdded.getBlockId()),"inodeId", Integer.toString(toBeAdded.getInodeId())});
        }
      }
    
  }
  
  private HopExcessReplica cloneExcessReplicaObj(HopExcessReplica src){
    return new HopExcessReplica(src.getStorageId(), src.getBlockId(), src.getInodeId());
  }
}