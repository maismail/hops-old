package se.sics.hop.metadata.context;

import se.sics.hop.metadata.hdfs.entity.EntityContext;
import java.util.*;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.metadata.hdfs.entity.hop.HopCorruptReplica;
import se.sics.hop.metadata.lock.HDFSTransactionLockAcquirer;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.metadata.lock.HDFSTransactionLocks;
import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.dal.CorruptReplicaDataAccess;
import se.sics.hop.exception.LockUpgradeException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.EntityContextStat;
import se.sics.hop.metadata.hdfs.entity.TransactionContextMaintenanceCmds;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINodeCandidatePK;
import se.sics.hop.transaction.lock.TransactionLocks;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class CorruptReplicaContext extends EntityContext<HopCorruptReplica> {
  protected Map<Long, Set<HopCorruptReplica>> blockCorruptReplicas = new HashMap<Long, Set<HopCorruptReplica>>();
  protected Map<HopCorruptReplica, HopCorruptReplica> newCorruptReplicas = new HashMap<HopCorruptReplica, HopCorruptReplica>();
  protected Map<HopCorruptReplica, HopCorruptReplica> removedCorruptReplicas = new HashMap<HopCorruptReplica, HopCorruptReplica>();
  private Set<Integer> inodesRead = new HashSet<Integer>();
  protected boolean allCorruptBlocksRead = false;
  private CorruptReplicaDataAccess dataAccess;


  public CorruptReplicaContext(CorruptReplicaDataAccess dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(HopCorruptReplica entity) throws PersistanceException {
    if (removedCorruptReplicas.get(entity) != null) {
      throw new TransactionContextException("Removed corrupt replica passed to be persisted");
    }
    newCorruptReplicas.put(entity, entity);
    Set<HopCorruptReplica> list = blockCorruptReplicas.get(entity.getBlockId());
    if(list == null)
    {
        list = new TreeSet<HopCorruptReplica>();
        blockCorruptReplicas.put(entity.getBlockId(), list);
    }
    list.add(entity);
    
    log("added-corrupt", CacheHitState.NA,
            new String[]{"bid", Long.toString(entity.getBlockId()), "sid", Integer.toString(entity.getStorageId())});
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    blockCorruptReplicas.clear();
    newCorruptReplicas.clear();
    removedCorruptReplicas.clear();
    allCorruptBlocksRead = false;
    inodesRead.clear();
  }

  @Override
  public int count(CounterType<HopCorruptReplica> counter, Object... params) throws PersistanceException {
    throw new RuntimeException(UNSUPPORTED_COUNTER);
  }

  @Override
  public HopCorruptReplica find(FinderType<HopCorruptReplica> finder, Object... params) throws PersistanceException {
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<HopCorruptReplica> findList(FinderType<HopCorruptReplica> finder, Object... params) throws PersistanceException {
    HopCorruptReplica.Finder cFinder = (HopCorruptReplica.Finder) finder;

    switch (cFinder) {
      case ByBlockId:
        Long blockId = (Long) params[0];
        Integer inodeId = (Integer) params[1];
        if (blockCorruptReplicas.containsKey(blockId)) {
          log("find-corrupts-by-bid", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId), "inodeid", Integer.toString(inodeId)});
          return new ArrayList(blockCorruptReplicas.get(blockId));
        } else if (inodesRead.contains(inodeId) /*|| inodeId == INode.NON_EXISTING_ID*/) {
          return null;
        } else {
          log("find-corrupts-by-bid", CacheHitState.LOSS, new String[]{"bid", Long.toString(blockId), "inodeid", Integer.toString(inodeId)});
          aboutToAccessStorage();
          Set<HopCorruptReplica> list = new TreeSet(dataAccess.findByBlockId(blockId, inodeId));
          blockCorruptReplicas.put(blockId, list);
          return new ArrayList(blockCorruptReplicas.get(blockId)); // Shallow copy
        }
        case ByINodeId:;
        inodeId = (Integer) params[0];
        List<HopCorruptReplica> result = null;
        if(inodesRead.contains(inodeId)){
          log("find-corrupts-by-inode-id", CacheHitState.HIT, new String[]{"inode_id", Integer.toString(inodeId)});
          return getCorruptReplicasForINode(inodeId);
        }else{
          log("find-corrupts-by-inode-id", CacheHitState.LOSS, new String[]{"inode_id", Integer.toString(inodeId)});
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
  
  
  List<HopCorruptReplica> getCorruptReplicasForINode(int inodeId) {
    List<HopCorruptReplica> list = new ArrayList<HopCorruptReplica>();

    for (Set<HopCorruptReplica> set : blockCorruptReplicas.values()) {
      for (HopCorruptReplica corruptReplica : set) {
          if(corruptReplica.getInodeId() == inodeId){
            list.add(corruptReplica);
          }
      }
    }
    return list;
  }
  
  
  private void saveLists(List<HopCorruptReplica> list){
     for(HopCorruptReplica cr : list){
       Set<HopCorruptReplica> set = blockCorruptReplicas.get(cr.getBlockId());
       if(set == null){
         set = new TreeSet<HopCorruptReplica>();
       }
       set.add(cr);
       blockCorruptReplicas.put(cr.getBlockId(), set);
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
//        if ((!removedCorruptReplicas.values().isEmpty())
//                && hlks.getCrLock() != TransactionLockTypes.LockType.WRITE) {
//            throw new LockUpgradeException("Trying to upgrade corrupt replica locks");
//        }
        dataAccess.prepare(removedCorruptReplicas.values(), newCorruptReplicas.values(), null);
    }

  @Override
  public void remove(HopCorruptReplica entity) throws PersistanceException {
//    corruptReplicas.remove(entity);
    newCorruptReplicas.remove(entity);
    removedCorruptReplicas.put(entity, entity);
    if (blockCorruptReplicas.containsKey(entity.getBlockId())) {
      blockCorruptReplicas.get(entity.getBlockId()).remove(entity);
    }
    log("removed-corrupt", CacheHitState.NA,
            new String[]{"bid", Long.toString(entity.getBlockId()), "sid", Integer.toString(entity.getStorageId())});
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(HopCorruptReplica entity) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  
  @Override
  public EntityContextStat collectSnapshotStat() throws PersistanceException {
    EntityContextStat stat = new EntityContextStat("Corrupt Replicas",newCorruptReplicas.size(),0,removedCorruptReplicas.size());
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
          log("snapshot-maintenance-corrupt-pk-change", CacheHitState.NA, new String[]{"Before inodeId", Integer.toString(inodeBeforeChange.getId()), "name", inodeBeforeChange.getLocalName(), "pid", Integer.toString(inodeBeforeChange.getParentId()),"After inodeId", Integer.toString(inodeAfterChange.getId()), "name", inodeAfterChange.getLocalName(), "pid", Integer.toString(inodeAfterChange.getParentId()) });
          List<HopINodeCandidatePK> deletedINodesPK = new ArrayList<HopINodeCandidatePK>();
          deletedINodesPK.add(new HopINodeCandidatePK(inodeBeforeChange.getId()));
          updateCorruptReplicas(new HopINodeCandidatePK(inodeAfterChange.getId()), deletedINodesPK);
        }
        break;
      case Concat:
        checkForSnapshotChange();
        HopINodeCandidatePK trg_param = (HopINodeCandidatePK)params[0];
        List<HopINodeCandidatePK> srcs_param = (List<HopINodeCandidatePK>)params[1];
        List<BlockInfo> oldBlks  = (List<BlockInfo>)params[2];
        updateCorruptReplicas(trg_param, srcs_param);
        break;
    }
  }
  
  private void checkForSnapshotChange(){
     if (newCorruptReplicas.size() != 0 || removedCorruptReplicas.size() != 0 ) // during the tx no replica should have been changed
        {
          throw new IllegalStateException("No corrupt replicas row should have been changed during the Tx");
        }
  }
  
  private void updateCorruptReplicas(HopINodeCandidatePK trg_param, List<HopINodeCandidatePK> toBeDeletedSrcs){
    for(Set<HopCorruptReplica> set :blockCorruptReplicas.values() ){
      for(HopCorruptReplica corruptReplica : set){
        HopINodeCandidatePK pk = new HopINodeCandidatePK(corruptReplica.getInodeId());
        if(!trg_param.equals(pk) && toBeDeletedSrcs.contains(pk)){
          HopCorruptReplica toBeDeleted = cloneCorruptReplicaObj(corruptReplica);
          HopCorruptReplica toBeAdded = cloneCorruptReplicaObj(corruptReplica);
          
          removedCorruptReplicas.put(toBeDeleted, toBeDeleted);
          log("snapshot-maintenance-removed-corrupt",CacheHitState.NA, new String[]{"bid", Long.toString(toBeDeleted.getBlockId()),"inodeId", Integer.toString(toBeDeleted.getInodeId()), "partKey", Integer.toString(toBeDeleted.getPartKey())});
          //both inode id and partKey has changed
          toBeAdded.setInodeId(trg_param.getInodeId());
          newCorruptReplicas.put(toBeAdded, toBeAdded);
          log("snapshot-maintenance-added-corrupt",CacheHitState.NA, new String[]{"bid", Long.toString(toBeAdded.getBlockId()),"inodeId", Integer.toString(toBeAdded.getInodeId()), "partKey", Integer.toString(toBeAdded.getPartKey())});
        }
      }
    }
  }
  
  private HopCorruptReplica cloneCorruptReplicaObj(HopCorruptReplica src){
    return new HopCorruptReplica(src.getBlockId(),src.getStorageId(),src.getInodeId());
  }
}
