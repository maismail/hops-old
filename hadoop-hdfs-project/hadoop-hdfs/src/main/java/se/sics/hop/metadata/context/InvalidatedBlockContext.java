package se.sics.hop.metadata.context;

import se.sics.hop.metadata.hdfs.entity.EntityContext;
import java.util.*;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.metadata.hdfs.entity.hop.HopInvalidatedBlock;
import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.dal.InvalidateBlockDataAccess;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.EntityContextStat;
import se.sics.hop.metadata.hdfs.entity.TransactionContextMaintenanceCmds;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINodeCandidatePK;
import se.sics.hop.metadata.hdfs.entity.hop.HopBlockLookUp;
import se.sics.hop.transaction.lock.TransactionLocks;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class InvalidatedBlockContext extends EntityContext<HopInvalidatedBlock> {

  private Map<HopInvalidatedBlock, HopInvalidatedBlock> invBlocks = new HashMap<HopInvalidatedBlock, HopInvalidatedBlock>();
  private Map<Long, TreeSet<HopInvalidatedBlock>> blockIdToInvBlocks = new HashMap<Long, TreeSet<HopInvalidatedBlock>>();
  private Map<HopInvalidatedBlock, HopInvalidatedBlock> newInvBlocks = new HashMap<HopInvalidatedBlock, HopInvalidatedBlock>();
  private Map<HopInvalidatedBlock, HopInvalidatedBlock> removedInvBlocks = new HashMap<HopInvalidatedBlock, HopInvalidatedBlock>();
  private Set<Integer> inodesRead = new HashSet<Integer>();
  private InvalidateBlockDataAccess<HopInvalidatedBlock> dataAccess;
  private int nullCount = 0;
  private boolean allInvBlocksRead = false;

  public InvalidatedBlockContext(InvalidateBlockDataAccess<HopInvalidatedBlock> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(HopInvalidatedBlock invBlock) throws PersistanceException {
    if (removedInvBlocks.containsKey(invBlock)) {
      throw new TransactionContextException("Removed invalidated-block passed to be persisted");
    }

    if (invBlocks.containsKey(invBlock) && invBlocks.get(invBlock) == null) {
      nullCount--;
    }
    invBlocks.put(invBlock, invBlock);
    
    TreeSet<HopInvalidatedBlock> set = blockIdToInvBlocks.get(invBlock.getBlockId());
    if(set == null){
      set = new TreeSet<HopInvalidatedBlock>();
    }
    set.add(invBlock);
    blockIdToInvBlocks.put(invBlock.getBlockId(), set);
    
    newInvBlocks.put(invBlock, invBlock);
    if (blockIdToInvBlocks.containsKey(invBlock.getBlockId())) {
      blockIdToInvBlocks.get(invBlock.getBlockId()).add(invBlock);
    }

    log("added-invblock", CacheHitState.NA,
            new String[]{"bid", Long.toString(invBlock.getBlockId()), "sid", Integer.toString(invBlock.getStorageId())});
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    allInvBlocksRead = false;
    invBlocks.clear();
    newInvBlocks.clear();
    removedInvBlocks.clear();
    blockIdToInvBlocks.clear();
    inodesRead.clear();
    nullCount = 0;
  }

  @Override
  public int count(CounterType<HopInvalidatedBlock> counter, Object... params) throws PersistanceException {
//    HopInvalidatedBlock.Counter iCounter = (HopInvalidatedBlock.Counter) counter;
//    switch (iCounter) {
//      case All:
//        if (allInvBlocksRead) {
//          log("count-all-invblocks", CacheHitState.HIT);
//          return invBlocks.size() - nullCount;
//        } else {
//          log("count-all-invblocks", CacheHitState.LOSS);
//          aboutToAccessStorage();
//          return dataAccess.countAll();
//        }
//    }

    throw new RuntimeException(UNSUPPORTED_COUNTER);
  }

  @Override
  public HopInvalidatedBlock find(FinderType<HopInvalidatedBlock> finder, Object... params) throws PersistanceException {
    HopInvalidatedBlock.Finder iFinder = (HopInvalidatedBlock.Finder) finder;

    switch (iFinder) {
      case ByPK:
        long blockId = (Long) params[0];
        int storageId = (Integer) params[1];
        Integer inodeId = (Integer) params[2];
        HopInvalidatedBlock searchInstance = new HopInvalidatedBlock(storageId, blockId, inodeId);
        if (blockIdToInvBlocks.containsKey(blockId) && !blockIdToInvBlocks.get(blockId).contains(searchInstance)) {
            log("find-invblock-by-pk-not-exist", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId), "sid", Integer.toString(storageId), "inodeId", inodeId.toString()});
            return null;
        }
        // otherwise search-key should be the new query or it must be a hit
        if (invBlocks.containsKey(searchInstance)) {
          log("find-invblock-by-pk", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId), "sid", Integer.toString(storageId), "inodeId", inodeId.toString()});
          return invBlocks.get(searchInstance);
        }else if (inodesRead.contains(inodeId) || inodeId == INode.NON_EXISTING_ID) {
           log("find-invblock-by-pk", CacheHitState.HIT,
                  new String[]{"bid", Long.toString(blockId), "sid", Integer.toString(storageId)});
          return null;
        } else if (removedInvBlocks.containsKey(searchInstance)) {
          log("find-invblock-by-pk-removed", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId), "sid", Integer.toString(storageId), "inodeId", inodeId.toString()});
          return null;
        } else {
          log("find-invblock-by-pk", CacheHitState.LOSS, new String[]{"bid", Long.toString(blockId), "sid", Integer.toString(storageId), "inodeId", inodeId.toString()});
          aboutToAccessStorage();
          HopInvalidatedBlock result = dataAccess.findInvBlockByPkey(blockId, storageId, inodeId);
          if (result == null) {
            this.invBlocks.put(searchInstance, null);
            TreeSet<HopInvalidatedBlock> set = blockIdToInvBlocks.get(searchInstance.getBlockId());
            if(set == null){
              set = new TreeSet<HopInvalidatedBlock>();
            }
            blockIdToInvBlocks.put(searchInstance.getBlockId(), set);
            nullCount++;
          }else{
            List<HopInvalidatedBlock> list = new ArrayList<HopInvalidatedBlock>();
            list.add(result);
            syncInstances(list);
          }
          return result;
        }
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<HopInvalidatedBlock> findList(FinderType<HopInvalidatedBlock> finder, Object... params) throws PersistanceException {
    HopInvalidatedBlock.Finder iFinder = (HopInvalidatedBlock.Finder) finder;
    
    switch (iFinder) {
      case ByBlockId:
        long bId = (Long) params[0];
        Integer inodeId = (Integer) params[1];
        if (blockIdToInvBlocks.containsKey(bId)) {
          log("find-invblock-by-blockId", CacheHitState.HIT, new String[]{"bid", String.valueOf(bId)});
        } 
        else if (inodesRead.contains(inodeId) /*|| inodeId == INode.NON_EXISTING_ID*/) {
           log("find-invblock-by-blockId", CacheHitState.HIT, new String[]{"bid", String.valueOf(bId)});
          return null;
        }
        else {
          log("find-invblock-by-blockId", CacheHitState.LOSS, new String[]{"bid", String.valueOf(bId)});
          aboutToAccessStorage();
          List<HopInvalidatedBlock> list = (List<HopInvalidatedBlock>)dataAccess.findInvalidatedBlocksByBlockId(bId, inodeId);
          
          TreeSet<HopInvalidatedBlock> set = blockIdToInvBlocks.get(bId);
          if(set == null){
            set = new TreeSet<HopInvalidatedBlock>();
          }
          blockIdToInvBlocks.put(bId, set); 
          
          if(list != null && !list.isEmpty()){
            syncInstances(list);
          }
        }
        return new ArrayList<HopInvalidatedBlock>(this.blockIdToInvBlocks.get(bId)); //clone the list reference
       case ByINodeId:
        inodeId = (Integer) params[0];
        if(inodesRead.contains(inodeId)){
          log("find-invblock-by-inode-id", CacheHitState.HIT, new String[]{"inode_id", Integer.toString(inodeId),});
          return getInvBlksForINode(inodeId);
        }else{
          log("find-invblock-by-inode-id", CacheHitState.LOSS, new String[]{"inode_id", Integer.toString(inodeId)});
          aboutToAccessStorage();
          List<HopInvalidatedBlock> list = (List<HopInvalidatedBlock>)dataAccess.findInvalidatedBlocksByINodeId(inodeId);
          inodesRead.add(inodeId);
          if(list != null && !list.isEmpty()){
            syncInstances(list);
          }
          return list;
        }
        case All:
        List<HopInvalidatedBlock> resultList = new ArrayList<HopInvalidatedBlock>();
        if (!allInvBlocksRead) {
          log("find-all-invblocks", CacheHitState.LOSS);
          aboutToAccessStorage();
          syncInstances(dataAccess.findAllInvalidatedBlocks());
          allInvBlocksRead = true;
        } else {
          log("find-all-invblocks", CacheHitState.HIT);
        }
        for (HopInvalidatedBlock invBlk : invBlocks.values()) {
          if (invBlk != null) {
            resultList.add(invBlk);
          }
        }
        return resultList;
      case ByPKS:
        long[] blockIds = (long[]) params[0];
        int[] inodeIds = (int[]) params[1];
        int sid = (Integer) params[2];
        log("find-invblocks-by-pks", CacheHitState.NA, new String[]{"Ids", "" + blockIds, "sid", Integer.toString(sid)});
        int[] sids = new int[blockIds.length];
        Arrays.fill(sids, sid);
        return syncInstances(dataAccess.findInvalidatedBlocksbyPKS(blockIds, inodeIds, sids), blockIds, inodeIds, sid);
        }   
    throw new RuntimeException(UNSUPPORTED_FINDER);
  }
  
  private List<HopInvalidatedBlock> getInvBlksForINode(int inodeId){
    List<HopInvalidatedBlock>  list = new ArrayList<HopInvalidatedBlock>();
    for(TreeSet<HopInvalidatedBlock> set : blockIdToInvBlocks.values()){
      for(HopInvalidatedBlock excess: set){
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
//        if ((!removedInvBlocks.values().isEmpty())
//                && hlks.getInvLocks()!= TransactionLockTypes.LockType.WRITE) {
//            throw new LockUpgradeException("Trying to upgrade invalidated blocks locks");
//        }  
    dataAccess.prepare(removedInvBlocks.values(), newInvBlocks.values(), new ArrayList<HopInvalidatedBlock>());
  }

  @Override
  public void remove(HopInvalidatedBlock invBlock) throws TransactionContextException {
    if (!invBlocks.containsKey(invBlock) && !inodesRead.contains(invBlock.getInodeId())) {
       // This is not necessary for invalidated-block
       throw new TransactionContextException("Unattached invalidated-block passed to be removed");
    }
    
    else if (!invBlocks.containsKey(invBlock) && inodesRead.contains(invBlock.getInodeId())) {
      // nothing to delete
      return;
    }

    invBlocks.remove(invBlock);
    newInvBlocks.remove(invBlock);
    removedInvBlocks.put(invBlock, invBlock);
   
    if (blockIdToInvBlocks.containsKey(invBlock.getBlockId())) {
      TreeSet<HopInvalidatedBlock> ibs = blockIdToInvBlocks.get(invBlock.getBlockId());
      ibs.remove(invBlock);
    }
    log("removed-invblock", CacheHitState.NA,
            new String[]{"bid", Long.toString(invBlock.getBlockId()), "sid", Integer.toString(invBlock.getStorageId()), "", ""});
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(HopInvalidatedBlock entity) throws TransactionContextException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
  

   private List<HopInvalidatedBlock> syncInstances(List<HopInvalidatedBlock> list, long[] blockIds, int[] inodeIds, int sid) {
    List<HopBlockLookUp> nullBlks = new ArrayList<HopBlockLookUp>();
    for (int i=0;i<blockIds.length;i++) {
      nullBlks.add(new HopBlockLookUp(blockIds[i], inodeIds[i]));
    }
    for (HopInvalidatedBlock ib : list) {
      HopBlockLookUp blk = new HopBlockLookUp(ib.getBlockId(), ib.getInodeId());
      if (nullBlks.contains(blk)) {
        nullBlks.remove(blk);
      }
    }

    for (HopBlockLookUp blk : nullBlks) {
      invBlocks.put(new HopInvalidatedBlock(sid, blk.getBlockId(), blk.getInodeId()), null);
    }
    syncInstances(list);
    return list;
  }
   
  /**
   *
   * @param list returns only the data fetched from the storage not those cached
   * in memory.
   * @return
   */
  private void syncInstances(Collection<HopInvalidatedBlock> list) {
    //return finalList;
    for (HopInvalidatedBlock invBlk : list) {
      if (!removedInvBlocks.containsKey(invBlk)) {
        if (invBlocks.containsKey(invBlk) && invBlocks.get(invBlk) == null ) {
            nullCount--;
        }
        invBlocks.put(invBlk, invBlk);
        
        TreeSet<HopInvalidatedBlock> set = blockIdToInvBlocks.get(invBlk.getBlockId());
        if(set == null){
          set = new TreeSet<HopInvalidatedBlock>();
        }
        set.add(invBlk);
        blockIdToInvBlocks.put(invBlk.getBlockId(), set);
      }
    }
  }

  
  @Override
  public EntityContextStat collectSnapshotStat() throws PersistanceException {
    EntityContextStat stat = new EntityContextStat("Invalidated Blocks",newInvBlocks.size(),0,removedInvBlocks.size());
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
        updateIvlidatedReplicas(trg_param, srcs_param);
        break;
    }
  }
  
  private void checkForSnapshotChange(){
     if ( !removedInvBlocks.isEmpty() ) // removing invalidated blocks during rename/concat is not supported
        {
          throw new IllegalStateException("No invalidated blocks can be removed duing rename/concat operation");
        }
  }
  
  private void updateIvlidatedReplicas(HopINodeCandidatePK trg_param, List<HopINodeCandidatePK> toBeDeletedSrcs){
    
    
      for(HopInvalidatedBlock exr : invBlocks.values()){
        if(exr == null) continue;
        HopINodeCandidatePK pk = new HopINodeCandidatePK(exr.getInodeId());
        if(!trg_param.equals(pk) && toBeDeletedSrcs.contains(pk)){
          HopInvalidatedBlock toBeDeleted = cloneInvalidatedReplicaObj(exr);
          HopInvalidatedBlock toBeAdded = cloneInvalidatedReplicaObj(exr);
          
          removedInvBlocks.put(toBeDeleted, toBeDeleted);
          log("snapshot-maintenance-removed-invblocks",CacheHitState.NA, new String[]{"bid", Long.toString(toBeDeleted.getBlockId()),"inodeId", Integer.toString(toBeDeleted.getInodeId())});
          
          //both inode id and partKey has changed
          toBeAdded.setInodeId(trg_param.getInodeId());
          newInvBlocks.put(toBeAdded, toBeAdded);
          log("snapshot-maintenance-added-invblocks",CacheHitState.NA, new String[]{"bid", Long.toString(toBeAdded.getBlockId()),"inodeId", Integer.toString(toBeAdded.getInodeId())});
        }
      }
    
  }
  
  private HopInvalidatedBlock cloneInvalidatedReplicaObj(HopInvalidatedBlock src){
    return new HopInvalidatedBlock(src.getStorageId(), src.getBlockId(),src.getInodeId());
  }
}
