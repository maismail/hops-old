package se.sics.hop.metadata.context;

import se.sics.hop.metadata.hdfs.entity.EntityContext;
import se.sics.hop.metadata.hdfs.dal.UnderReplicatedBlockDataAccess;
import java.util.*;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.metadata.hdfs.entity.hop.HopUnderReplicatedBlock;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.metadata.lock.HDFSTransactionLocks;
import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.exception.LockUpgradeException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.EntityContextStat;
import se.sics.hop.metadata.hdfs.entity.TransactionContextMaintenanceCmds;
import se.sics.hop.transaction.lock.TransactionLocks;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class UnderReplicatedBlockContext extends EntityContext<HopUnderReplicatedBlock> {

  private Map<Long, HopUnderReplicatedBlock> urBlocks = new HashMap<Long, HopUnderReplicatedBlock>();
  private Map<Integer, HashSet<HopUnderReplicatedBlock>> levelToReplicas = new HashMap<Integer, HashSet<HopUnderReplicatedBlock>>();
  private Map<Long, HopUnderReplicatedBlock> newurBlocks = new HashMap<Long, HopUnderReplicatedBlock>();
  private Map<Long, HopUnderReplicatedBlock> modifiedurBlocks = new HashMap<Long, HopUnderReplicatedBlock>();
  private Map<Long, HopUnderReplicatedBlock> removedurBlocks = new HashMap<Long, HopUnderReplicatedBlock>();
  private Set<Integer> inodesRead = new HashSet<Integer>();
  private boolean allUrBlocksRead = false;
  private UnderReplicatedBlockDataAccess<HopUnderReplicatedBlock> dataAccess;

  public UnderReplicatedBlockContext( UnderReplicatedBlockDataAccess<HopUnderReplicatedBlock> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(HopUnderReplicatedBlock entity) throws PersistanceException {
    if (removedurBlocks.get(entity.getBlockId()) != null) {
//      throw new TransactionContextException("Removed under replica passed to be persisted");
        removedurBlocks.remove(entity.getBlockId());  
    }

    addNewReplica(entity);
    newurBlocks.put(entity.getBlockId(), entity);

    log("added-urblock", CacheHitState.NA,
            new String[]{"bid", Long.toString(entity.getBlockId()),
              "level", Integer.toString(entity.getLevel()),"inodeId",Integer.toString(entity.getInodeId()),"partKey",Integer.toString(entity.getPartKey())});
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    urBlocks.clear();
    newurBlocks.clear();
    modifiedurBlocks.clear();
    removedurBlocks.clear();
    levelToReplicas.clear();
    inodesRead.clear();
    allUrBlocksRead = false;
  }

  @Override
  public int count(CounterType counter, Object... params) throws PersistanceException {
    HopUnderReplicatedBlock.Counter urCounter = (HopUnderReplicatedBlock.Counter) counter;

    switch (urCounter) {
      case All:
        log("count-all-urblocks", CacheHitState.LOSS);
        return dataAccess.countAll();
      case ByLevel:
        Integer level = (Integer) params[0];
        log("count-urblocks-by-level", CacheHitState.LOSS, new String[]{Integer.toString(level)});
        return dataAccess.countByLevel(level);
      case LessThanLevel:
        level = (Integer) params[0];
        log("count-urblocks-less-than-level", CacheHitState.LOSS, new String[]{Integer.toString(level)});
        return dataAccess.countLessThanALevel(level);
    }

    throw new RuntimeException(UNSUPPORTED_COUNTER);
  }

  @Override
  public Collection<HopUnderReplicatedBlock> findList(FinderType<HopUnderReplicatedBlock> finder, Object... params) throws PersistanceException {
    HopUnderReplicatedBlock.Finder urFinder = (HopUnderReplicatedBlock.Finder) finder;
    switch (urFinder) {
      case All:
        if (allUrBlocksRead) {
          log("find-all-urblocks", CacheHitState.HIT);
        } else {
          log("find-all-urblocks", CacheHitState.LOSS);
          aboutToAccessStorage();
          syncUnderReplicatedBlockInstances(dataAccess.findAll());
          allUrBlocksRead = true;
        }
        List<HopUnderReplicatedBlock> result = new ArrayList();
        for (HopUnderReplicatedBlock urb : urBlocks.values()) {
          if (urb != null) {
            result.add(urb);
          }
        }
        Collections.sort(result, HopUnderReplicatedBlock.Order.ByLevel);
        return result;
      case ByLevel:
        Integer level = (Integer) params[0];
        if (allUrBlocksRead) {
          log("find-urblocks-by-level", CacheHitState.HIT, new String[]{"level", Integer.toString(level)});
        } else {
          log("find-urblocks-by-level", CacheHitState.LOSS, new String[]{"level", Integer.toString(level)});
          aboutToAccessStorage();
          syncUnderReplicatedBlockInstances(dataAccess.findByLevel(level));
        }
        if (levelToReplicas.containsKey(level)) {
          return new ArrayList(levelToReplicas.get(level));
        } else {
          return new ArrayList<HopUnderReplicatedBlock>();
        }
      case ByINodeId:
        Integer inodeId = (Integer) params[0];
        Integer partKey = (Integer) params[1];
        if(inodesRead.contains(inodeId)){
          log("find-urblocks-by-inode-id", CacheHitState.HIT, new String[]{"inode_id", Integer.toString(inodeId),"part_key", partKey!=null?Integer.toString(partKey):"NULL"});
          return getUnderReplicatedBlocksForINode(inodeId);
        }else{
          log("find-urblocks-by-inode-id", CacheHitState.LOSS, new String[]{"inode_id", Integer.toString(inodeId),"part_key", partKey!=null?Integer.toString(partKey):"NULL"});
          aboutToAccessStorage();
          result = dataAccess.findByINodeId(inodeId, partKey);
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
  public HopUnderReplicatedBlock find(FinderType<HopUnderReplicatedBlock> finder, Object... params) throws PersistanceException {
    HopUnderReplicatedBlock.Finder urFinder = (HopUnderReplicatedBlock.Finder) finder;
    switch (urFinder) {
      case ByBlockId:
        long blockId = (Long) params[0];
        Integer inodeId = (Integer) params[1];
        Integer partKey = (Integer) params[2];
                
        if (urBlocks.containsKey(blockId)) {
          log("find-urblock-by-bid", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId),"inode_id", Integer.toString(inodeId),"part_key", partKey!=null?Integer.toString(partKey):"NULL"});
          return urBlocks.get(blockId);
        }else if (inodesRead.contains(inodeId) /*|| inodeId == INode.NON_EXISTING_ID*/){
          return null;
        }else{
          log("find-urblock-by-bid", CacheHitState.LOSS, new String[]{"bid", Long.toString(blockId),"inode_id", Integer.toString(inodeId),"part_key", partKey!=null?Integer.toString(partKey):"NULL"});
          aboutToAccessStorage();
          HopUnderReplicatedBlock block = dataAccess.findByPk(blockId, inodeId, partKey);
          urBlocks.put(blockId, block);
          return block;
        }
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

   private List<HopUnderReplicatedBlock> getUnderReplicatedBlocksForINode(int inodeId){
    List<HopUnderReplicatedBlock>  list = new ArrayList<HopUnderReplicatedBlock>();
    for(HopUnderReplicatedBlock pbi: urBlocks.values()){
      if(pbi.getInodeId() == inodeId){
        list.add(pbi);
      }
    }
    return list;
  }
  
   private void saveLists(List<HopUnderReplicatedBlock> list){
     for(HopUnderReplicatedBlock pbi : list){
       urBlocks.put(pbi.getBlockId(), pbi);
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
//        if ((removedurBlocks.values().size() != 0
//                || modifiedurBlocks.values().size() != 0)
//                && hlks.getUrbLock()!= TransactionLockTypes.LockType.WRITE) {
//            throw new LockUpgradeException("Trying to upgrade under replicated locks");
//        }
    dataAccess.prepare(removedurBlocks.values(), newurBlocks.values(), modifiedurBlocks.values());
  }

  @Override
  public void remove(HopUnderReplicatedBlock entity) throws PersistanceException {

    if (!urBlocks.containsKey(entity.getBlockId())) {
      throw new TransactionContextException("Unattached under replica [blk:" + entity.getBlockId() + ", level: " + entity.getLevel() + " ] passed to be removed");
    }
    urBlocks.put(entity.getBlockId(), null);
    newurBlocks.remove(entity.getBlockId());
    modifiedurBlocks.remove(entity.getBlockId());
    removedurBlocks.put(entity.getBlockId(), entity);
    if (levelToReplicas.containsKey(entity.getLevel())) {
      levelToReplicas.get(entity.getLevel()).remove(entity);
    }
    log("removed-urblock", CacheHitState.NA,
            new String[]{"bid", Long.toString(entity.getBlockId()),
              "level", Integer.toString(entity.getLevel())});
  }

  @Override
  public void removeAll() throws PersistanceException {
    clear();
    aboutToAccessStorage();
    dataAccess.removeAll();
    log("removed-all-urblocks");
  }

  @Override
  public void update(HopUnderReplicatedBlock entity) throws PersistanceException {
    if (removedurBlocks.get(entity.getBlockId()) != null) {
      throw new TransactionContextException("Removed under replica passed to be persisted");
    }

    urBlocks.put(entity.getBlockId(), entity);
    modifiedurBlocks.put(entity.getBlockId(), entity);
    log("updated-urblock", CacheHitState.NA,
            new String[]{"bid", Long.toString(entity.getBlockId()),
              "level", Integer.toString(entity.getLevel())});
  }

  private List<HopUnderReplicatedBlock> syncUnderReplicatedBlockInstances(List<HopUnderReplicatedBlock> blocks) {
    ArrayList<HopUnderReplicatedBlock> finalList = new ArrayList<HopUnderReplicatedBlock>();

    for (HopUnderReplicatedBlock block : blocks) {
      if (removedurBlocks.containsKey(block.getBlockId())) {
        continue;
      }
      if (urBlocks.containsKey(block.getBlockId())) {
        if (urBlocks.get(block.getBlockId()) == null) {
          urBlocks.put(block.getBlockId(), block);
        }
        finalList.add(urBlocks.get(block.getBlockId()));
      } else {
        addNewReplica(block);
        finalList.add(block);
      }
    }

    return finalList;
  }

  private void addNewReplica(HopUnderReplicatedBlock block) {
    urBlocks.put(block.getBlockId(), block);
    if (!levelToReplicas.containsKey(block.getLevel())) {
      levelToReplicas.put(block.getLevel(), new HashSet<HopUnderReplicatedBlock>());
    }
    levelToReplicas.get(block.getLevel()).add(block);
  }
  
  @Override
  public EntityContextStat collectSnapshotStat() throws PersistanceException {
    EntityContextStat stat = new EntityContextStat("Under Replicated Blocks",newurBlocks.size(),modifiedurBlocks.size(),removedurBlocks.size());
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
        log("snapshot-maintenance-removed-urblock", CacheHitState.NA, new String[]{"id", Integer.toString(inodeBeforeChange.getId()), "name", inodeBeforeChange.getLocalName(), "pid", Integer.toString(inodeBeforeChange.getParentId()) });
        List<INodePK> deletedINodesPK = new ArrayList<INodePK>();
        deletedINodesPK.add(new INodePK(inodeBeforeChange.getId(), inodeBeforeChange.getPartKey()));
        updateReplicaUCs(new INodePK(inodeAfterChange.getId(), inodeAfterChange.getPartKey()), deletedINodesPK);
        break;
      case Concat:
        checkForSnapshotChange();
        INodePK trg_param = (INodePK)params[0];
        List<INodePK> srcs_param = (List<INodePK>)params[1];
        List<BlockInfo> oldBlks  = (List<BlockInfo>)params[2];
        updateReplicaUCs(trg_param, srcs_param);
        break;
    }
  }
  
  private void checkForSnapshotChange(){
     if (newurBlocks.size() != 0 || removedurBlocks.size() != 0 || modifiedurBlocks.size() != 0) // during the tx no replica should have been changed
        {
          throw new IllegalStateException("No under replicated blocks row should have been changed during the Tx");
        }
  }
  
  private void updateReplicaUCs(INodePK trg_param, List<INodePK> toBeDeletedSrcs){
    
    
      for(HopUnderReplicatedBlock pending : urBlocks.values()){
        INodePK pk = new INodePK(pending.getInodeId(), pending.getPartKey());
        if(!trg_param.equals(pk) && toBeDeletedSrcs.contains(pk)){
          HopUnderReplicatedBlock toBeDeleted = cloneURBObj(pending);
          HopUnderReplicatedBlock toBeAdded = cloneURBObj(pending);
          
          removedurBlocks.put(toBeDeleted.getBlockId(), toBeDeleted);
          
          //both inode id and partKey has changed
          toBeAdded.setInodeId(trg_param.id);
          toBeAdded.setPartKey(trg_param.partKey);
          newurBlocks.put(toBeAdded.getBlockId(), toBeAdded);
        }
      }
  }
  
  private HopUnderReplicatedBlock cloneURBObj(HopUnderReplicatedBlock src){
    return new HopUnderReplicatedBlock(src.getLevel(),src.getBlockId(),src.getInodeId(),src.getPartKey());
  }
}
