package se.sics.hop.metadata.context;

import se.sics.hop.metadata.hdfs.entity.EntityContext;
import java.util.*;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.transaction.TransactionContext;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.dal.BlockInfoDataAccess;
import se.sics.hop.exception.StorageException;
import org.apache.log4j.Logger;
import static se.sics.hop.metadata.context.HOPTransactionContextMaintenanceCmds.INodePKChanged;
import static se.sics.hop.metadata.hdfs.entity.EntityContext.log;
import se.sics.hop.metadata.hdfs.entity.EntityContextStat;
import se.sics.hop.metadata.hdfs.entity.TransactionContextMaintenanceCmds;
import se.sics.hop.transaction.lock.TransactionLocks;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class BlockInfoContext extends EntityContext<BlockInfo> {

  private final static Logger LOG = Logger.getLogger(TransactionContext.class);
  protected Map<Long, BlockInfo> blocks = new HashMap<Long, BlockInfo>();
  protected Map<Long, BlockInfo> newBlocks = new HashMap<Long, BlockInfo>();
  protected Map<Long, BlockInfo> modifiedBlocks = new HashMap<Long, BlockInfo>();
  protected Map<Long, BlockInfo> removedBlocks = new HashMap<Long, BlockInfo>();
  protected Map<Integer, List<BlockInfo>> inodeBlocks = new HashMap<Integer, List<BlockInfo>>();
  protected boolean allBlocksRead = false;
  BlockInfoDataAccess<BlockInfo> dataAccess;
  private int nullCount = 0;

  public BlockInfoContext(BlockInfoDataAccess<BlockInfo> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(BlockInfo block) throws PersistanceException {
    if (removedBlocks.containsKey(block.getBlockId())) {
      throw new TransactionContextException("Removed block passed to be persisted");
    }
    if (blocks.containsKey(block.getBlockId()) && blocks.get(block.getBlockId()) == null) {
      nullCount--;
    }
    blocks.put(block.getBlockId(), block);
    newBlocks.put(block.getBlockId(), block);
    log("added-blockinfo", CacheHitState.NA, new String[]{"bid", Long.toString(block.getBlockId()),
              "inodeid", Long.toString(block.getInodeId())});
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    blocks.clear();
    newBlocks.clear();
    modifiedBlocks.clear();
    removedBlocks.clear();
    inodeBlocks.clear();
    allBlocksRead = false;
    nullCount = 0;
  }

  @Override
  public int count(CounterType<BlockInfo> counter, Object... params) throws PersistanceException {
    BlockInfo.Counter bCounter = (BlockInfo.Counter) counter;
    switch (bCounter) {
      case All:
        if (allBlocksRead) {
          log("Count-all-blocks", CacheHitState.HIT);
          return blocks.size() - nullCount;
        } else {
          log("Count-all-blocks", CacheHitState.LOSS);
//          aboutToAccessStorage();
          return dataAccess.countAll();
        }
    }
    throw new RuntimeException(UNSUPPORTED_COUNTER);
  }

  @Override
  public BlockInfo find(FinderType<BlockInfo> finder, Object... params) throws PersistanceException {
    BlockInfo.Finder bFinder = (BlockInfo.Finder) finder;
    BlockInfo result = null;
    switch (bFinder) {
      case ById:
        long id = (Long) params[0];
        Integer  partKey =  null;
        if(params.length > 1 && params[1] != null){
          partKey = (Integer) params[1];
        }
        result = blocks.get(id);
        if (result == null && !blocks.containsKey(id)) { // a key may have null object associated with it
                                                         // some test intentionally look for blocks that are not in the DB
                                                         // duing the acquire lock phase if we see that an id does not
                                                         // exist in the db then we should put null in the cache for that id

          log("find-block-by-bid", CacheHitState.LOSS, new String[]{"bid", Long.toString(id),"part_key", partKey!=null?Integer.toString(partKey):"NULL"});
          aboutToAccessStorage();
          if(partKey == null){
            throw new NullPointerException("Part Key is not set");
          }
          result = dataAccess.findById(id,partKey);
          if (result == null) {
            nullCount++;
          }
          blocks.put(id, result);
        } else {
          log("find-block-by-bid", CacheHitState.HIT, new String[]{"bid", Long.toString(id),"part_key", partKey!=null?Integer.toString(partKey):"NULL"});
        }
        return result;
      case MAX_BLK_INDX:
        //returning the block with max index
        final int inodeID = (Integer) params[0];
        return findMaxBlk(inodeID);
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public List<BlockInfo> findList(FinderType<BlockInfo> finder, Object... params) throws PersistanceException {
    BlockInfo.Finder bFinder = (BlockInfo.Finder) finder;
    List<BlockInfo> result = null;
    switch (bFinder) {
      case ByInodeId:
        Integer inodeId = (Integer) params[0];
        Integer partKey = (Integer) params[1];
        if (inodeBlocks.containsKey(inodeId)) {
          log("find-blocks-by-inodeid", CacheHitState.HIT, new String[]{"inodeid", Integer.toString(inodeId),"part_key", Integer.toString(partKey)});
          return inodeBlocks.get(inodeId);
        } else {
          log("find-blocks-by-inodeid", CacheHitState.LOSS, new String[]{"inodeid", Integer.toString(inodeId),"part_key", Integer.toString(partKey)});
          aboutToAccessStorage();
          result = dataAccess.findByInodeId(inodeId,partKey);
          inodeBlocks.put(inodeId, syncBlockInfoInstances(result));
          return result;
        }
//      case ByStorageId:
//        int storageId = (Integer) params[0];
//        log("find-blocks-by-storageid", CacheHitState.NA, new String[]{"storageid", Integer.toString(storageId)});
//        aboutToAccessStorage();
//        result = dataAccess.findByStorageId(storageId);
//        return syncBlockInfoInstances(result);
      case All:
        if (allBlocksRead) {
          log("find-all-blocks", CacheHitState.HIT);
          List<BlockInfo> list = new ArrayList<BlockInfo>();
          for (BlockInfo info : blocks.values()) {
            if (info != null) {
              list.add(info);
            }
          }
          return list;
        } else {
          log("find-all-blocks", CacheHitState.LOSS);
          aboutToAccessStorage();
          result = dataAccess.findAllBlocks();
          allBlocksRead = true;
          return syncBlockInfoInstances(result);
        }
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
    public void prepare(TransactionLocks lks) throws StorageException {
        // if the list is not empty then check for the lock types
        // lock type is checked after when list lenght is checked 
        // because some times in the tx handler the acquire lock 
        // function is empty and in that case tlm will throw 
        // null pointer exceptions
//        HDFSTransactionLocks hlks = (HDFSTransactionLocks)lks;
//        if ((!removedBlocks.values().isEmpty()
//                || !modifiedBlocks.values().isEmpty())
//                && hlks.getBlockLock() != LockType.WRITE) {
//            throw new LockUpgradeException("Trying to upgrade block locks");
//        }
        dataAccess.prepare(removedBlocks.values(), newBlocks.values(), modifiedBlocks.values());
    }

  @Override
  public void remove(BlockInfo block) throws PersistanceException {
//    if (block.getBlockId() == 0l) {
//      throw new TransactionContextException("Unassigned-Id block passed to be removed");
//    }

    BlockInfo attachedBlock = blocks.get(block.getBlockId());

    if (attachedBlock == null) {
      throw new TransactionContextException("Unattached block passed to be removed");
    }

    blocks.remove(block.getBlockId());
    newBlocks.remove(block.getBlockId());
    modifiedBlocks.remove(block.getBlockId());
    removedBlocks.put(block.getBlockId(), attachedBlock);
    removeBlockFromInodeBlocks(block);
    log("removed-blockinfo", CacheHitState.NA, new String[]{"bid", Long.toString(block.getBlockId())});
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(BlockInfo block) throws PersistanceException {
    if (removedBlocks.containsKey(block.getBlockId())) {
      throw new TransactionContextException("Removed block passed to be persisted");
    }
    blocks.put(block.getBlockId(), block);
    modifiedBlocks.put(block.getBlockId(), block);
    updateInodeBlocks(block);
    log("updated-blockinfo", CacheHitState.NA, new String[]{"bid", Long.toString(block.getBlockId()), "inodeId", Long.toString(block.getInodeId()), "blk index", Integer.toString(block.getBlockIndex())});
  }

  private List<BlockInfo> syncBlockInfoInstances(List<BlockInfo> newBlocks) {
    List<BlockInfo> finalList = new ArrayList<BlockInfo>();

    for (BlockInfo blockInfo : newBlocks) {
      if (blocks.containsKey(blockInfo.getBlockId()) && !removedBlocks.containsKey(blockInfo.getBlockId())) {
        if (blocks.get(blockInfo.getBlockId()) == null) {
          blocks.put(blockInfo.getBlockId(), blockInfo);
          nullCount--;
        }
        finalList.add(blocks.get(blockInfo.getBlockId()));
      } else {
        blocks.put(blockInfo.getBlockId(), blockInfo);
        finalList.add(blockInfo);
      }
    }

    return finalList;
  }

  private void updateInodeBlocks(BlockInfo newBlock/*new or updated block*/) {
    List<BlockInfo> blockList = inodeBlocks.get(newBlock.getInodeId());

    if (blockList != null) {
      if (blockList.contains(newBlock)) {
        BlockInfo oldBlock = blockList.remove(blockList.indexOf(newBlock));
//        LOG.debug("xxxxxxxxx  blk_id "+newBlock.getBlockId()+" old state "+oldBlock.getBlockUCState()+" new state "+newBlock.getBlockUCState()+" inode id "+newBlock.getInodeId()+" blocks are "+(blockList.size()+1));
        blockList.add(newBlock);
      }      
    }
  }

  private void removeBlockFromInodeBlocks(BlockInfo block) throws TransactionContextException {
    List<BlockInfo> blockList = inodeBlocks.get(block.getInodeId());
    if (blockList != null) {
      if (blockList.contains(block)) {
        blockList.remove(block);
      } else {
        throw new TransactionContextException("Trying to remove a block that does not exist");
      }
    }
  }
  
  private BlockInfo findMaxBlk(final int inodeID) {
    // find the max block in the following lists
    // inodeBlocks
    // modified list
    // new list
    BlockInfo maxBlk = null;
    List<BlockInfo> blockList = inodeBlocks.get(inodeID);

    for (int i = 0; i < blockList.size(); i++) {
      if (maxBlk == null || maxBlk.getBlockIndex() < blockList.get(i).getBlockIndex()) {
        maxBlk = blockList.get(i);
      }
    }

    Collection<BlockInfo> mBlks = modifiedBlocks.values();
    for (BlockInfo blk : mBlks) {
      if (maxBlk == null || (blk.getInodeId() == inodeID && blk.getBlockIndex() > maxBlk.getBlockIndex())) {
        maxBlk = blk;
      }
    }


    Collection<BlockInfo> nBlks = this.newBlocks.values();
    for (BlockInfo blk : nBlks) {
      if (maxBlk == null || (blk.getInodeId() == inodeID && blk.getBlockIndex() > maxBlk.getBlockIndex())) {
        maxBlk = blk;
      }
    }


    return maxBlk;
  }
  
  @Override
  public EntityContextStat collectSnapshotStat() throws PersistanceException {
    EntityContextStat stat = new EntityContextStat("Blocks", newBlocks.size(),modifiedBlocks.size(),removedBlocks.size());
    return stat;
  }
  
  @Override
  public void snapshotMaintenance(TransactionContextMaintenanceCmds cmds, Object... params) throws PersistanceException {
    HOPTransactionContextMaintenanceCmds hopCmds = (HOPTransactionContextMaintenanceCmds) cmds;
    switch (hopCmds) {
      case INodePKChanged:
        //delete the previous row from db
        INode inodeBeforeChange  = (INode) params[0];
        INode inodeAfterChange   = (INode) params[1];
        updateBlocks(inodeBeforeChange, inodeAfterChange);
        break;
      case Concat:
        // do nothing here
        // the concat function addes the blocks to the inode and updates the id and partkey
        break;
    }
  }
  
  private void updateBlocks(INode inodeBeforeChange, INode inodeAfterChange) throws PersistanceException {
    // when you overwrite a file the dst file blocks are removed
    // removedBlocks list may not be empty
    if (!newBlocks.isEmpty() || !modifiedBlocks.isEmpty()) {//incase of move and rename the blocks should not have been modified in any way
        throw new StorageException("Renaming a file(s) whose blocks are changed. During rename and move no block blocks should have been changed.");
      }
    
    if (inodeBeforeChange instanceof INodeFile) { // with the current partitioning mechanism the blocks are only changed if only a file is renamed or moved. 
        
      
      if (inodeBeforeChange.getLocalName().equals(inodeAfterChange.getLocalName()) ==  false) { //file name was changed. partKey has to be changed in the blocks of the src file
        for (BlockInfo bInfo : blocks.values()) {
          if (bInfo.getInodeId() == inodeBeforeChange.getId()) {
            BlockInfo removedBlk = cloneBlock(bInfo);
            removedBlocks.put(removedBlk.getBlockId(), removedBlk);

            bInfo.setPartKeyNoPersistance(inodeAfterChange.getPartKey());
            modifiedBlocks.put(bInfo.getBlockId(), bInfo);
          }
        }
        log("snapshot-maintenance-removed-inode", CacheHitState.NA, new String[]{"id", Integer.toString(inodeBeforeChange.getId()), "name", inodeBeforeChange.getLocalName(), "pid", Integer.toString(inodeBeforeChange.getParentId())});
      }
    }
  }
  
  
  private BlockInfo cloneBlock(BlockInfo block) throws PersistanceException{
    if(block instanceof BlockInfo){
      return new BlockInfo(((BlockInfo)block),((BlockInfo)block).getInodeId(),((BlockInfo)block).getPartKey());
    }
    else if(block instanceof  BlockInfoUnderConstruction){
      return new BlockInfoUnderConstruction((BlockInfoUnderConstruction)block, ((BlockInfoUnderConstruction)block).getInodeId(),((BlockInfoUnderConstruction)block).getPartKey());
    }else{
      throw new StorageException("Unable to create a clone of the Block");
    }
  }
  
  
  
  
}
