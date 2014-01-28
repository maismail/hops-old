package se.sics.hop.metadata.context;

import se.sics.hop.metadata.entity.EntityContext;
import java.util.*;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import se.sics.hop.transaction.lock.TransactionLockTypes.LockType;
import se.sics.hop.metadata.lock.HDFSTransactionLocks;
import se.sics.hop.metadata.entity.CounterType;
import se.sics.hop.metadata.entity.FinderType;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.transaction.TransactionContext;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.dal.BlockInfoDataAccess;
import se.sics.hop.exception.LockUpgradeException;
import se.sics.hop.exception.StorageException;
import org.apache.log4j.Logger;
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
  protected Map<Long, List<BlockInfo>> inodeBlocks = new HashMap<Long, List<BlockInfo>>();
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
        result = blocks.get(id);
        if (result == null && !blocks.containsKey(id)) { // a key may have null object associated with it
                                                         // some test intentionally look for blocks that are not in the DB
                                                         // duing the acquire lock phase if we see that an id does not
                                                         // exist in the db then we should put null in the cache for that id

          log("find-block-by-bid", CacheHitState.LOSS, new String[]{"bid", Long.toString(id)});
          aboutToAccessStorage();
          result = dataAccess.findById(id);
          if (result == null) {
            nullCount++;
          }
          blocks.put(id, result);
        } else {
          log("find-block-by-bid", CacheHitState.HIT, new String[]{"bid", Long.toString(id)});
        }
        return result;
      case MAX_BLK_INDX:
        //returning the block with max index
        final long inodeID = (Long) params[0];
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
        long inodeId = (Long) params[0];
        if (inodeBlocks.containsKey(inodeId)) {
          log("find-blocks-by-inodeid", CacheHitState.HIT, new String[]{"inodeid", Long.toString(inodeId)});
          return inodeBlocks.get(inodeId);
        } else {
          log("find-blocks-by-inodeid", CacheHitState.LOSS, new String[]{"inodeid", Long.toString(inodeId)});
          aboutToAccessStorage();
          result = dataAccess.findByInodeId(inodeId);
          inodeBlocks.put(inodeId, syncBlockInfoInstances(result));
          return result;
        }
      case ByStorageId:
        String storageId = (String) params[0];
        log("find-blocks-by-storageid", CacheHitState.NA, new String[]{"storageid", storageId});
        aboutToAccessStorage();
        result = dataAccess.findByStorageId(storageId);
        return syncBlockInfoInstances(result);
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
        HDFSTransactionLocks hlks = (HDFSTransactionLocks)lks;
        if ((!removedBlocks.values().isEmpty()
                || !modifiedBlocks.values().isEmpty())
                && hlks.getBlockLock() != LockType.WRITE) {
            throw new LockUpgradeException("Trying to upgrade block locks");
        }
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
    log("updated-blockinfo", CacheHitState.NA, new String[]{"bid", Long.toString(block.getBlockId())});
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
  
  private BlockInfo findMaxBlk(final long inodeID) {
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
}
