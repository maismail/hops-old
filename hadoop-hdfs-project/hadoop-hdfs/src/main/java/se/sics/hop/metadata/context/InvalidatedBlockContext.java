package se.sics.hop.metadata.context;

import se.sics.hop.metadata.hdfs.entity.EntityContext;
import java.util.*;
import se.sics.hop.metadata.hdfs.entity.hop.HopInvalidatedBlock;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.metadata.lock.HDFSTransactionLocks;
import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.dal.InvalidateBlockDataAccess;
import se.sics.hop.exception.LockUpgradeException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.transaction.lock.TransactionLocks;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class InvalidatedBlockContext extends EntityContext<HopInvalidatedBlock> {

  private Map<HopInvalidatedBlock, HopInvalidatedBlock> invBlocks = new HashMap<HopInvalidatedBlock, HopInvalidatedBlock>();
  private Map<String, HashSet<HopInvalidatedBlock>> storageIdToInvBlocks = new HashMap<String, HashSet<HopInvalidatedBlock>>();
  private Map<Long, HashSet<HopInvalidatedBlock>> blockIdToInvBlocks = new HashMap<Long, HashSet<HopInvalidatedBlock>>();
  private Map<HopInvalidatedBlock, HopInvalidatedBlock> newInvBlocks = new HashMap<HopInvalidatedBlock, HopInvalidatedBlock>();
  private Map<HopInvalidatedBlock, HopInvalidatedBlock> removedInvBlocks = new HashMap<HopInvalidatedBlock, HopInvalidatedBlock>();
  private boolean allInvBlocksRead = false;
  private int nullCount = 0;
  private InvalidateBlockDataAccess<HopInvalidatedBlock> dataAccess;

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
    newInvBlocks.put(invBlock, invBlock);

    if (storageIdToInvBlocks.containsKey(invBlock.getStorageId())) {
      storageIdToInvBlocks.get(invBlock.getStorageId()).add(invBlock);
    }

    if (blockIdToInvBlocks.containsKey(invBlock.getBlockId())) {
      blockIdToInvBlocks.get(invBlock.getBlockId()).add(invBlock);
    }

    log("added-invblock", CacheHitState.NA,
            new String[]{"bid", Long.toString(invBlock.getBlockId()), "sid", invBlock.getStorageId()});
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    invBlocks.clear();
    storageIdToInvBlocks.clear();
    newInvBlocks.clear();
    removedInvBlocks.clear();
    blockIdToInvBlocks.clear();
    allInvBlocksRead = false;
    nullCount = 0;
  }

  @Override
  public int count(CounterType<HopInvalidatedBlock> counter, Object... params) throws PersistanceException {
    HopInvalidatedBlock.Counter iCounter = (HopInvalidatedBlock.Counter) counter;
    switch (iCounter) {
      case All:
        if (allInvBlocksRead) {
          log("count-all-invblocks", CacheHitState.HIT);
          return invBlocks.size() - nullCount;
        } else {
          log("count-all-invblocks", CacheHitState.LOSS);
          aboutToAccessStorage();
          return dataAccess.countAll();
        }
    }

    throw new RuntimeException(UNSUPPORTED_COUNTER);
  }

  @Override
  public HopInvalidatedBlock find(FinderType<HopInvalidatedBlock> finder, Object... params) throws PersistanceException {
    HopInvalidatedBlock.Finder iFinder = (HopInvalidatedBlock.Finder) finder;

    switch (iFinder) {
      case ByPrimaryKey:
        long blockId = (Long) params[0];
        String storageId = (String) params[1];
        HopInvalidatedBlock searchInstance = new HopInvalidatedBlock(storageId, blockId);
        if (blockIdToInvBlocks.containsKey(blockId)) { // if inv-blocks are queried by bid but the search-key deos not exist
          if (!blockIdToInvBlocks.get(blockId).contains(searchInstance)) {
            log("find-invblock-by-pk-not-exist", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId), "sid", storageId});
            return null;
          }
        }
        // otherwise search-key should be the new query or it must be a hit
        if (invBlocks.containsKey(searchInstance)) {
          log("find-invblock-by-pk", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId), "sid", storageId});
          return invBlocks.get(searchInstance);
        } else if (removedInvBlocks.containsKey(searchInstance)) {
          log("find-invblock-by-pk-removed", CacheHitState.HIT, new String[]{"bid", Long.toString(blockId), "sid", storageId});
          return null;
        } else {
          log("find-invblock-by-pk", CacheHitState.LOSS, new String[]{"bid", Long.toString(blockId), "sid", storageId});
          aboutToAccessStorage();
          HopInvalidatedBlock result = dataAccess.findInvBlockByPkey(params);
          if (result == null) {
            nullCount++;
          }
          this.invBlocks.put(result, result);
          return result;
        }
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public List<HopInvalidatedBlock> findList(FinderType<HopInvalidatedBlock> finder, Object... params) throws PersistanceException {
    HopInvalidatedBlock.Finder iFinder = (HopInvalidatedBlock.Finder) finder;

    switch (iFinder) {
      case ByBlockId:
        long bid = (Long) params[0];
        if (blockIdToInvBlocks.containsKey(bid)) {
          log("find-invblocks-by-bid", CacheHitState.HIT, new String[]{"bid", String.valueOf(bid)});
          return new ArrayList<HopInvalidatedBlock>(this.blockIdToInvBlocks.get(bid)); //clone the list reference
        } else {
          log("find-invblocks-by-bid", CacheHitState.LOSS, new String[]{"bid", String.valueOf(bid)});
          aboutToAccessStorage();
          return syncInstancesForBlockId(dataAccess.findInvalidatedBlocksByBlockId(bid), bid);
        }
      case ByStorageId:
        String storageId = (String) params[0];
        if (storageIdToInvBlocks.containsKey(storageId)) {
          log("find-invblocks-by-storageid", CacheHitState.HIT, new String[]{"sid", storageId});
          return new ArrayList<HopInvalidatedBlock>(this.storageIdToInvBlocks.get(storageId)); //clone the list reference
        } else {
          log("find-invblocks-by-storageid", CacheHitState.LOSS, new String[]{"sid", storageId});
          aboutToAccessStorage();
          return syncInstancesForStorageId(dataAccess.findInvalidatedBlockByStorageId(storageId), storageId);
        }
      case All:
        List<HopInvalidatedBlock> result = new ArrayList<HopInvalidatedBlock>();
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
            result.add(invBlk);
          }
        }
        return result;
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
        if ((!removedInvBlocks.values().isEmpty())
                && hlks.getInvLocks()!= TransactionLockTypes.LockType.WRITE) {
            throw new LockUpgradeException("Trying to upgrade invalidated blocks locks");
        }  
    dataAccess.prepare(removedInvBlocks.values(), newInvBlocks.values(), new ArrayList<HopInvalidatedBlock>());
  }

  @Override
  public void remove(HopInvalidatedBlock invBlock) throws TransactionContextException {
    if (!invBlocks.containsKey(invBlock)) {
      // This is not necessary for invalidated-block
//      throw new TransactionContextException("Unattached invalidated-block passed to be removed");
    }

    invBlocks.remove(invBlock);
    newInvBlocks.remove(invBlock);
    removedInvBlocks.put(invBlock, invBlock);
    if (storageIdToInvBlocks.containsKey(invBlock.getStorageId())) {
      HashSet<HopInvalidatedBlock> ibs = storageIdToInvBlocks.get(invBlock.getStorageId());
      ibs.remove(invBlock);
    }
    if (blockIdToInvBlocks.containsKey(invBlock.getBlockId())) {
      HashSet<HopInvalidatedBlock> ibs = blockIdToInvBlocks.get(invBlock.getBlockId());
      ibs.remove(invBlock);
    }
    log("removed-invblock", CacheHitState.NA,
            new String[]{"bid", Long.toString(invBlock.getBlockId()), "sid", invBlock.getStorageId()});
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(HopInvalidatedBlock entity) throws TransactionContextException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  /**
   *
   * @param list returns only the data fetched from the storage not those cached
   * in memory.
   * @return
   */
  private List<HopInvalidatedBlock> syncInstances(Collection<HopInvalidatedBlock> list) {
    List<HopInvalidatedBlock> finalList = new ArrayList<HopInvalidatedBlock>();
    for (HopInvalidatedBlock invBlock : list) {
      if (!removedInvBlocks.containsKey(invBlock)) {
        if (invBlocks.containsKey(invBlock)) {
          if (invBlocks.get(invBlock) == null) {
            invBlocks.put(invBlock, invBlock);
            nullCount--;
          }
          finalList.add(invBlocks.get(invBlock));
        } else {
          invBlocks.put(invBlock, invBlock);
          finalList.add(invBlock);
        }
      }
    }

    return finalList;
  }

  private List<HopInvalidatedBlock> syncInstancesForStorageId(Collection<HopInvalidatedBlock> list, String sid) {
    HashSet<HopInvalidatedBlock> ibs = new HashSet<HopInvalidatedBlock>();
    for (HopInvalidatedBlock newBlock : newInvBlocks.values()) {
      if (newBlock.getStorageId().equals(sid)) {
        ibs.add(newBlock);
      }
    }

    filterRemovedBlocks(list, ibs);
    storageIdToInvBlocks.put(sid, ibs);

    return new ArrayList<HopInvalidatedBlock>(ibs);
  }

  private void filterRemovedBlocks(Collection<HopInvalidatedBlock> list, HashSet<HopInvalidatedBlock> existings) {
    for (HopInvalidatedBlock invBlock : list) {
      if (!removedInvBlocks.containsKey(invBlock)) {
        if (invBlocks.containsKey(invBlock)) {
          existings.add(invBlocks.get(invBlock));
        } else {
          invBlocks.put(invBlock, invBlock);
          existings.add(invBlock);
        }
      }
    }
  }

  private List<HopInvalidatedBlock> syncInstancesForBlockId(Collection<HopInvalidatedBlock> list, long bid) {
    HashSet<HopInvalidatedBlock> ibs = new HashSet<HopInvalidatedBlock>();
    for (HopInvalidatedBlock newBlock : newInvBlocks.values()) {
      if (newBlock.getBlockId() == bid) {
        ibs.add(newBlock);
      }
    }

    filterRemovedBlocks(list, ibs);
    blockIdToInvBlocks.put(bid, ibs);

    return new ArrayList<HopInvalidatedBlock>(ibs);
  }
}
