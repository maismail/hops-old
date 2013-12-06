package se.sics.hop.metadata.persistence.context.entity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import se.sics.hop.metadata.persistence.lock.TransactionLockTypes.LockType;
import se.sics.hop.metadata.persistence.lock.TransactionLocks;
import se.sics.hop.metadata.persistence.CounterType;
import se.sics.hop.metadata.persistence.FinderType;
import se.sics.hop.metadata.persistence.exceptions.PersistanceException;
import se.sics.hop.metadata.persistence.context.TransactionContextException;
import se.sics.hop.metadata.persistence.dal.BlockTokenKeyDataAccess;
import se.sics.hop.metadata.persistence.context.LockUpgradeException;
import se.sics.hop.metadata.persistence.exceptions.StorageException;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class BlockTokenKeyContext extends EntityContext<BlockKey> {

  protected Map<Integer, BlockKey> keys = new HashMap<Integer, BlockKey>();
  protected Map<Integer, BlockKey> newKeys = new HashMap<Integer, BlockKey>();
  protected Map<Integer, BlockKey> modifiedKeys = new HashMap<Integer, BlockKey>();
  protected Map<Integer, BlockKey> removedKeys = new HashMap<Integer, BlockKey>();
  BlockTokenKeyDataAccess<BlockKey> dataAccess;
  protected boolean allKeysRead = false;
  protected BlockKey currKey;
  protected BlockKey nextKey;
  private int nullCount = 0;

  public BlockTokenKeyContext(BlockTokenKeyDataAccess<BlockKey> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(BlockKey key) throws PersistanceException {
    if (removedKeys.containsKey(key.getKeyId())) {
      throw new TransactionContextException("Removed blockkey passed to be persisted");
    }
    if (keys.containsKey(key.getKeyId()) && keys.get(key.getKeyId()) == null) {
      nullCount--;
    }
    keys.put(key.getKeyId(), key);
    newKeys.put(key.getKeyId(), key);
    log("added-blockkey", CacheHitState.NA, new String[]{"kid", Integer.toString(key.getKeyId())});
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    keys.clear();
    newKeys.clear();
    modifiedKeys.clear();
    removedKeys.clear();
    allKeysRead = false;
    currKey = null;
    nextKey = null;
    nullCount = 0;
  }

  @Override
  public int count(CounterType<BlockKey> counter, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public BlockKey find(FinderType<BlockKey> finder, Object... params) throws PersistanceException {
    BlockKey.Finder bFinder = (BlockKey.Finder) finder;
    switch (bFinder) {
      case ById:
        int id = (Integer) params[0];
        BlockKey result;
        if (!keys.containsKey(id)) {
          log("find-blockkey-by-kid", CacheHitState.LOSS, new String[]{"kid", Integer.toString(id)});
          aboutToAccessStorage();
          result = dataAccess.findByKeyId(id);
          if (result == null) {
            nullCount++;
          }
          keys.put(id, result);
        } else {
          result = keys.get(id);
          log("find-blockkey-by-kid", CacheHitState.HIT, new String[]{"kid", Integer.toString(id)});
        }
        return result;
      case ByType:
        short type = (Short) params[0];
        if (type == BlockKey.CURR_KEY) {
          if (currKey == null) {
            currKey = dataAccess.findByKeyType(type);
          }
          return currKey;
        }
        if (type == BlockKey.NEXT_KEY) {
          if (nextKey == null) {
            nextKey = dataAccess.findByKeyType(type);
          }
          return nextKey;
        }
        throw new RuntimeException("Wrong key type " + type);
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<BlockKey> findList(FinderType<BlockKey> finder, Object... params) throws PersistanceException {
    BlockKey.Finder bFinder = (BlockKey.Finder) finder;
    List<BlockKey> result = null;
    switch (bFinder) {
      case All:
        if (allKeysRead) {
          log("find-all-blockkeys", CacheHitState.HIT);
          List<BlockKey> list = new ArrayList<BlockKey>();
          for (BlockKey blockKey : keys.values()) {
            if (blockKey != null) {
              list.add(blockKey);
            }
          }
          return list;
        } else {
          log("find-all-blockkeys", CacheHitState.LOSS);
          aboutToAccessStorage();
          result = dataAccess.findAll();
          allKeysRead = true;
          return syncBlockKeyInstances(result);
        }
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public void prepare(TransactionLocks lks) throws StorageException {
    if ((!removedKeys.isEmpty()
                || !modifiedKeys.isEmpty())
                && lks.getBlockKeyLock() != LockType.WRITE) {
            throw new LockUpgradeException("Trying to upgrade block locks");
        }
    dataAccess.prepare(removedKeys.values(), newKeys.values(), modifiedKeys.values());
  }

  @Override
  public void remove(BlockKey key) throws PersistanceException {

    BlockKey attachedKey = keys.get(key.getKeyId());

    if (attachedKey == null) {
      throw new TransactionContextException("Unattached blockkey passed to be removed");
    }

    keys.remove(key.getKeyId());
    newKeys.remove(key.getKeyId());
    modifiedKeys.remove(key.getKeyId());
    removedKeys.put(key.getKeyId(), attachedKey);
    log("removed-blockkey", CacheHitState.NA, new String[]{"kid", Integer.toString(key.getKeyId())});
  }

  @Override
  public void removeAll() throws PersistanceException {
    clear();
    aboutToAccessStorage();
    dataAccess.removeAll();
    log("removed-all-blockkeys");
  }

  @Override
  public void update(BlockKey blockKey) throws PersistanceException {
    if (removedKeys.containsKey(blockKey.getKeyId())) {
      throw new TransactionContextException("Removed blockkey passed to be persisted");
    }
    keys.put(blockKey.getKeyId(), blockKey);
    modifiedKeys.put(blockKey.getKeyId(), blockKey);
    log("updated-blockkey", CacheHitState.NA, new String[]{"kid", Long.toString(blockKey.getKeyId())});
  }

  private Collection<BlockKey> syncBlockKeyInstances(List<BlockKey> newBlockKeys) {
    List<BlockKey> finalList = new ArrayList<BlockKey>();

    for (BlockKey key : newBlockKeys) {
      if (keys.containsKey(key.getKeyId()) && !removedKeys.containsKey(key.getKeyId())) {
        if (keys.get(key.getKeyId()) == null) {
          keys.put(key.getKeyId(), key);
          nullCount--;
        }
        finalList.add(keys.get(key.getKeyId()));
      } else {
        keys.put(key.getKeyId(), key);
        finalList.add(key);
      }
    }

    return finalList;
  }
}
