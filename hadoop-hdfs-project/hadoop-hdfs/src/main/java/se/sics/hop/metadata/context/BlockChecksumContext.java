package se.sics.hop.metadata.context;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.dal.BlockChecksumDataAccess;
import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.metadata.hdfs.entity.EntityContext;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.metadata.hdfs.entity.TransactionContextMaintenanceCmds;
import se.sics.hop.metadata.hdfs.entity.hop.BlockChecksum;
import se.sics.hop.transaction.lock.OldTransactionLocks;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;

import static se.sics.hop.metadata.hdfs.dal.BlockChecksumDataAccess.KeyTuple;

public class BlockChecksumContext extends EntityContext<BlockChecksum> {
  private BlockChecksumDataAccess<BlockChecksum> dataAccess;

  private Map<KeyTuple, BlockChecksum> blockChecksums = new HashMap<KeyTuple, BlockChecksum>();
  private Map<KeyTuple, BlockChecksum> added = new HashMap<KeyTuple, BlockChecksum>();
  private Map<KeyTuple, BlockChecksum> updated = new HashMap<KeyTuple, BlockChecksum>();
  private Map<KeyTuple, BlockChecksum> deleted = new HashMap<KeyTuple, BlockChecksum>();
  private Map<Integer, Collection<BlockChecksum>> collections = new HashMap<Integer, Collection<BlockChecksum>>();

  public BlockChecksumContext(BlockChecksumDataAccess<BlockChecksum> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    blockChecksums.clear();
    added.clear();
    updated.clear();
    deleted.clear();
    collections.clear();
  }

  @Override
  public int count(CounterType<BlockChecksum> counter, Object... params) throws PersistanceException {
    throw new NotImplementedException();
  }

  @Override
  public void add(BlockChecksum blockChecksum) throws PersistanceException {
    added.put(new KeyTuple(blockChecksum.getInodeId(), blockChecksum.getBlockIndex()), blockChecksum);
  }

  @Override
  public void remove(BlockChecksum blockChecksum) throws PersistanceException {
    deleted.put(new KeyTuple(blockChecksum.getInodeId(), blockChecksum.getBlockIndex()), blockChecksum);
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(BlockChecksum blockChecksum) throws PersistanceException {
    updated.put(new KeyTuple(blockChecksum.getInodeId(), blockChecksum.getBlockIndex()), blockChecksum);
  }

  @Override
  public void snapshotMaintenance(TransactionContextMaintenanceCmds cmds, Object... params)
      throws PersistanceException {
  }

  @Override
  public BlockChecksum find(FinderType<BlockChecksum> finder, Object... params) throws PersistanceException {
    BlockChecksum.Finder eFinder = (BlockChecksum.Finder) finder;
    BlockChecksum result;

    KeyTuple key = (KeyTuple) params[0];
    if (key == null) {
      return null;
    }

    switch (eFinder) {
      case ByKeyTuple:
        if (blockChecksums.containsKey(key)) {
          log("find-block-checksum-by-keyTuple", CacheHitState.HIT, new String[]{"KeyTuple", key.toString()});
          return blockChecksums.get(key);
        } else {
          log("find-block-checksum-by-keyTuple", CacheHitState.LOSS, new String[]{"KeyTuple", key.toString()});
          aboutToAccessStorage();
          result = dataAccess.find(key.getInodeId(), key.getBlockIndex());
          blockChecksums.put(key, result);
          return result;
        }
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }

  @Override
  public Collection<BlockChecksum> findList(FinderType<BlockChecksum> finder, Object... params)
      throws PersistanceException {
    BlockChecksum.Finder eFinder = (BlockChecksum.Finder) finder;

    switch (eFinder) {
      case ByInodeId:
        Integer inodeId = (Integer) params[0];
        if (collections.containsKey(inodeId)) {
          return collections.get(inodeId);
        } else {
          aboutToAccessStorage();
          Collection<BlockChecksum> result = dataAccess.findAll((Integer) params[0]);
          collections.put(inodeId, result);
          sync(result);
          return result;
        }
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }

  @Override
  public void prepare(OldTransactionLocks tlm) throws StorageException {
    for (BlockChecksum blockChecksum : added.values()) {
      dataAccess.add(blockChecksum);
    }
    for (BlockChecksum blockChecksum : updated.values()) {
      dataAccess.update(blockChecksum);
    }
    for (BlockChecksum blockChecksum : deleted.values()) {
      dataAccess.delete(blockChecksum);
    }
  }

  private List<BlockChecksum> sync(Collection<BlockChecksum> blockChecksums) {
    List<BlockChecksum> finalList = new ArrayList<BlockChecksum>();
    for (BlockChecksum blockChecksum : blockChecksums) {
      KeyTuple key = new KeyTuple(blockChecksum.getInodeId(), blockChecksum.getBlockIndex());
      if (collections.containsKey(key)) {
        if (this.blockChecksums.get(key) == null) {
          this.blockChecksums.put(key, blockChecksum);
        }
        finalList.add(this.blockChecksums.get(key));
      } else {
        this.blockChecksums.put(key, blockChecksum);
        finalList.add(blockChecksum);
      }
    }
    return finalList;
  }
}
