package se.sics.hop.metadata.context;

import se.sics.hop.metadata.entity.EntityContext;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import se.sics.hop.metadata.entity.CounterType;
import se.sics.hop.metadata.entity.FinderType;
import se.sics.hop.exception.LockUpgradeException;
import se.sics.hop.exception.StorageCallPreventedException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.dal.LeasePathDataAccess;
import se.sics.hop.metadata.entity.hop.HopLeasePath;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.metadata.lock.HDFSTransactionLocks;
import se.sics.hop.transaction.lock.TransactionLocks;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class LeasePathContext extends EntityContext<HopLeasePath> {

  private Map<Integer, Collection<HopLeasePath>> holderLeasePaths = new HashMap<Integer, Collection<HopLeasePath>>();
  private Map<HopLeasePath, HopLeasePath> leasePaths = new HashMap<HopLeasePath, HopLeasePath>();
  private Map<HopLeasePath, HopLeasePath> newLPaths = new HashMap<HopLeasePath, HopLeasePath>();
  private Map<HopLeasePath, HopLeasePath> modifiedLPaths = new HashMap<HopLeasePath, HopLeasePath>();
  private Map<HopLeasePath, HopLeasePath> removedLPaths = new HashMap<HopLeasePath, HopLeasePath>();
  private Map<String, HopLeasePath> pathToLeasePath = new HashMap<String, HopLeasePath>();
  private boolean allLeasePathsRead = false;
  private LeasePathDataAccess<HopLeasePath> dataAccess;

  public LeasePathContext(LeasePathDataAccess<HopLeasePath> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(HopLeasePath lPath) throws PersistanceException {
    if (removedLPaths.containsKey(lPath)) {
      throw new TransactionContextException("Removed lease-path passed to be persisted");
    }

    newLPaths.put(lPath, lPath);
    leasePaths.put(lPath, lPath);
    pathToLeasePath.put(lPath.getPath(), lPath);
    if (allLeasePathsRead) {
      if (holderLeasePaths.containsKey(lPath.getHolderId())) {
        holderLeasePaths.get(lPath.getHolderId()).add(lPath);
      } else {
        TreeSet<HopLeasePath> lSet = new TreeSet<HopLeasePath>();
        lSet.add(lPath);
        holderLeasePaths.put(lPath.getHolderId(), lSet);
      }
    }
    log("added-lpath", CacheHitState.NA,
            new String[]{"path", lPath.getPath(), "hid", Long.toString(lPath.getHolderId())});
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    holderLeasePaths.clear();
    leasePaths.clear();
    newLPaths.clear();
    modifiedLPaths.clear();
    removedLPaths.clear();
    pathToLeasePath.clear();
    allLeasePathsRead = false;
  }

  @Override
  public int count(CounterType counter, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
  }

  @Override
  public HopLeasePath find(FinderType<HopLeasePath> finder, Object... params) throws PersistanceException {
    HopLeasePath.Finder lFinder = (HopLeasePath.Finder) finder;
    HopLeasePath result = null;

    switch (lFinder) {
      case ByPKey:
        String path = (String) params[0];
        if (pathToLeasePath.containsKey(path)) {
          log("find-lpath-by-pk", CacheHitState.HIT, new String[]{"path", path});
          result = pathToLeasePath.get(path);
        } else {
          log("find-lpath-by-pk", CacheHitState.LOSS, new String[]{"path", path});
          aboutToAccessStorage();
          result = dataAccess.findByPKey(path);
          if (result != null) {
            leasePaths.put(result, result);
          }
          pathToLeasePath.put(path, result);
        }
        return result;
    }

    throw new RuntimeException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<HopLeasePath> findList(FinderType<HopLeasePath> finder, Object... params) throws PersistanceException {
    HopLeasePath.Finder lFinder = (HopLeasePath.Finder) finder;
    Collection<HopLeasePath> result = new TreeSet<HopLeasePath>();

    switch (lFinder) {
      case ByHolderId:
        int holderId = (Integer) params[0];
        if (holderLeasePaths.containsKey(holderId)) {
          log("find-lpaths-by-holderid", CacheHitState.HIT, new String[]{"hid", Long.toString(holderId)});
          result = holderLeasePaths.get(holderId);
        } else {
          if (!allLeasePathsRead) {
            log("find-lpaths-by-holderid", CacheHitState.LOSS, new String[]{"hid", Long.toString(holderId)});
            aboutToAccessStorage();
            result = syncLeasePathInstances(dataAccess.findByHolderId(holderId), false);
            holderLeasePaths.put(holderId, result);
          }
        }
        return result;
      case ByPrefix:
        String prefix = (String) params[0];
        try {
          aboutToAccessStorage();
          result = syncLeasePathInstances(dataAccess.findByPrefix(prefix), false);
          log("find-lpaths-by-prefix", CacheHitState.LOSS, new String[]{"prefix", prefix, "numOfLps", String.valueOf(result.size())});
        } catch (StorageCallPreventedException ex) {
          // This is allowed in querying lease-path by prefix, this is needed in delete operation for example.
          result = getCachedLpsByPrefix(prefix);
          log("find-lpaths-by-prefix", CacheHitState.HIT, new String[]{"prefix", prefix, "numOfLps", String.valueOf(result.size())});
        }
        return result;
      case All:
        if (allLeasePathsRead) {
          log("find-all-lpaths", CacheHitState.HIT);
          result = new TreeSet<HopLeasePath>();
          for (HopLeasePath lp : leasePaths.values()) {
            if (lp != null) {
              result.add(lp);
            }
          }
        } else {
          log("find-all-lpaths", CacheHitState.LOSS);
          aboutToAccessStorage();
          result = syncLeasePathInstances(dataAccess.findAll(), true);
          allLeasePathsRead = true;
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
        if ((!removedLPaths.values().isEmpty()
                || !modifiedLPaths.values().isEmpty())
                && hlks.getLpLock()!= TransactionLockTypes.LockType.WRITE) {
            throw new LockUpgradeException("Trying to upgrade lease path locks");
        }
        dataAccess.prepare(removedLPaths.values(), newLPaths.values(), modifiedLPaths.values());
    }

  @Override
  public void remove(HopLeasePath lPath) throws PersistanceException {
    if (leasePaths.remove(lPath) == null) {
      throw new TransactionContextException("Unattached lease-path passed to be removed");
    }

    pathToLeasePath.remove(lPath.getPath());
    newLPaths.remove(lPath);
    modifiedLPaths.remove(lPath);
    if (holderLeasePaths.containsKey(lPath.getHolderId())) {
      Collection<HopLeasePath> lSet = holderLeasePaths.get(lPath.getHolderId());
      lSet.remove(lPath);
      if (lSet.isEmpty()) {
        holderLeasePaths.remove(lPath.getHolderId());
      }
    }
    removedLPaths.put(lPath, lPath);
    log("removed-lpath", CacheHitState.NA, new String[]{"path", lPath.getPath()});
  }

  @Override
  public void removeAll() throws PersistanceException {
    dataAccess.removeAll();
  }

  @Override
  public void update(HopLeasePath lPath) throws PersistanceException {
    if (removedLPaths.containsKey(lPath)) {
      throw new TransactionContextException("Removed lease-path passed to be persisted");
    }

    modifiedLPaths.put(lPath, lPath);
    leasePaths.put(lPath, lPath);
    pathToLeasePath.put(lPath.getPath(), lPath);
    if (allLeasePathsRead) {
      if (holderLeasePaths.containsKey(lPath.getHolderId())) {
        holderLeasePaths.get(lPath.getHolderId()).add(lPath);
      } else {
        TreeSet<HopLeasePath> lSet = new TreeSet<HopLeasePath>();
        lSet.add(lPath);
        holderLeasePaths.put(lPath.getHolderId(), lSet);
      }
    }

    log("updated-lpath", CacheHitState.NA,
            new String[]{"path", lPath.getPath(), "hid", Long.toString(lPath.getHolderId())});
  }

  private TreeSet<HopLeasePath> syncLeasePathInstances(Collection<HopLeasePath> list, boolean allRead) {
    TreeSet<HopLeasePath> finalList = new TreeSet<HopLeasePath>();

    for (HopLeasePath lPath : list) {
      if (!removedLPaths.containsKey(lPath)) {
        if (leasePaths.containsKey(lPath)) {
          if (leasePaths.get(lPath) == null) {
            leasePaths.put(lPath, lPath);
          }
          lPath = leasePaths.get(lPath);
        } else {
          this.leasePaths.put(lPath, lPath);
        }
        if (pathToLeasePath.containsKey(lPath.getPath())) {
          if (pathToLeasePath.get(lPath.getPath()) == null) {
            pathToLeasePath.put(lPath.getPath(), lPath);
          }
        } else {
          pathToLeasePath.put(lPath.getPath(), lPath);
        }
        finalList.add(lPath);
        if (allRead) {
          if (holderLeasePaths.containsKey(lPath.getHolderId())) {
            holderLeasePaths.get(lPath.getHolderId()).add(lPath);
          } else {
            TreeSet<HopLeasePath> lSet = new TreeSet<HopLeasePath>();
            lSet.add(lPath);
            holderLeasePaths.put(lPath.getHolderId(), lSet);
          }
        }
      }
    }

    return finalList;
  }

  private TreeSet<HopLeasePath> getCachedLpsByPrefix(String prefix) {
    TreeSet<HopLeasePath> hits = new TreeSet<HopLeasePath>();
    for (HopLeasePath lp : leasePaths.values()) {
      if (lp.getPath().contains(prefix)) {
        hits.add(lp);
      }
    }

    return hits;
  }
}
