package se.sics.hop.metadata.context;

import se.sics.hop.erasure_coding.EncodingStatus;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.dal.EncodingStatusDataAccess;
import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.metadata.hdfs.entity.EntityContext;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.metadata.hdfs.entity.TransactionContextMaintenanceCmds;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class EncodingStatusContext extends EntityContext<EncodingStatus> {

  private EncodingStatusDataAccess<EncodingStatus> dataAccess;

  private Map<Long, EncodingStatus> inodeIdToEncodingStatus = new HashMap<Long, EncodingStatus>();
  private Map<Long, EncodingStatus> inodeIdToEncodingStatusToAdd = new HashMap<Long, EncodingStatus>();
  private Map<Long, EncodingStatus> inodeIdToEncodingStatusToDelete = new HashMap<Long, EncodingStatus>();
  private Map<Long, EncodingStatus> inodeIdToEncodingStatusToUpdate = new HashMap<Long, EncodingStatus>();

  public EncodingStatusContext(EncodingStatusDataAccess<EncodingStatus> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(EncodingStatus entity) throws PersistanceException {
     inodeIdToEncodingStatusToAdd.put(entity.getInodeId(), entity);
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    inodeIdToEncodingStatus.clear();
    inodeIdToEncodingStatusToAdd.clear();
    inodeIdToEncodingStatusToDelete.clear();
    inodeIdToEncodingStatusToUpdate.clear();
  }

  @Override
  public int count(CounterType<EncodingStatus> counter, Object... params) throws PersistanceException {
    EncodingStatus.Counter eCounter = (EncodingStatus.Counter) counter;
    switch (eCounter) {
      case RequestedEncodings:
        return dataAccess.countRequestedEncodings();
      case ActiveEncodings:
        return dataAccess.countActiveEncodings();
      case ActiveRepairs:
        return dataAccess.countActiveRepairs();
      case Encoded:
        return dataAccess.countEncoded();
      default:
        throw new RuntimeException(UNSUPPORTED_COUNTER);
    }
  }

  @Override
  public EncodingStatus find(FinderType<EncodingStatus> finder, Object... params) throws PersistanceException {
    EncodingStatus.Finder eFinder = (EncodingStatus.Finder) finder;
    EncodingStatus result;

    switch (eFinder) {
      case ByInodeId:
        long inodeId = (Long) params[0];
        if (inodeIdToEncodingStatus.containsKey(inodeId)) {
          log("find-encoding-status-by-inodeid", CacheHitState.HIT, new String[]{"inodeid", Long.toString(inodeId)});
          return inodeIdToEncodingStatus.get(inodeId);
        } else {
          log("find-encoding-status-by-inodeid", CacheHitState.LOSS, new String[]{"inodeid", Long.toString(inodeId)});
          aboutToAccessStorage();
          result = dataAccess.findByInodeId(inodeId);
          inodeIdToEncodingStatus.put(inodeId, result);
          return result;
        }
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }

  @Override
  public Collection<EncodingStatus> findList(FinderType<EncodingStatus> finder, Object... params)
      throws PersistanceException {
    EncodingStatus.Finder eFinder = (EncodingStatus.Finder) finder;

    // TODO STEFFEN - Do I need to synchronize results with the cache to support multiple operations per transaction?
    switch (eFinder) {
      case LimitedByStatusRequestedEncodings:
        return dataAccess.findRequestedEncodings((Long) params[0]);
      case ByStatusActiveEncodings:
        return  dataAccess.findActiveEncodings();
      case ByStatusActiveRepairs:
        return dataAccess.findActiveRepairs();
      case LimitedByStatusEncoded:
        return dataAccess.findEncoded((Long) params[0]);
      case LimitedByStatusRequestedRepair:
        return dataAccess.findRequestedRepairs((Long) params[0]);
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }

  @Override
  public void prepare(TransactionLocks tlm) throws StorageException {
    for (EncodingStatus status : inodeIdToEncodingStatusToAdd.values()) {
      dataAccess.add(status);
    }

    for (EncodingStatus status : inodeIdToEncodingStatusToUpdate.values()) {
      dataAccess.update(status);
    }

    for (EncodingStatus status : inodeIdToEncodingStatusToDelete.values()) {
      dataAccess.delete(status);
    }
  }

  @Override
  public void remove(EncodingStatus entity) throws PersistanceException {
    inodeIdToEncodingStatusToDelete.put(entity.getInodeId(), entity);
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(EncodingStatus entity) throws PersistanceException {
    inodeIdToEncodingStatusToUpdate.put(entity.getInodeId(), entity);
  }

  @Override
  public void snapshotMaintenance(TransactionContextMaintenanceCmds cmds, Object... params)
      throws PersistanceException {

  }
}
