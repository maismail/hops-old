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

  private Map<Integer, EncodingStatus> inodeIdToEncodingStatus = new HashMap<Integer, EncodingStatus>();
  private Map<Integer, EncodingStatus> parityInodeIdToEncodingStatus = new HashMap<Integer, EncodingStatus>();
  private Map<Integer, EncodingStatus> inodeIdToEncodingStatusToAdd = new HashMap<Integer, EncodingStatus>();
  private Map<Integer, EncodingStatus> inodeIdToEncodingStatusToDelete = new HashMap<Integer, EncodingStatus>();
  private Map<Integer, EncodingStatus> inodeIdToEncodingStatusToUpdate = new HashMap<Integer, EncodingStatus>();

  public EncodingStatusContext(EncodingStatusDataAccess<EncodingStatus> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    inodeIdToEncodingStatus.clear();
    parityInodeIdToEncodingStatus.clear();
    inodeIdToEncodingStatusToAdd.clear();
    inodeIdToEncodingStatusToDelete.clear();
    inodeIdToEncodingStatusToUpdate.clear();
  }

  @Override
  public void add(EncodingStatus entity) throws PersistanceException {
    inodeIdToEncodingStatusToAdd.put(entity.getInodeId(), entity);
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

  @Override
  public EncodingStatus find(FinderType<EncodingStatus> finder, Object... params) throws PersistanceException {
    EncodingStatus.Finder eFinder = (EncodingStatus.Finder) finder;
    EncodingStatus result;

    Integer inodeId = (Integer) params[0];
    if (inodeId == null) {
      return null;
    }

    switch (eFinder) {
      case ByInodeId:
        if (inodeIdToEncodingStatus.containsKey(inodeId)) {
          log("find-encoding-status-by-inodeid", CacheHitState.HIT, new String[]{"inodeid", Integer.toString(inodeId)});
          return inodeIdToEncodingStatus.get(inodeId);
        } else {
          log("find-encoding-status-by-inodeid", CacheHitState.LOSS, new String[]{"inodeid", Integer.toString(inodeId)});
          aboutToAccessStorage();
          result = dataAccess.findByInodeId(inodeId);
          inodeIdToEncodingStatus.put(inodeId, result);
          return result;
        }
      case ByParityInodeId:
        if (parityInodeIdToEncodingStatus.containsKey(inodeId)) {
          log("find-encoding-status-by-parityInodeid", CacheHitState.HIT, new String[]{"inodeid", Integer.toString(inodeId)});
          return parityInodeIdToEncodingStatus.get(inodeId);
        } else {
          log("find-encoding-status-by-parityInodeid", CacheHitState.LOSS, new String[]{"inodeid", Integer.toString(inodeId)});
          aboutToAccessStorage();
          result = dataAccess.findByParityInodeId(inodeId);
          parityInodeIdToEncodingStatus.put(inodeId, result);
          return result;
        }
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }

  @Override
  public Collection<EncodingStatus> findList(FinderType<EncodingStatus> finder, Object... params) throws PersistanceException {
    return null;
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
}
