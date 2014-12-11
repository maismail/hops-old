package se.sics.hop.metadata.context;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.sics.hop.exception.StorageCallPreventedException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.dal.QuotaUpdateDataAccess;
import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.metadata.hdfs.entity.EntityContext;
import se.sics.hop.metadata.hdfs.entity.EntityContextStat;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.metadata.hdfs.entity.TransactionContextMaintenanceCmds;
import se.sics.hop.metadata.hdfs.entity.hop.QuotaUpdate;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QuotaUpdateContext extends EntityContext<QuotaUpdate> {
  public static final Log LOG = LogFactory.getLog(QuotaUpdateContext.class);

  private enum CacheRowStatus {
    ADDED,
    DELETED
  }

  private class QuotaUpdateWrapper {
    private QuotaUpdate quotaUpdate;
    private CacheRowStatus status;

    public QuotaUpdateWrapper(QuotaUpdate quotaUpdate, CacheRowStatus status) {
      this.quotaUpdate = quotaUpdate;
      this.status = status;
    }

    public QuotaUpdate getQuotaUpdate() {
      return quotaUpdate;
    }

    public CacheRowStatus getStatus() {
      return status;
    }

    public void setQuotaUpdate(QuotaUpdate quota) {
      this.quotaUpdate = quota;
    }

    public void setStatus(CacheRowStatus status) {
      this.status = status;
    }

    @Override
    public boolean equals(Object obj) {
      throw new UnsupportedOperationException();
    }
  }

  private Map<Integer, QuotaUpdateWrapper> cachedRows = new HashMap<Integer, QuotaUpdateWrapper>();
  private Map<Integer, List<Integer>> quotaUpdateByInodeId = new HashMap<Integer, List<Integer>>();
  private QuotaUpdateDataAccess<QuotaUpdate> dataAccess;

  public QuotaUpdateContext(QuotaUpdateDataAccess<QuotaUpdate> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void add(QuotaUpdate quotaUpdate) {
    QuotaUpdateWrapper wrapper = new QuotaUpdateWrapper(quotaUpdate, CacheRowStatus.ADDED);
    cachedRows.put(quotaUpdate.getId(), wrapper);
  }

  @Override
  public void clear() {
    storageCallPrevented = false;
    cachedRows.clear();
    quotaUpdateByInodeId.clear();
  }

  @Override
  public int count(CounterType<QuotaUpdate> counter, Object... params) {
    throw new UnsupportedOperationException();
  }

  @Override
  public QuotaUpdate find(FinderType<QuotaUpdate> finder, Object... params) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<QuotaUpdate> findList(FinderType<QuotaUpdate> finder, Object... params)
      throws StorageCallPreventedException, StorageException {
    Integer inodeId = (Integer) params[0];
    QuotaUpdate.Finder qFinder = (QuotaUpdate.Finder) finder;
    List<QuotaUpdate> list = new ArrayList<QuotaUpdate>();
    switch (qFinder) {
      case ByInodeId:
        if (!quotaUpdateByInodeId.containsKey(inodeId)) {
          aboutToAccessStorage();
          List<QuotaUpdate> updates = dataAccess.findByInodeId(inodeId);
          List<Integer> ids = new ArrayList<Integer>();
          for (QuotaUpdate update : updates) {
            add(update);
            ids.add(update.getId());
          }
          quotaUpdateByInodeId.put(inodeId, ids);
        } else {
          for (Integer id : quotaUpdateByInodeId.get(inodeId)) {
            list.add(cachedRows.get(id).getQuotaUpdate());
          }
        }
        break;
    }
    return list;
  }

  @Override
  public void prepare(TransactionLocks lks) throws StorageException {
    List<QuotaUpdate> added = new ArrayList<QuotaUpdate>();
    List<QuotaUpdate> deleted = new ArrayList<QuotaUpdate>();
    for (QuotaUpdateWrapper wrapper : cachedRows.values()) {
      if (wrapper.getStatus() == CacheRowStatus.DELETED) {
        if (wrapper.getQuotaUpdate() != null) {
          deleted.add(wrapper.getQuotaUpdate());
        }
      } else if (wrapper.getStatus() == CacheRowStatus.ADDED) {
        if (wrapper.getQuotaUpdate() != null) {
          added.add(wrapper.getQuotaUpdate());
        }
      }
    }
    dataAccess.prepare(added, deleted);
  }

  @Override
  public void remove(QuotaUpdate quotaUpdate) {
    if (cachedRows.containsKey(quotaUpdate.getId())) {
      cachedRows.get(quotaUpdate.getId()).setStatus(CacheRowStatus.DELETED);
      log("removed-quotaUpdate", CacheHitState.NA, new String[]{"id", Integer.toString(quotaUpdate.getId())});
    } else {
      cachedRows.put(quotaUpdate.getId(), new QuotaUpdateWrapper(quotaUpdate, CacheRowStatus.DELETED));
      log("removed-quotaUpdate", CacheHitState.NA, new String[]{"id", Integer.toString(quotaUpdate.getId())});
    }
  }

  @Override
  public void removeAll() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void update(QuotaUpdate quotaUpdate) {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public EntityContextStat collectSnapshotStat() {
    List<QuotaUpdate> added = new ArrayList<QuotaUpdate>();
    List<QuotaUpdate> deleted = new ArrayList<QuotaUpdate>();
    for (QuotaUpdateWrapper wrapper : cachedRows.values()) {
      if (wrapper.getStatus() == CacheRowStatus.DELETED) {
        if (wrapper.getQuotaUpdate() != null) {
          deleted.add(wrapper.getQuotaUpdate());
        }
      } else if (wrapper.getStatus() == CacheRowStatus.ADDED) {
        if (wrapper.getQuotaUpdate() != null) {
          added.add(wrapper.getQuotaUpdate());
        }
      }
    }
    EntityContextStat stat = new EntityContextStat("INode Attributes", added.size(), 0, deleted.size());
    return stat;
  }
  
  @Override
  public void snapshotMaintenance(TransactionContextMaintenanceCmds cmds, Object... params) {

  }
}
