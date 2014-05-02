package se.sics.hop.metadata.context;

import se.sics.hop.metadata.hdfs.entity.EntityContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import se.sics.hop.metadata.hdfs.entity.CounterType;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.exception.PersistanceException;
import static se.sics.hop.metadata.hdfs.entity.EntityContext.log;
import se.sics.hop.metadata.hdfs.dal.INodeAttributesDataAccess;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.EntityContextStat;
import se.sics.hop.metadata.hdfs.entity.TransactionContextMaintenanceCmds;
import se.sics.hop.transaction.lock.TransactionLocks;

/**
 *
 * @author Salman Niazi <salman@sics.se>
 * this is one way how all the caches should have been implemented
 */
public class INodeAttributesContext extends EntityContext<INodeAttributes> {

  private enum CacheRowStatus {

    UN_MODIFIED,
    MODIFIED, //modification and new are same in our case
    DELETED
  }

  private class AttributeWrapper {

    private INodeAttributes attributes;
    private CacheRowStatus status;

    public AttributeWrapper(INodeAttributes quota, CacheRowStatus status) {
      this.attributes = quota;
      this.status = status;
    }

    public INodeAttributes getAttributes() {
      return attributes;
    }

    public CacheRowStatus getStatus() {
      return status;
    }

    public void setAttributes(INodeAttributes quota) {
      this.attributes = quota;
    }

    public void setStatus(CacheRowStatus status) {
      this.status = status;
    }

    @Override
    public boolean equals(Object obj) {
      throw new UnsupportedOperationException("Implement it if you want this functionality");
    }
  }
  private Map<Integer, AttributeWrapper> cachedRows = new HashMap<Integer, AttributeWrapper>();
  private INodeAttributesDataAccess<INodeAttributes> da;

  public INodeAttributesContext(INodeAttributesDataAccess<INodeAttributes> da) {
    this.da = da;
  }

  @Override
  public void add(INodeAttributes entity) throws PersistanceException {
    //even if it already exists overwite it with modified flag
    AttributeWrapper wrapper = new AttributeWrapper(entity, CacheRowStatus.MODIFIED);
    cachedRows.put(entity.getInodeId(), wrapper);
  }

  @Override
  public void clear() {
    log("CLEARING THE INODE ATTRIBUTES CONTEXT");
    storageCallPrevented = false;
    cachedRows.clear();
  }

  @Override
  public int count(CounterType<INodeAttributes> counter, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public INodeAttributes find(FinderType<INodeAttributes> finder, Object... params) throws PersistanceException {
    INodeAttributes.Finder qfinder = (INodeAttributes.Finder) finder;
    

    switch (qfinder) {
      case ByPKey:
        Integer inodeId = (Integer) params[0];
        Integer partKey = (Integer) params[1];
        if (cachedRows.containsKey(inodeId)) {
          log("find-attributes-by-pk", EntityContext.CacheHitState.HIT, new String[]{"id", Integer.toString(inodeId), "partKey", Integer.toString(partKey)});
          return cachedRows.get(inodeId).getAttributes();
        } else {
          log("find-attributes-by-pk", EntityContext.CacheHitState.LOSS, new String[]{"id", Integer.toString(inodeId), "partKey", Integer.toString(partKey)});
          aboutToAccessStorage(" id = " + inodeId);
          INodeAttributes quota = da.findAttributesByPk(inodeId,partKey);
          //dont worry if it is null. 
          AttributeWrapper wrapper = new AttributeWrapper(quota, CacheRowStatus.UN_MODIFIED);
          cachedRows.put(inodeId, wrapper);
          return quota;
        }
    }

    throw new UnsupportedOperationException("Finder not supported");
  }

  @Override
  public Collection<INodeAttributes> findList(FinderType<INodeAttributes> finder, Object... params) throws PersistanceException {
    INodeAttributes.Finder qfinder = (INodeAttributes.Finder) finder;
    Map<Integer, Integer> inodes = (Map<Integer/*inodeid*/, Integer/*partKey*/>) params[0];
    switch (qfinder) {
      case ByPKList: //only used for batch reading
        boolean allDataRead = true;
        for(Integer inodeId : inodes.keySet()){
          if (!cachedRows.containsKey(inodeId)) {
            allDataRead = false;
            break;
          }
        }
        if (allDataRead) {
          log("find-attributes-by-pk-list", EntityContext.CacheHitState.HIT, new String[]{"id", Arrays.toString(inodes.keySet().toArray())});
          List<INodeAttributes> retQuotaList = new ArrayList<INodeAttributes>();
          for(Integer inodeId : inodes.keySet()) {
            retQuotaList.add(cachedRows.get(inodeId).getAttributes());
          }
          return retQuotaList;
        } else {
          log("find-attributes-by-pk-list", EntityContext.CacheHitState.LOSS, new String[]{"id", Arrays.toString(inodes.keySet().toArray())});
          aboutToAccessStorage(" ids = " + Arrays.toString(inodes.keySet().toArray()));
          List<INodeAttributes> quotaList = (List<INodeAttributes>) da.findAttributesByPkList(inodes);
          for (int i = 0; i < quotaList.size(); i++) {
            INodeAttributes quota = quotaList.get(i);
            AttributeWrapper wrapper = new AttributeWrapper(quota, CacheRowStatus.UN_MODIFIED);
            cachedRows.put(quota.getInodeId(), wrapper);
          }
          return quotaList;
        }
    }
    throw new UnsupportedOperationException("Finder not supported");
  }

  @Override
  public void prepare(TransactionLocks lks) throws StorageException {
    //there will be no checking for locks
    List<INodeAttributes> modified = new ArrayList<INodeAttributes>();
    List<INodeAttributes> deleted = new ArrayList<INodeAttributes>();
    for (AttributeWrapper wrapper : cachedRows.values()) {
      if (wrapper.getStatus() == CacheRowStatus.DELETED) {
        if (wrapper.getAttributes() != null) {
          deleted.add(wrapper.getAttributes());
        }
      } else if (wrapper.getStatus() == CacheRowStatus.MODIFIED) {
        if (wrapper.getAttributes() != null) {
          modified.add(wrapper.getAttributes());
        }
      }
    }
    da.prepare(modified, deleted);
  }

  @Override
  public void remove(INodeAttributes var) throws PersistanceException {
    if (cachedRows.containsKey(var.getInodeId())) {
      cachedRows.get(var.getInodeId()).setStatus(CacheRowStatus.DELETED);
      log("removed-attributes", CacheHitState.NA, new String[]{"id", Integer.toString(var.getInodeId())});
    } else {
      throw new UnsupportedOperationException("Removing a row that is not in the cache");
    }
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(INodeAttributes var) throws PersistanceException {

    if (var.getInodeId() == INode.NON_EXISTING_ID) {
      log("updated-attributes -- IGNORED as id is not set");
    } else {
      AttributeWrapper attrWrapper = new AttributeWrapper(var, CacheRowStatus.MODIFIED);
      cachedRows.put(var.getInodeId(), attrWrapper);
      log("updated-attributes", CacheHitState.NA, new String[]{"id", Integer.toString(var.getInodeId())});
    }
  }
  
  @Override
  public EntityContextStat collectSnapshotStat() throws PersistanceException {
        //there will be no checking for locks
    List<INodeAttributes> modified = new ArrayList<INodeAttributes>();
    List<INodeAttributes> deleted = new ArrayList<INodeAttributes>();
    for (AttributeWrapper wrapper : cachedRows.values()) {
      if (wrapper.getStatus() == CacheRowStatus.DELETED) {
        if (wrapper.getAttributes() != null) {
          deleted.add(wrapper.getAttributes());
        }
      } else if (wrapper.getStatus() == CacheRowStatus.MODIFIED) {
        if (wrapper.getAttributes() != null) {
          modified.add(wrapper.getAttributes());
        }
      }
    }
    EntityContextStat stat = new EntityContextStat("INode Attributes",0,modified.size(),deleted.size());
    return stat;
  }
  
  @Override
  public void snapshotMaintenance(TransactionContextMaintenanceCmds cmds, Object... params) throws PersistanceException {
  }
}
