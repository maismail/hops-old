package se.sics.hop.metadata.context;

import se.sics.hop.metadata.hdfs.entity.EntityContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
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
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINodeCandidatePK;
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
  private Map<HopINodeCandidatePK, AttributeWrapper> cachedRows = new HashMap<HopINodeCandidatePK, AttributeWrapper>();
  private INodeAttributesDataAccess<INodeAttributes> da;

  public INodeAttributesContext(INodeAttributesDataAccess<INodeAttributes> da) {
    this.da = da;
  }

  @Override
  public void add(INodeAttributes entity) throws PersistanceException {
    //even if it already exists overwite it with modified flag
    AttributeWrapper wrapper = new AttributeWrapper(entity, CacheRowStatus.MODIFIED);
    HopINodeCandidatePK pk = new HopINodeCandidatePK(entity.getInodeId());
    cachedRows.put(pk, wrapper);
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
        HopINodeCandidatePK pk = new HopINodeCandidatePK(inodeId);
        if (cachedRows.containsKey(pk)) {
          log("find-attributes-by-pk", EntityContext.CacheHitState.HIT, new String[]{"id", Integer.toString(inodeId)});
          return cachedRows.get(pk).getAttributes();
        } else {
          log("find-attributes-by-pk", EntityContext.CacheHitState.LOSS, new String[]{"id", Integer.toString(inodeId), "size ", Integer.toString(cachedRows.size())});
          System.out.println("Keys  "+Arrays.toString(cachedRows.keySet().toArray())+" is eq "+pk.toString());
          aboutToAccessStorage(" id = " + inodeId);
          
          INodeAttributes quota = da.findAttributesByPk(inodeId);
          //dont worry if it is null. 
          AttributeWrapper wrapper = new AttributeWrapper(quota, CacheRowStatus.UN_MODIFIED);
          cachedRows.put(pk, wrapper);
          return quota;
        }
    }

    throw new UnsupportedOperationException("Finder not supported");
  }

  @Override
  public Collection<INodeAttributes> findList(FinderType<INodeAttributes> finder, Object... params) throws PersistanceException {
    INodeAttributes.Finder qfinder = (INodeAttributes.Finder) finder;
    List<HopINodeCandidatePK> inodePks = (List<HopINodeCandidatePK>) params[0];
    switch (qfinder) {
      case ByPKList: //only used for batch reading
        boolean allDataRead = true;
        for(HopINodeCandidatePK inodePk : inodePks){
          if (!cachedRows.containsKey(inodePk)) {
            allDataRead = false;
            break;
          }
        }
        if (allDataRead) {
          log("find-attributes-by-pk-list", EntityContext.CacheHitState.HIT, new String[]{"id", Arrays.toString(inodePks.toArray())});
          List<INodeAttributes> retQuotaList = new ArrayList<INodeAttributes>();
          for(HopINodeCandidatePK inodePk : inodePks) {
            retQuotaList.add(cachedRows.get(inodePk).getAttributes());
          }
          return retQuotaList;
        } else {
          log("find-attributes-by-pk-list", EntityContext.CacheHitState.LOSS, new String[]{"id", Arrays.toString(inodePks.toArray())});
          aboutToAccessStorage(" ids = " + Arrays.toString(inodePks.toArray()));
          List<INodeAttributes> quotaList = (List<INodeAttributes>) da.findAttributesByPkList(inodePks);
          for (int i = 0; i < quotaList.size(); i++) {
            INodeAttributes quota = quotaList.get(i);
            AttributeWrapper wrapper = new AttributeWrapper(quota, CacheRowStatus.UN_MODIFIED);
            HopINodeCandidatePK pk = new HopINodeCandidatePK(quota.getInodeId());
            cachedRows.put(pk, wrapper);
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
    HopINodeCandidatePK pk = new HopINodeCandidatePK(var.getInodeId());
    if (cachedRows.containsKey(pk)) {
      cachedRows.get(pk).setStatus(CacheRowStatus.DELETED);
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
      HopINodeCandidatePK pk = new HopINodeCandidatePK(var.getInodeId());
      cachedRows.put(pk, attrWrapper);
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
    HOPTransactionContextMaintenanceCmds hopCmds = (HOPTransactionContextMaintenanceCmds) cmds;
    switch (hopCmds) {
      case INodePKChanged:
          // need to update the rows with updated inodeId or partKey
        checkForSnapshotChange();        
        INode inodeBeforeChange = (INode) params[0];
        INode inodeAfterChange  = (INode) params[1];
        if (inodeBeforeChange.getLocalName().equals(inodeAfterChange.getLocalName()) ==  false){
          log("snapshot-maintenance-inode-attributes-pk-change", CacheHitState.NA, new String[]{"Before inodeId", Integer.toString(inodeBeforeChange.getId()), "name", inodeBeforeChange.getLocalName(), "pid", Integer.toString(inodeBeforeChange.getParentId()),"After inodeId", Integer.toString(inodeAfterChange.getId()), "name", inodeAfterChange.getLocalName(), "pid", Integer.toString(inodeAfterChange.getParentId()) });
          List<HopINodeCandidatePK> deletedINodesPK = new ArrayList<HopINodeCandidatePK>();
          deletedINodesPK.add(new HopINodeCandidatePK(inodeBeforeChange.getId()));
          updateAttributes(new HopINodeCandidatePK(inodeAfterChange.getId()), deletedINodesPK);
        }
        break;
      case Concat:
        checkForSnapshotChange();
        HopINodeCandidatePK trg_param = (HopINodeCandidatePK)params[0];
        List<HopINodeCandidatePK> srcs_param = (List<HopINodeCandidatePK>)params[1];
        List<BlockInfo> oldBlks  = (List<BlockInfo>)params[2];
        updateAttributes(trg_param, srcs_param);
        break;
    }
  }
  
  private void checkForSnapshotChange(){
  
  }
  
  private void updateAttributes(HopINodeCandidatePK trg_param, List<HopINodeCandidatePK> toBeDeletedSrcs){
    
    Map<HopINodeCandidatePK, AttributeWrapper> toBeAddedList = new HashMap<HopINodeCandidatePK, AttributeWrapper>();
    
      for(HopINodeCandidatePK key : cachedRows.keySet()){
        if(!trg_param.equals(key) && toBeDeletedSrcs.contains(key)){
          INodeAttributes toBeDeleted = cachedRows.get(key).getAttributes();
          INodeAttributes toBeAdded = cloneINodeAttributeObj(cachedRows.get(key).getAttributes());
          
          cachedRows.get(key).setStatus(CacheRowStatus.DELETED);
          log("snapshot-maintenance-removed-inode-attribute",CacheHitState.NA, new String[]{"inodeId", Integer.toString(toBeDeleted.getInodeId())});
          
          //both inode id and partKey has changed
          toBeAdded.setInodeIdNoPersistance(trg_param.getInodeId());
          HopINodeCandidatePK newPK = new HopINodeCandidatePK(toBeAdded.getInodeId());
          toBeAddedList.put(newPK,  new AttributeWrapper(toBeAdded, CacheRowStatus.MODIFIED));
          
          log("snapshot-maintenance-added-inode-attribute",CacheHitState.NA, new String[]{"inodeId", Integer.toString(toBeAdded.getInodeId())});
        }
      }
      
      for(HopINodeCandidatePK key : toBeAddedList.keySet()){
        cachedRows.put(key, toBeAddedList.get(key));
      }
    
  }
  
  private INodeAttributes cloneINodeAttributeObj(INodeAttributes src){
    return new INodeAttributes(src.getInodeId(),src.getNsQuota(),src.getNsCount(),src.getDsQuota(),src.getDiskspace());
  }
}
