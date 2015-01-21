/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package se.sics.hop.transaction.context;

import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import se.sics.hop.exception.StorageCallPreventedException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.dal.INodeAttributesDataAccess;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINodeCandidatePK;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class INodeAttributesContext extends BaseEntityContext<Integer,
    INodeAttributes> {

  private final INodeAttributesDataAccess<INodeAttributes> dataAccess;

  public INodeAttributesContext(
      INodeAttributesDataAccess<INodeAttributes> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public void update(INodeAttributes iNodeAttributes)
      throws TransactionContextException {
    if (iNodeAttributes.getInodeId() != INode.NON_EXISTING_ID) {
      super.update(iNodeAttributes);
      log("updated-attributes", "id", iNodeAttributes.getInodeId());
    } else {
      log("updated-attributes -- IGNORED as id is not set");
    }
  }

  @Override
  public void remove(INodeAttributes iNodeAttributes)
      throws TransactionContextException {
    super.remove(iNodeAttributes);
    log("removed-attributes", "id", iNodeAttributes.getInodeId());
  }

  @Override
  public INodeAttributes find(FinderType<INodeAttributes> finder,
      Object... params) throws TransactionContextException, StorageException {
    INodeAttributes.Finder qfinder = (INodeAttributes.Finder) finder;
    switch (qfinder) {
      case ByINodeId:
        return findByPrimaryKey(qfinder, params);
    }
    throw new UnsupportedOperationException(UNSUPPORTED_FINDER);
  }

  @Override
  public Collection<INodeAttributes> findList(
      FinderType<INodeAttributes> finder, Object... params)
      throws TransactionContextException, StorageException {
    INodeAttributes.Finder qfinder = (INodeAttributes.Finder) finder;
    switch (qfinder) {
      case ByINodeIds:
        return findByPrimaryKeys(qfinder, params);
    }
    throw new UnsupportedOperationException(UNSUPPORTED_FINDER);
  }

  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    Collection<INodeAttributes> modified = new ArrayList<INodeAttributes>
        (getModified());
    modified.addAll(getAdded());
    dataAccess.prepare(modified, getRemoved());
  }

  @Override
  Integer getKey(INodeAttributes iNodeAttributes) {
    return iNodeAttributes.getInodeId();
  }

  @Override
  public void snapshotMaintenance(TransactionContextMaintenanceCmds cmds,
      Object... params) throws TransactionContextException {
    HOPTransactionContextMaintenanceCmds hopCmds =
        (HOPTransactionContextMaintenanceCmds) cmds;
    switch (hopCmds) {
      case INodePKChanged:
        // need to update the rows with updated inodeId or partKey
        INode inodeBeforeChange = (INode) params[0];
        INode inodeAfterChange = (INode) params[1];
        break;
      case Concat:
        HopINodeCandidatePK trg_param = (HopINodeCandidatePK) params[0];
        List<HopINodeCandidatePK> srcs_param =
            (List<HopINodeCandidatePK>) params[1];
        List<BlockInfo> oldBlks = (List<BlockInfo>) params[2];
        updateAttributes(trg_param, srcs_param);
        break;
    }
  }

  private INodeAttributes findByPrimaryKey(INodeAttributes.Finder qfinder,
      Object[] params)
      throws StorageCallPreventedException, StorageException {
    final int inodeId = (Integer) params[0];
    INodeAttributes result = null;
    if (contains(inodeId)) {
      result = get(inodeId);
      hit(qfinder, result, "inodeid", inodeId);
    } else {
      aboutToAccessStorage(" id = " + inodeId);
      result = dataAccess.findAttributesByPk(inodeId);
      gotFromDB(inodeId, result);
      miss(qfinder, result, "inodeid", inodeId, "size", size());
    }
    return result;
  }

  private Collection<INodeAttributes> findByPrimaryKeys(INodeAttributes
      .Finder qfinder, Object[] params)
      throws StorageCallPreventedException, StorageException {
    final List<HopINodeCandidatePK> inodePks = (List<HopINodeCandidatePK>)
        params[0];
    Collection<INodeAttributes> result = null;
    if (contains(inodePks)) {
      result = get(inodePks);
      hit(qfinder, result, "inodeids", inodePks);
    } else {
      aboutToAccessStorage(" ids = " + Arrays.toString(inodePks.toArray()));
      result = dataAccess.findAttributesByPkList(inodePks);
      gotFromDB(result);
      miss(qfinder, result, "inodeids", inodePks);
    }
    return result;
  }


  private boolean contains(List<HopINodeCandidatePK> iNodeCandidatePKs) {
    for (HopINodeCandidatePK pk : iNodeCandidatePKs) {
      if (!contains(pk.getInodeId())) {
        return false;
      }
    }
    return true;
  }

  private Collection<INodeAttributes> get(List<HopINodeCandidatePK>
      iNodeCandidatePKs) {
    Collection<INodeAttributes> iNodeAttributeses = new
        ArrayList<INodeAttributes>(iNodeCandidatePKs.size());
    for (HopINodeCandidatePK pk : iNodeCandidatePKs) {
      iNodeAttributeses.add(get(pk.getInodeId()));
    }
    return iNodeAttributeses;
  }

  private void updateAttributes(HopINodeCandidatePK trg_param,
      List<HopINodeCandidatePK> toBeDeletedSrcs)
      throws TransactionContextException {
    toBeDeletedSrcs.remove(trg_param);
    for (HopINodeCandidatePK src : toBeDeletedSrcs) {
      if (contains(src.getInodeId())) {
        INodeAttributes toBeDeleted = get(src.getInodeId());
        INodeAttributes toBeAdded = clone(toBeDeleted, trg_param.getInodeId());

        remove(toBeDeleted);
        log("snapshot-maintenance-removed-inode-attribute", "inodeId",
            toBeDeleted.getInodeId());

        add(toBeAdded);
        log("snapshot-maintenance-added-inode-attribute", "inodeId",
            toBeAdded.getInodeId());
      }
    }
  }

  private INodeAttributes clone(INodeAttributes src, int inodeId) {
    return new INodeAttributes(inodeId, src.getNsQuota(), src.getNsCount(), src
        .getDsQuota(), src.getDiskspace());
  }

}
