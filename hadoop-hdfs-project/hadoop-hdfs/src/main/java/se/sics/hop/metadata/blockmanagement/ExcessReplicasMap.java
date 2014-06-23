/*
 * Copyright 2013 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package se.sics.hop.metadata.blockmanagement;

import java.io.IOException;
import java.util.Collection;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeManager;
import se.sics.hop.metadata.hdfs.entity.hop.HopExcessReplica;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.transaction.handler.LightWeightRequestHandler;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.metadata.hdfs.dal.ExcessReplicaDataAccess;
import se.sics.hop.metadata.StorageFactory;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;

/**
 *
 * @author Mahmoud Ismail
 */
public class ExcessReplicasMap {

  private final DatanodeManager datanodeManager;
  
  public ExcessReplicasMap(DatanodeManager datanodeManager){
    this.datanodeManager = datanodeManager;
  }
  
  //[M] only needed in TestOverReplicatedBlocks
  public LightWeightLinkedSet<Block> get(String dn) throws IOException {
    Collection<HopExcessReplica> excessReplicas = getExcessReplicas(datanodeManager.getDatanode(dn).getSId());
    if (excessReplicas == null) {
      return null;
    }
    LightWeightLinkedSet<Block> excessBlocks = new LightWeightLinkedSet<Block>();
    for (HopExcessReplica er : excessReplicas) {
      //FIXME: [M] might need to get the blockinfo from the db, but for now we don't need it
      excessBlocks.add(new Block(er.getBlockId()));
    }
    return excessBlocks;
  }

  public boolean put(String dn, BlockInfo excessBlk) throws PersistanceException {
    HopExcessReplica er = getExcessReplica(datanodeManager.getDatanode(dn).getSId(), excessBlk);
    if (er == null) {
      addExcessReplicaToDB(new HopExcessReplica(datanodeManager.getDatanode(dn).getSId(), excessBlk.getBlockId(), excessBlk.getInodeId()));
      return true;
    }
    return false;
  }

  public boolean remove(String dn, BlockInfo block) throws PersistanceException {
    HopExcessReplica er = getExcessReplica(datanodeManager.getDatanode(dn).getSId(), block);
    if (er != null) {
      removeExcessReplicaFromDB(er);
      return true;
    } else {
      return false;
    }
  }

  public Collection<String> get(BlockInfo blk) throws PersistanceException {
    Collection<HopExcessReplica> excessReplicas = getExcessReplicas(blk);
    if (excessReplicas == null) {
      return null;
    }
    TreeSet<String> stIds = new TreeSet<String>();
    for (HopExcessReplica er : excessReplicas) {
      stIds.add(datanodeManager.getDatanode(er.getStorageId()).getStorageID());
    }
    return stIds;
  }

  public boolean contains(String dn, BlockInfo blk) throws PersistanceException {
    Collection<HopExcessReplica> ers = getExcessReplicas(blk);
    if (ers == null) {
      return false;
    }
    return ers.contains(new HopExcessReplica(datanodeManager.getDatanode(dn).getSId(), blk.getBlockId(), blk.getInodeId()));
  }

  public void clear() throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.DEL_ALL_EXCESS_BLKS) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        ExcessReplicaDataAccess da = (ExcessReplicaDataAccess) StorageFactory.getDataAccess(ExcessReplicaDataAccess.class);
        da.removeAll();
        return null;
      }
    }.handle();
  }

  private Collection<HopExcessReplica> getExcessReplicas(final int dn) throws IOException {
    return (Collection<HopExcessReplica>) new LightWeightRequestHandler(HDFSOperationType.GET_EXCESS_RELPLICAS_BY_STORAGEID) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        ExcessReplicaDataAccess da = (ExcessReplicaDataAccess) StorageFactory.getDataAccess(ExcessReplicaDataAccess.class);
        return da.findExcessReplicaByStorageId(dn);
      }
    }.handle();
  }

  private void addExcessReplicaToDB(HopExcessReplica er) throws PersistanceException {
    EntityManager.add(er);
  }

  private void removeExcessReplicaFromDB(HopExcessReplica er) throws PersistanceException {
    EntityManager.remove(er);
  }

  private Collection<HopExcessReplica> getExcessReplicas(BlockInfo blk) throws PersistanceException {
    return EntityManager.findList(HopExcessReplica.Finder.ByBlockId, blk.getBlockId(), blk.getInodeId());
  }

  private HopExcessReplica getExcessReplica(int dn, BlockInfo block) throws PersistanceException {
    return EntityManager.find(HopExcessReplica.Finder.ByPKey, block.getBlockId(), dn, block.getInodeId());
  }
}
