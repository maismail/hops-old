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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.Collection;
import java.util.TreeSet;
import org.apache.hadoop.hdfs.protocol.Block;
import se.sics.hop.metadata.persistence.EntityManager;
import se.sics.hop.metadata.persistence.LightWeightRequestHandler;
import se.sics.hop.metadata.persistence.exceptions.PersistanceException;
import se.sics.hop.metadata.persistence.RequestHandler.OperationType;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.ExcessReplicaDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;
import org.apache.hadoop.hdfs.util.LightWeightLinkedSet;

/**
 *
 * @author Mahmoud Ismail
 */
public class ExcessReplicasMap {

  //[M] only needed in TestOverReplicatedBlocks
  public LightWeightLinkedSet<Block> get(String dn) throws IOException {
    Collection<ExcessReplica> excessReplicas = getExcessReplicas(dn);
    if (excessReplicas == null) {
      return null;
    }
    LightWeightLinkedSet<Block> excessBlocks = new LightWeightLinkedSet<Block>();
    for (ExcessReplica er : excessReplicas) {
      //FIXME: [M] might need to get the blockinfo from the db, but for now we don't need it
      excessBlocks.add(new Block(er.getBlockId()));
    }
    return excessBlocks;
  }

  public boolean put(String dn, Block excessBlk) throws PersistanceException {
    ExcessReplica er = getExcessReplica(dn, excessBlk);
    if (er == null) {
      addExcessReplicaToDB(new ExcessReplica(dn, excessBlk.getBlockId()));
      return true;
    }
    return false;
  }

  public boolean remove(String dn, Block block) throws PersistanceException {
    ExcessReplica er = getExcessReplica(dn, block);
    if (er != null) {
      removeExcessReplicaFromDB(er);
      return true;
    } else {
      return false;
    }
  }

  public Collection<String> get(Block blk) throws PersistanceException {
    Collection<ExcessReplica> excessReplicas = getExcessReplicas(blk);
    if (excessReplicas == null) {
      return null;
    }
    TreeSet<String> stIds = new TreeSet<String>();
    for (ExcessReplica er : excessReplicas) {
      stIds.add(er.getStorageId());
    }
    return stIds;
  }

  public boolean contains(String dn, Block blk) throws PersistanceException {
    Collection<ExcessReplica> ers = getExcessReplicas(blk);
    if (ers == null) {
      return false;
    }
    return ers.contains(new ExcessReplica(dn, blk.getBlockId()));
  }

  public void clear() throws IOException {
    new LightWeightRequestHandler(OperationType.DEL_ALL_EXCESS_BLKS) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        ExcessReplicaDataAccess da = (ExcessReplicaDataAccess) StorageFactory.getDataAccess(ExcessReplicaDataAccess.class);
        da.removeAll();
        return null;
      }
    }.handle(null);
  }

  private Collection<ExcessReplica> getExcessReplicas(final String dn) throws IOException {
    return (Collection<ExcessReplica>) new LightWeightRequestHandler(OperationType.GET_EXCESS_RELPLICAS_BY_STORAGEID) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        ExcessReplicaDataAccess da = (ExcessReplicaDataAccess) StorageFactory.getDataAccess(ExcessReplicaDataAccess.class);
        return da.findExcessReplicaByStorageId(dn);
      }
    }.handle(null);
  }

  private void addExcessReplicaToDB(ExcessReplica er) throws PersistanceException {
    EntityManager.add(er);
  }

  private void removeExcessReplicaFromDB(ExcessReplica er) throws PersistanceException {
    EntityManager.remove(er);
  }

  private Collection<ExcessReplica> getExcessReplicas(Block blk) throws PersistanceException {
    return EntityManager.findList(ExcessReplica.Finder.ByBlockId, blk.getBlockId());
  }

  private ExcessReplica getExcessReplica(String dn, Block block) throws PersistanceException {
    return EntityManager.find(ExcessReplica.Finder.ByPKey, block.getBlockId(), dn);
  }
}
