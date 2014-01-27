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
package se.sics.hop.metadata.adaptor;

import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import se.sics.hop.metadata.DALAdaptor;
import se.sics.hop.metadata.dal.ReplicaUnderConstructionDataAccess;
import se.sics.hop.metadata.entity.hdfs.HopReplicaUnderConstruction;
import se.sics.hop.exception.StorageException;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class ReplicaUnderConstructionDALAdaptor extends DALAdaptor<ReplicaUnderConstruction, HopReplicaUnderConstruction> implements ReplicaUnderConstructionDataAccess<ReplicaUnderConstruction> {

  private final ReplicaUnderConstructionDataAccess<HopReplicaUnderConstruction> dataAccces;

  public ReplicaUnderConstructionDALAdaptor(ReplicaUnderConstructionDataAccess<HopReplicaUnderConstruction> dataAccess) {
    this.dataAccces = dataAccess;
  }

  @Override
  public List<ReplicaUnderConstruction> findReplicaUnderConstructionByBlockId(long blockId) throws StorageException {
    return (List<ReplicaUnderConstruction>) convertDALtoHDFS(dataAccces.findReplicaUnderConstructionByBlockId(blockId));
  }

  @Override
  public void prepare(Collection<ReplicaUnderConstruction> removed, Collection<ReplicaUnderConstruction> newed, Collection<ReplicaUnderConstruction> modified) throws StorageException {
    dataAccces.prepare(convertHDFStoDAL(removed), convertHDFStoDAL(newed), convertHDFStoDAL(modified));
  }

  @Override
  public HopReplicaUnderConstruction convertHDFStoDAL(ReplicaUnderConstruction hdfsClass) throws StorageException {
    if (hdfsClass != null) {
      return new HopReplicaUnderConstruction(hdfsClass.getState().ordinal(), hdfsClass.getStorageId(), hdfsClass.getBlockId(), hdfsClass.getIndex());
    } else {
      return null;
    }
  }

  @Override
  public ReplicaUnderConstruction convertDALtoHDFS(HopReplicaUnderConstruction dalClass) throws StorageException {
    if (dalClass != null) {
      return new ReplicaUnderConstruction(HdfsServerConstants.ReplicaState.values()[dalClass.getState()], dalClass.getStorageId(), dalClass.getBlockId(), dalClass.getIndex());
    } else {
      return null;
    }

  }
}
