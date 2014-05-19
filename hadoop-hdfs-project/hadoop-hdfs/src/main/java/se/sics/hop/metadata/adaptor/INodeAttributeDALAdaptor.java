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
import java.util.Map;
import org.apache.hadoop.hdfs.server.namenode.INodeAttributes;
import se.sics.hop.metadata.DALAdaptor;
import se.sics.hop.metadata.hdfs.dal.INodeAttributesDataAccess;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINodeAttributes;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINodeCandidatePK;

/**
 *
 * @author salman
 */
public class INodeAttributeDALAdaptor extends DALAdaptor<INodeAttributes, HopINodeAttributes> implements INodeAttributesDataAccess<INodeAttributes> {

  private INodeAttributesDataAccess<HopINodeAttributes> dataAccess;

  public INodeAttributeDALAdaptor(INodeAttributesDataAccess<HopINodeAttributes> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public INodeAttributes findAttributesByPk(Integer inodeId) throws StorageException {
    return convertDALtoHDFS(dataAccess.findAttributesByPk(inodeId));
  }

  @Override
  public void prepare(Collection<INodeAttributes> modified, Collection<INodeAttributes> removed) throws StorageException {
    dataAccess.prepare(convertHDFStoDAL(modified), convertHDFStoDAL(removed));

  }

  @Override
  public HopINodeAttributes convertHDFStoDAL(INodeAttributes attribute) throws StorageException {
    if (attribute != null) {
      HopINodeAttributes hia = new HopINodeAttributes(
              attribute.getInodeId(),
              attribute.getNsQuota(),
              attribute.getNsCount(),
              attribute.getDsQuota(),
              attribute.getDiskspace());
      return hia;
    } else {
      return null;
    }
  }

  @Override
  public INodeAttributes convertDALtoHDFS(HopINodeAttributes hia) throws StorageException {
    if (hia != null) {
      INodeAttributes iNodeAttributes = new INodeAttributes(
              hia.getInodeId(),
              hia.getNsQuota(),
              hia.getNsCount(),
              hia.getDsQuota(),
              hia.getDiskspace());
      return iNodeAttributes;
    } else {
      return null;
    }
  }

  @Override
  public Collection<INodeAttributes> findAttributesByPkList(List<HopINodeCandidatePK> inodePks) throws StorageException {
    return convertDALtoHDFS(dataAccess.findAttributesByPkList(inodePks));
  }
}
