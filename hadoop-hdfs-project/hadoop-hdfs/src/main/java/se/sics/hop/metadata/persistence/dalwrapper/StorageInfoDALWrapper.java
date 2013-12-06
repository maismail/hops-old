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
package se.sics.hop.metadata.persistence.dalwrapper;

import org.apache.hadoop.hdfs.server.common.StorageInfo;
import se.sics.hop.metadata.persistence.DALWrapper;
import se.sics.hop.metadata.persistence.dal.StorageInfoDataAccess;
import se.sics.hop.metadata.persistence.entity.hdfs.HopStorageInfo;
import se.sics.hop.metadata.persistence.exceptions.StorageException;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class StorageInfoDALWrapper extends DALWrapper<StorageInfo, HopStorageInfo> implements StorageInfoDataAccess<StorageInfo> {

  private final StorageInfoDataAccess<HopStorageInfo> dataAccess;

  public StorageInfoDALWrapper(StorageInfoDataAccess<HopStorageInfo> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public StorageInfo findByPk(int infoType) throws StorageException {
    return convertDALtoHDFS(dataAccess.findByPk(infoType));
  }

  @Override
  public void prepare(StorageInfo storageInfo) throws StorageException {
    dataAccess.prepare(convertHDFStoDAL(storageInfo));
  }

  @Override
  public HopStorageInfo convertHDFStoDAL(StorageInfo hdfsClass) throws StorageException {
    return new HopStorageInfo(
            hdfsClass.DEFAULT_ROW_ID,
            hdfsClass.getLayoutVersion(),
            hdfsClass.getNamespaceID(),
            hdfsClass.getClusterID(),
            hdfsClass.getCTime(),
            hdfsClass.getBlockPoolId());
  }

  @Override
  public StorageInfo convertDALtoHDFS(HopStorageInfo dalClass) throws StorageException {
    return new StorageInfo(dalClass.getLayoutVersion(), dalClass.getNamespaceId(), dalClass.getClusterId(), dalClass.getCreationTime(), dalClass.getBlockPoolId());
  }
}
