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
package org.apache.hadoop.hdfs.server.common;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.protocol.LayoutVersion.Feature;

import com.google.common.base.Joiner;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.NNStorage;
import org.apache.hadoop.hdfs.server.namenode.persistance.LightWeightRequestHandler;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.RequestHandler.OperationType;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.StorageInfoDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;

/**
 * Common class for storage information.
 * 
 * TODO namespaceID should be long and computed as hash(address + port)
 */
@InterfaceAudience.Private
public class StorageInfo {
  //START_HOP_CODE
  public static final int DEFAULT_ROW_ID = 0; // StorageInfo is stored as one row in the database.
  //END_HOP_CODE
  public int   layoutVersion;   // layout version of the storage data
  public int   namespaceID;     // id of the file system
  public String clusterID;      // id of the cluster
  public long  cTime;           // creation time of the file system state
 
  public StorageInfo () {
    this(0, 0, "", 0L);
  }

  public StorageInfo(int layoutV, int nsID, String cid, long cT) {
    layoutVersion = layoutV;
    clusterID = cid;
    namespaceID = nsID;
    cTime = cT;
  }
  
  public StorageInfo(StorageInfo from) {
    setStorageInfo(from);
  }

  /**
   * Layout version of the storage data.
   */
  public int    getLayoutVersion(){ return layoutVersion; }

  /**
   * Namespace id of the file system.<p>
   * Assigned to the file system at formatting and never changes after that.
   * Shared by all file system components.
   */
  public int    getNamespaceID()  { return namespaceID; }

  /**
   * cluster id of the file system.<p>
   */
  public String    getClusterID()  { return clusterID; }
  
  /**
   * Creation time of the file system state.<p>
   * Modified during upgrades.
   */
  public long   getCTime()        { return cTime; }
  
  public void   setStorageInfo(StorageInfo from) {
    layoutVersion = from.layoutVersion;
    clusterID = from.clusterID;
    namespaceID = from.namespaceID;
    cTime = from.cTime;
  }

  public boolean versionSupportsFederation() {
    return LayoutVersion.supports(Feature.FEDERATION, layoutVersion);
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("lv=").append(layoutVersion).append(";cid=").append(clusterID)
    .append(";nsid=").append(namespaceID).append(";c=").append(cTime);
    return sb.toString();
  }
  
  public String toColonSeparatedString() {
    return Joiner.on(":").join(
        layoutVersion, namespaceID, cTime, clusterID);
  }
  
  //START_HOP_CODE
  public static StorageInfo getStorageInfoFromDB() throws IOException {
    LightWeightRequestHandler getStorageInfoHandler = new LightWeightRequestHandler(OperationType.GET_STORAGE_INFO) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        StorageInfoDataAccess da = (StorageInfoDataAccess) StorageFactory.getDataAccess(StorageInfoDataAccess.class);
        return da.findByPk(StorageInfo.DEFAULT_ROW_ID);
      }
    };
    return (StorageInfo) getStorageInfoHandler.handle();
  }

  public static void storeStorageInfoToDB(final String clusterId) throws IOException { // should only be called by the format function once during the life time of the cluster. 
                                                                                       // FIXME [S] it can cause problems in the future when we try to run multiple NN
                                                                                       // Solution. call format on only one namenode or every one puts the same values.  
    LightWeightRequestHandler formatHandler = new LightWeightRequestHandler(OperationType.ADD_STORAGE_INFO) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        Configuration conf = new Configuration();
        StorageInfoDataAccess da = (StorageInfoDataAccess) StorageFactory.getDataAccess(StorageInfoDataAccess.class);
        da.prepare(new StorageInfo(HdfsConstants.LAYOUT_VERSION,
                conf.getInt(DFSConfigKeys.DFS_NAME_SPACE_ID, DFSConfigKeys.DFS_NAME_SPACE_ID_DEFAULT),
                clusterId, 0L));
        return null;
      }
    };
    formatHandler.handle();
  }
  //END_HOP_CODE
}
