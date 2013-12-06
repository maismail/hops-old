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
import java.net.UnknownHostException;
import java.util.UUID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import se.sics.hop.metadata.persistence.lock.TransactionLocks;
import se.sics.hop.transcation.LightWeightRequestHandler;
import se.sics.hop.metadata.persistence.exceptions.PersistanceException;
import se.sics.hop.transcation.RequestHandler.OperationType;
import se.sics.hop.transcation.TransactionalRequestHandler;
import se.sics.hop.metadata.persistence.dal.StorageInfoDataAccess;
import se.sics.hop.metadata.persistence.StorageFactory;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.Time;
import se.sics.hop.metadata.persistence.dalwrapper.StorageInfoDALWrapper;

/**
 * Common class for storage information.
 * 
 * TODO namespaceID should be long and computed as hash(address + port)
 */
@InterfaceAudience.Private
public class StorageInfo {
  //START_HOP_CODE
  public static final Log LOG = LogFactory.getLog(StorageInfo.class);
  public static final int DEFAULT_ROW_ID = 0; // StorageInfo is stored as one row in the database.
  protected String blockpoolID = ""; // id of the block pool. moved it from NNStorage.java to here. This is where it should have been
  //END_HOP_CODE
  public int   layoutVersion;   // layout version of the storage data
  public int   namespaceID;     // id of the file system
  public String clusterID;      // id of the cluster
  public long  cTime;           // creation time of the file system state
 
  public StorageInfo () {
    this(0, 0, "", 0L, "");
  }

  public StorageInfo(int layoutV, int nsID, String cid, long cT, String bpid) {
    layoutVersion = layoutV;
    clusterID = cid;
    namespaceID = nsID;
    cTime = cT;
    //START_HOP_CODE
    blockpoolID=bpid;
    //END_HOP_CODE
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
        StorageInfoDataAccess<StorageInfo> da = (StorageInfoDataAccess)StorageFactory.getDataAccess(StorageInfoDataAccess.class);
        return da.findByPk(StorageInfo.DEFAULT_ROW_ID);
      }
    };
    return (StorageInfo) getStorageInfoHandler.handle(null);
  }

  public static void storeStorageInfoToDB(final String clusterId) throws IOException { // should only be called by the format function once during the life time of the cluster. 
                                                                                       // FIXME [S] it can cause problems in the future when we try to run multiple NN
                                                                                       // Solution. call format on only one namenode or every one puts the same values.  
    // HOP FIXME use context
    TransactionalRequestHandler formatHandler = new TransactionalRequestHandler(OperationType.ADD_STORAGE_INFO) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        Configuration conf = new Configuration();
        String bpid = newBlockPoolID();
        StorageInfoDataAccess<StorageInfo> da = (StorageInfoDataAccess)StorageFactory.getDataAccess(StorageInfoDataAccess.class);
        da.prepare(new StorageInfo(HdfsConstants.LAYOUT_VERSION,
                conf.getInt(DFSConfigKeys.DFS_NAME_SPACE_ID_KEY, DFSConfigKeys.DFS_NAME_SPACE_ID_DEFAULT),
                clusterId, 0L, bpid));
        LOG.info("Added new entry to storage info. nsid:"+DFSConfigKeys.DFS_NAME_SPACE_ID_KEY+" CID:"+clusterId+" pbid:"+bpid);
        return null;
      }

      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        return null;
      }
    };
    formatHandler.handle(null);
  }
  
  public String getBlockPoolId()
  {
    return blockpoolID;
  }
  
  static String newBlockPoolID() throws UnknownHostException {
    String ip = "unknownIP";
    try {
      ip = DNS.getDefaultIP("default");
    } catch (UnknownHostException e) {
      System.out.println("Could not find ip address of \"default\" inteface.");
      throw e;
    }

    int rand = DFSUtil.getSecureRandom().nextInt(Integer.MAX_VALUE);
    String bpid = "BP-" + rand + "-" + ip + "-" + Time.now();
    return bpid;
  }
  
  /**
   * Generate new clusterID.
   * 
   * clusterID is a persistent attribute of the cluster.
   * It is generated when the cluster is created and remains the same
   * during the life cycle of the cluster.  When a new name node is formated, if 
   * this is a new cluster, a new clusterID is geneated and stored.  Subsequent 
   * name node must be given the same ClusterID during its format to be in the 
   * same cluster.
   * When a datanode register it receive the clusterID and stick with it.
   * If at any point, name node or data node tries to join another cluster, it 
   * will be rejected.
   * 
   * @return new clusterID
   */ 
  public static String newClusterID() {
    return "CID-" + UUID.randomUUID().toString();
  }
  
  public int getDefaultRowId()
  {
    return this.DEFAULT_ROW_ID;
  }
  //END_HOP_CODE
}
