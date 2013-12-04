package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.StorageInfoDataAccess;
import se.sics.hop.metadata.persistence.exceptions.StorageException;

/**
 *
 * @author hooman
 */
public class StorageInfoClusterj extends StorageInfoDataAccess {

  private StorageInfo createStorageInfo(StorageInfoDTO dto) {
    return new StorageInfo(
            dto.getLayoutVersion(),
            dto.getNamespaceId(),
            dto.getClusterId(),
            dto.getCreationTime(),
            dto.getBlockPoolId());
  }
  
  @PersistenceCapable(table = TABLE_NAME)
  public interface StorageInfoDTO {
    
    @PrimaryKey
    @Column(name = ID)
    int getId();
    
    void setId(int id);
    
    @Column(name = LAYOUT_VERSION)
    int getLayoutVersion();
    
    void setLayoutVersion(int layoutVersion);
    
    @Column(name = NAMESPACE_ID)
    int getNamespaceId();
    
    void setNamespaceId(int namespaceId);
    
    @Column(name = CLUSTER_ID)
    String getClusterId();
    
    void setClusterId(String clusterId);
    
    @Column(name = CREATION_TIME)
    long getCreationTime();
    
    void setCreationTime(long creationTime);
    
    @Column(name = BLOCK_POOL_ID)
    String getBlockPoolId();
    
    void setBlockPoolId(String bpid);
  }
  
  private ClusterjConnector connector = ClusterjConnector.INSTANCE;

  @Override
  public StorageInfo findByPk(int infoType) throws StorageException {
    try {
      Session session = connector.obtainSession();
      StorageInfoDTO si = session.find(StorageInfoDTO.class, infoType);
      if (si == null) {
        return null;
      }
      return createStorageInfo(si);
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(StorageInfo storageInfo) throws StorageException {
    try {
      Session session = connector.obtainSession();
      StorageInfoDTO dto = session.newInstance(StorageInfoDTO.class);
      dto.setId(StorageInfo.DEFAULT_ROW_ID);
      dto.setClusterId(storageInfo.getClusterID());
      dto.setLayoutVersion(storageInfo.layoutVersion);
      dto.setNamespaceId(storageInfo.getNamespaceID());
      dto.setCreationTime(storageInfo.getCTime());
      dto.setBlockPoolId(storageInfo.getBlockPoolId());
      session.savePersistent(dto);
    } catch (Exception ex) {
      throw new StorageException(ex);
    }
  }
}
