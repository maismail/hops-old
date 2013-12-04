
package org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity;

import org.apache.hadoop.hdfs.server.common.StorageInfo;
import se.sics.hop.metadata.persistence.exceptions.StorageException;

/**
 *
 * @author hooman
 */
public abstract class StorageInfoDataAccess extends EntityDataAccess{
  public static final String TABLE_NAME = "storage_info";
  public static final String ID = "id";
  public static final String LAYOUT_VERSION = "layout_version";
  public static final String NAMESPACE_ID = "namespace_id";
  public static final String CLUSTER_ID = "cluster_id";
  public static final String CREATION_TIME = "creation_time";
  public static final String BLOCK_POOL_ID = "block_pool_id";

  public abstract StorageInfo findByPk(int infoType) throws StorageException;

  public abstract void prepare(StorageInfo storageInfo) throws StorageException;
}
