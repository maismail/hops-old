package org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity;

import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public abstract class InodeDataAccess extends EntityDataAccess {

  public static final String TABLE_NAME = "inodes";
  public static final String ID = "id";
  public static final String NAME = "name";
  public static final String PARENT_ID = "parent_id";
  public static final String IS_DIR = "is_dir";
  public static final String MODIFICATION_TIME = "modification_time";
  public static final String ACCESS_TIME = "access_time";
  public static final String PERMISSION = "permission";
  public static final String NSQUOTA = "nsquota";
  public static final String DSQUOTA = "dsquota";
  public static final String IS_UNDER_CONSTRUCTION = "is_under_construction";
  public static final String CLIENT_NAME = "client_name";
  public static final String CLIENT_MACHINE = "client_machine";
  public static final String CLIENT_NODE = "client_node";
  public static final String IS_CLOSED_FILE = "is_closed_file";
  public static final String HEADER = "header";
  public static final String IS_DIR_WITH_QUOTA = "is_dir_with_quota";
  public static final String NSCOUNT = "nscount";
  public static final String DSCOUNT = "dscount";
  public static final String SYMLINK = "symlink";

  public abstract INode findInodeById(long inodeId) throws StorageException;

  public abstract List<INode> findInodesByParentIdSortedByName(long parentId) throws StorageException;

  public abstract INode findInodeByNameAndParentId(String name, long parentId) throws StorageException;

  public abstract List<INode> findInodesByIds(List<Long> ids) throws StorageException;

  public abstract void prepare(Collection<INode> removed, Collection<INode> newed, Collection<INode> modified) throws StorageException;
  
  /**
   * Counts all the inodes.
   * @return Numbre of inodes.
   * @throws StorageException 
   */
  public abstract int countAll() throws StorageException;
}
