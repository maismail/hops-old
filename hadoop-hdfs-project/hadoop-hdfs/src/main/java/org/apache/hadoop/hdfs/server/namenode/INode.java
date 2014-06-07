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
package org.apache.hadoop.hdfs.server.namenode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.util.StringUtils;

import com.google.common.primitives.SignedBytes;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;
import se.sics.hop.Common;
import se.sics.hop.erasure_coding.EncodingStatus;
import se.sics.hop.erasure_coding.ErasureCodingManager;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.exception.PersistanceException;

import static org.apache.hadoop.hdfs.server.namenode.FSNamesystem.LOG;

/**
 * We keep an in-memory representation of the file/block hierarchy.
 * This is a base INode class containing common fields for file and 
 * directory inodes.
 */
@InterfaceAudience.Private
//HOP: change class to public
public abstract class INode implements Comparable<byte[]> {
  
  static final List<INode> EMPTY_LIST = Collections.unmodifiableList(new ArrayList<INode>());

  //START_HOP_CODE
  public static enum Finder implements FinderType<INode> {

    ByINodeID, ParentId, ByPK_NameAndParentId;

    @Override
    public Class getType() {
      return INode.class;
    }
  }

  public static enum Order implements Comparator<INode> {

    ByName() {
      @Override
      public int compare(INode o1, INode o2) {
        // TODO - JIM why not compare by ID - more efficient?
        return o1.compareTo(o2.getLocalNameBytes());
      }
    };

    @Override
    public abstract int compare(INode o1, INode o2);

    public Comparator acsending() {
      return this;
    }

    public Comparator descending() {
      return Collections.reverseOrder(this);
    }
  }
  //END_HOP_CODE
    
  /**
   *  The inode name is in java UTF8 encoding; 
   *  The name in HdfsFileStatus should keep the same encoding as this.
   *  if this encoding is changed, implicitly getFileInfo and listStatus in
   *  clientProtocol are changed; The decoding at the client
   *  side should change accordingly.
   */
  protected byte[] name;
  protected INodeDirectory parent;
  protected long modificationTime;
  protected long accessTime;
  
    //START_HOP_CODE
  public static final int NON_EXISTING_ID = 0;
  protected int id = NON_EXISTING_ID;
  protected int parentId = NON_EXISTING_ID;
  //END_HOP_CODE

  /** Simple wrapper for two counters : 
   *  nsCount (namespace consumed) and dsCount (diskspace consumed).
   */
  static class DirCounts {
    long nsCount = 0;
    long dsCount = 0;
    
    /** returns namespace count */
    long getNsCount() {
      return nsCount;
    }
    /** returns diskspace count */
    long getDsCount() {
      return dsCount;
    }
  }
  
  //Only updated by updatePermissionStatus(...).
  //Other codes should not modify it.
  private long permission;

  private static enum PermissionStatusFormat {
    MODE(0, 16),
    GROUP(MODE.OFFSET + MODE.LENGTH, 25),
    USER(GROUP.OFFSET + GROUP.LENGTH, 23);

    final int OFFSET;
    final int LENGTH; //bit length
    final long MASK;

    PermissionStatusFormat(int offset, int length) {
      OFFSET = offset;
      LENGTH = length;
      MASK = ((-1L) >>> (64 - LENGTH)) << OFFSET;
    }

    long retrieve(long record) {
      return (record & MASK) >>> OFFSET;
    }

    long combine(long bits, long record) {
      return (record & ~MASK) | (bits << OFFSET);
    }
  }

  INode(PermissionStatus permissions, long mTime, long atime) {
    this.setLocalNameNoPersistance((byte[])null);
    this.parent = null;
    this.modificationTime = mTime;
    setAccessTimeNoPersistance(atime);
    setPermissionStatusNoPersistance(permissions);
  }

  protected INode(String name, PermissionStatus permissions) {
    this(permissions, 0L, 0L);
    setLocalNameNoPersistance(name);
  }
  
  /** copy constructor
   * 
   * @param other Other node to be copied
   */
  INode(INode other) throws PersistanceException {
    setLocalNameNoPersistance(other.getLocalName());
    this.parent = other.getParent();
    setPermissionStatusNoPersistance(other.getPermissionStatus());
    setModificationTimeNoPersistance(other.getModificationTime());
    setAccessTimeNoPersistance(other.getAccessTime());
    //START_HOP_CODE
    this.parentId = other.getParentId();
    this.id = other.getId();
    //STOP_HOP_CODE
  }

  /**
   * Check whether this is the root inode.
   */
  boolean isRoot() {
    return name.length == 0;
  }

  /** Set the {@link PermissionStatus} */
  private void setPermissionStatusNoPersistance(PermissionStatus ps) {
    setUserNoPersistance(ps.getUserName());
    setGroupNoPersistance(ps.getGroupName());
    setPermissionNoPersistance(ps.getPermission());
  }
  
  //HOP: protected => public
  /** Get the {@link PermissionStatus} */
  public PermissionStatus getPermissionStatus() {
    return new PermissionStatus(getUserName(),getGroupName(),getFsPermission());
  }
  private void updatePermissionStatus(PermissionStatusFormat f, long n) {
    permission = f.combine(n, permission);
  }
  /** Get user name */
  public String getUserName() {
    int n = (int)PermissionStatusFormat.USER.retrieve(permission);
    return SerialNumberManager.INSTANCE.getUser(n);
  }
  /** Set user */
  private void setUserNoPersistance(String user) {
    int n = SerialNumberManager.INSTANCE.getUserSerialNumber(user);
    updatePermissionStatus(PermissionStatusFormat.USER, n);
  }
  /** Get group name */
  public String getGroupName() {
    int n = (int)PermissionStatusFormat.GROUP.retrieve(permission);
    return SerialNumberManager.INSTANCE.getGroup(n);
  }
  /** Set group */
  private void setGroupNoPersistance(String group) {
    int n = SerialNumberManager.INSTANCE.getGroupSerialNumber(group);
    updatePermissionStatus(PermissionStatusFormat.GROUP, n);
  }
  /** Get the {@link FsPermission} */
  public FsPermission getFsPermission() {
    return new FsPermission(
        (short)PermissionStatusFormat.MODE.retrieve(permission));
  }
  protected short getFsPermissionShort() {
    return (short)PermissionStatusFormat.MODE.retrieve(permission);
  }
  /** Set the {@link FsPermission} of this {@link INode} */
  private void setPermissionNoPersistance(FsPermission permission) {
    updatePermissionStatus(PermissionStatusFormat.MODE, permission.toShort());
  }

  /**
   * Check whether it's a directory
   */
  public boolean isDirectory() {
    return false;
  }

  /**
   * Collect all the blocks in all children of this INode.
   * Count and return the number of files in the sub tree.
   * Also clears references since this INode is deleted.
   */
  abstract int collectSubtreeBlocksAndClear(List<Block> v) throws PersistanceException;

  /** Compute {@link ContentSummary}. */
  public final ContentSummary computeContentSummary() throws PersistanceException{
    long[] a = computeContentSummary(new long[]{0,0,0,0});
    return new ContentSummary(a[0], a[1], a[2], getNsQuota(), 
                              a[3], getDsQuota());
  }
  /**
   * @return an array of three longs. 
   * 0: length, 1: file count, 2: directory count 3: disk space
   */
  abstract long[] computeContentSummary(long[] summary) throws PersistanceException;
  
  /**
   * Get the quota set for this inode
   * @return the quota if it is set; -1 otherwise
   */
  public long getNsQuota() throws PersistanceException{
    return -1;
  }

  public long getDsQuota() throws PersistanceException{
    return -1;
  }
  
  boolean isQuotaSet() throws PersistanceException{
    return getNsQuota() >= 0 || getDsQuota() >= 0;
  }
  
  /**
   * Adds total number of names and total disk space taken under 
   * this tree to counts.
   * Returns updated counts object.
   */
  abstract DirCounts spaceConsumedInTree(DirCounts counts) throws PersistanceException;
  
  /**
   * Get local file name
   * @return local file name
   */
  public String getLocalName() {
    return DFSUtil.bytes2String(name);
  }


  String getLocalParentDir() throws PersistanceException {
    INode inode = isRoot() ? this : getParent();
    String parentDir = "";
    if (inode != null) {
      parentDir = inode.getFullPathName();
    }
    return (parentDir != null) ? parentDir : "";
  }

  /**
   * Get local file name
   * @return local file name
   */
  byte[] getLocalNameBytes() {
    return name;
  }

  
  //HOP: in KTHFS, these methods were set/getName
  /**
   * Set local file name
   */
  public void setLocalNameNoPersistance(String name) {
    this.name = DFSUtil.string2Bytes(name);
  }

  /**
   * Set local file name
   */
  public void setLocalNameNoPersistance(byte[] name) {
    this.name = name;
  }
  
  public String getFullPathName() throws PersistanceException{
    // Get the full path name of this inode.
    return FSDirectory.getFullPathName(this);
  }

  @Override
  public String toString() {
      try {
    return "\"" + getFullPathName() + "\":"
    + getUserName() + ":" + getGroupName() + ":"
    + (isDirectory()? "d": "-") + getFsPermission();
      } catch (PersistanceException ex) {
          Logger.getLogger(INode.class.getName()).log(Level.SEVERE, null, ex);
      }
      return null;
  }

  /**
   * Get parent directory 
   * @return parent INode
   */
  INodeDirectory getParent() throws PersistanceException{
    //START_HOP_CODE
    if(isRoot()){
      return null;
    }
    if(parent == null){
        parent = (INodeDirectory) EntityManager.find(INode.Finder.ByINodeID, getParentId());
    }
    //END_HOP_CODE
    return this.parent;
  }

  /** 
   * Get last modification time of inode.
   * @return access time
   */
  public long getModificationTime() {
    return this.modificationTime;
  }

  /**
   * Set last modification time of inode.
   */
  public void setModificationTimeNoPersistance(long modtime) {
    //assert isDirectory();
    if (this.modificationTime <= modtime) {
      this.modificationTime = modtime;
    }
  }

  /**
   * Always set the last modification time of inode.
   */
  protected void setModificationTimeForceNoPersistance(long modtime) {
    this.modificationTime = modtime;
  }

  /**
   * Get access time of inode.
   * @return access time
   */
  public long getAccessTime() {
    return accessTime;
  }

  /**
   * Set last access time of inode.
   */
  public void setAccessTimeNoPersistance(long atime) {
    accessTime = atime;
  }

  /**
   * Is this inode being constructed?
   */
  public boolean isUnderConstruction() {
    return false;
  }

  /**
   * Check whether it's a symlink
   */
  public boolean isSymlink() {
    return false;
  }

  /**
   * Breaks file path into components.
   * @param path
   * @return array of byte arrays each of which represents 
   * a single path component.
   */
  public static byte[][] getPathComponents(String path) {
    return getPathComponents(getPathNames(path));
  }

  /** Convert strings to byte arrays for path components. */
  public static byte[][] getPathComponents(String[] strings) {
    if (strings.length == 0) {
      return new byte[][]{null};
    }
    byte[][] bytes = new byte[strings.length][];
    for (int i = 0; i < strings.length; i++)
      bytes[i] = DFSUtil.string2Bytes(strings[i]);
    return bytes;
  }

  /**
   * Splits an absolute path into an array of path components.
   * @param path
   * @throws AssertionError if the given path is invalid.
   * @return array of path components.
   */
  static String[] getPathNames(String path) {
    if (path == null || !path.startsWith(Path.SEPARATOR)) {
      throw new AssertionError("Absolute path required");
    }
    return StringUtils.split(path, Path.SEPARATOR_CHAR);
  }

  /**
   * Given some components, create a path name.
   * @param components The path components
   * @param start index
   * @param end index
   * @return concatenated path
   */
  static String constructPath(byte[][] components, int start, int end) {
    StringBuilder buf = new StringBuilder();
    for (int i = start; i < end; i++) {
      buf.append(DFSUtil.bytes2String(components[i]));
      if (i < end - 1) {
        buf.append(Path.SEPARATOR);
      }
    }
    return buf.toString();
  }


  boolean removeNode() throws PersistanceException {
    if (parent == null) {
      return false;
    } else {
      parent.removeChild(this);
      parent = null;
      return true;
    }
  }

  private static final byte[] EMPTY_BYTES = {};

  @Override
  public final int compareTo(byte[] bytes) {
    final byte[] left = name == null? EMPTY_BYTES: name;
    final byte[] right = bytes == null? EMPTY_BYTES: bytes;
    return SignedBytes.lexicographicalComparator().compare(left, right);
  }

  @Override
  public final boolean equals(Object that) {
    if (this == that) {
      return true;
    }
    if (that == null || !(that instanceof INode)) {
      return false;
    }
    if(Arrays.equals(this.name, ((INode)that).name) 
            && this.id == ((INode)that).id
            && this.parentId == ((INode)that).parentId){
        return true;
    }
    return false;
  }

  @Override
  public final int hashCode() {
    return Arrays.hashCode(this.name);
  }
  
  /**
   * Create an INode; the inode's name is not set yet
   * 
   * @param permissions permissions
   * @param blocks blocks if a file
   * @param symlink symblic link if a symbolic link
   * @param replication replication factor
   * @param modificationTime modification time
   * @param atime access time
   * @param nsQuota namespace quota
   * @param dsQuota disk quota
   * @param preferredBlockSize block size
   * @return an inode
   */
  /*static INode newINode(PermissionStatus permissions,
                        BlockInfo[] blocks,
                        String symlink,
                        short replication,
                        long modificationTime,
                        long atime,
                        long nsQuota,
                        long dsQuota,
                        long preferredBlockSize) {
    if (symlink.length() != 0) { // check if symbolic link
      return new INodeSymlink(symlink, modificationTime, atime, permissions);
    }  else if (blocks == null) { //not sym link and blocks null? directory!
      if (nsQuota >= 0 || dsQuota >= 0) {
        return new INodeDirectoryWithQuota(
            permissions, modificationTime, nsQuota, dsQuota);
      } 
      // regular directory
      return new INodeDirectory(permissions, modificationTime);
    }
    // file
    return new INodeFile(permissions, blocks, replication,
        modificationTime, atime, preferredBlockSize);
  }*/
  
  //START_HOP_CODE
  public final void setIdNoPersistance(int id){
    this.id = id;
  }
  public int getId() {
    return this.id;
  }
  
  public void setParent(INodeDirectory p) throws PersistanceException {
    setParentNoPersistance(p);
    save();
  }

  public void setParentNoPersistance(INodeDirectory p) {
    this.parent = p;
    this.parentId = p.getId();
  }
  
  public void setParentIdNoPersistance(int pid) {
    this.parentId = pid;
  }

  public int getParentId() {
    return this.parentId;
  }

  public String nameParentKey() {
    return parentId + getLocalName();
  }
  
   /** Set user */
  protected void setUser(String user) throws PersistanceException {
    setUserNoPersistance(user);
    save();
  }

  protected void setGroup(String group) throws PersistanceException {
    setGroupNoPersistance(group);
    save();
  }

  void setPermission(FsPermission permission) throws PersistanceException {
    setPermissionNoPersistance(permission);
    save();
  }

  protected void setPermissionStatus(PermissionStatus ps) throws PersistanceException {
    setUser(ps.getUserName());
    setGroup(ps.getGroupName());
    setPermission(ps.getPermission());
  }

  public void setLocalName(String name) throws PersistanceException {
    setLocalNameNoPersistance(name);
    save();
  }
  
  public void setLocalName(byte[] name) throws PersistanceException {
    setLocalNameNoPersistance(name);
    save();
  }
  
  public void setModificationTime(long modtime) throws PersistanceException {
    setModificationTimeNoPersistance(modtime);
    save();
  }

  public void setAccessTime(long atime) throws PersistanceException {
    setAccessTimeNoPersistance(atime);
    save();
  }

  void setModificationTimeForce(long modtime) throws PersistanceException {
    setModificationTimeForceNoPersistance(modtime);
    save();
  }
  
  public boolean exists(){
    if(id == NON_EXISTING_ID)
    {
      return false;
    }
    
    return true;
  }

  protected void save() throws PersistanceException {
    save(this);
  }

  protected void save(INode node) throws PersistanceException {
    EntityManager.update(node);
  }

  protected void remove() throws PersistanceException {
    remove(this);
  }

  protected void remove(INode node) throws PersistanceException {
    EntityManager.remove(node);
    //if This inode is of type INodeDirectoryWithQuota then also delete the INode Attribute table
    if(node instanceof INodeDirectoryWithQuota){
      ((INodeDirectoryWithQuota)node).removeAttributes();
    }
    cleanParity(node);
  }

  private void cleanParity(INode node) throws PersistanceException {
    if (ErasureCodingManager.isEnabled()) {
      EncodingStatus status = EntityManager.find(EncodingStatus.Finder.ByInodeId, node.getId());
      if (status != null) {
        status.setStatus(EncodingStatus.Status.DELETED);
        EntityManager.update(status);
        return;
      }
    }
  }
  //END_HOP_CODE:
}
