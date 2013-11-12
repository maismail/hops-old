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

import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;

/**
 * Directory INode class that has a quota restriction
 */
public class INodeDirectoryWithQuota extends INodeDirectory {
  private long nsQuota; /// NameSpace quota
  private long nsCount;
  private long dsQuota; /// disk space quota
  private long diskspace;
  
  /** Convert an existing directory inode to one with the given quota
   * 
   * @param nsQuota Namespace quota to be assigned to this inode
   * @param dsQuota Diskspace quota to be assigned to this indoe
   * @param other The other inode from which all other properties are copied
   */
  INodeDirectoryWithQuota(long nsQuota, long dsQuota,
      INodeDirectory other) throws PersistanceException {
    super(other);
    INode.DirCounts counts = new INode.DirCounts();
    other.spaceConsumedInTree(counts);
    this.nsCount = counts.getNsCount();
    this.diskspace = counts.getDsCount();
    setQuota(nsQuota, dsQuota);
  }
  
  /** constructor with no quota verification */
  INodeDirectoryWithQuota(PermissionStatus permissions, long modificationTime,
      long nsQuota, long dsQuota) {
    super(permissions, modificationTime);
    this.nsQuota = nsQuota;
    this.dsQuota = dsQuota;
    this.nsCount = 1;
  }
  
  /** constructor with no quota verification */
  public INodeDirectoryWithQuota(String name, PermissionStatus permissions,
      long nsQuota, long dsQuota) {
    super(name, permissions);
    this.nsQuota = nsQuota;
    this.dsQuota = dsQuota;
    this.nsCount = 1;
  }
  
  /** Get this directory's namespace quota
   * @return this directory's namespace quota
   */
  @Override
  public long getNsQuota() {
    return nsQuota;
  }
  
  /** Get this directory's diskspace quota
   * @return this directory's diskspace quota
   */
  @Override
  public long getDsQuota() {
    return dsQuota;
  }
  
  /** Set this directory's quota
   * 
   * @param nsQuota Namespace quota to be set
   * @param dsQuota diskspace quota to be set
   *                                
   */
  void setQuota(long newNsQuota, long newDsQuota) throws PersistanceException {
    nsQuota = newNsQuota;
    dsQuota = newDsQuota;
    save();
  }
  
  
  @Override
  DirCounts spaceConsumedInTree(DirCounts counts) {
    counts.nsCount += nsCount;
    counts.dsCount += diskspace;
    System.out.println("XXX dsCount is "+counts.dsCount);
    return counts;
  }

  /** Get the number of names in the subtree rooted at this directory
   * @return the size of the subtree rooted at this directory
   */
  public long numItemsInTree() {
    return nsCount;
  }
  
  public long diskspaceConsumed() {
    return diskspace;
  }
  
  /** Update the size of the tree
   * 
   * @param nsDelta the change of the tree size
   * @param dsDelta change to disk space occupied
   */
  void updateNumItemsInTree(long nsDelta, long dsDelta) throws PersistanceException {
    nsCount += nsDelta;
    diskspace += dsDelta;
    save();
    System.out.println("XXX diskspace is "+diskspace);
  }
  
  /** Update the size of the tree
   * 
   * @param nsDelta the change of the tree size
   * @param dsDelta change to disk space occupied
   **/
  void unprotectedUpdateNumItemsInTree(long nsDelta, long dsDelta) throws PersistanceException {
    nsCount = nsCount + nsDelta;
    diskspace = diskspace + dsDelta;
    save();
    System.out.println("XXX diskspace2 is "+diskspace);
  }
  
  /** 
   * Sets namespace and diskspace take by the directory rooted 
   * at this INode. This should be used carefully. It does not check 
   * for quota violations.
   * 
   * @param namespace size of the directory to be set
   * @param diskspace disk space take by all the nodes under this directory
   */
  public void setSpaceConsumed(long namespace, long diskspace) throws PersistanceException {
    setSpaceConsumedNoPersistance(namespace, diskspace);
    save();
  }
  
  public void setSpaceConsumedNoPersistance(long namespace, long diskspace) throws PersistanceException {
    this.nsCount = namespace;
    this.diskspace = diskspace;
  }
  /** Verify if the namespace count disk space satisfies the quota restriction 
   * @throws QuotaExceededException if the given quota is less than the count
   */
  void verifyQuota(long nsDelta, long dsDelta) throws QuotaExceededException {
    long newCount = nsCount + nsDelta;
    long newDiskspace = diskspace + dsDelta;
    if (nsDelta>0 || dsDelta>0) {
      if (nsQuota >= 0 && nsQuota < newCount) {
        throw new NSQuotaExceededException(nsQuota, newCount);
      }
      if (dsQuota >= 0 && dsQuota < newDiskspace) {
        throw new DSQuotaExceededException(dsQuota, newDiskspace);
      }
    }
  }
 
  //START_HOP_CODE
  public static INodeDirectoryWithQuota createRootDir(PermissionStatus permissions,
          long nsQuota, long dsQuota) {
    INodeDirectoryWithQuota newRootINode = new INodeDirectoryWithQuota(ROOT_NAME, permissions, nsQuota, dsQuota);
    newRootINode.setIdNoPersistance(ROOT_ID);
    newRootINode.setParentIdNoPersistance(ROOT_PARENT_ID);
    return newRootINode;
  }

  public static INodeDirectoryWithQuota getRootDir() throws PersistanceException {
    return (INodeDirectoryWithQuota) EntityManager.find(INode.Finder.ByNameAndParentId, ROOT_NAME, ROOT_PARENT_ID);
  }
  //END_HOP_CODE
}
