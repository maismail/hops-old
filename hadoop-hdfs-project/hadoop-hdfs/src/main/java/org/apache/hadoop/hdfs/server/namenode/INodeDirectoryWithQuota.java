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
import static org.apache.hadoop.hdfs.server.namenode.INodeDirectory.ROOT_NAME;
import se.sics.hop.Common;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.exception.PersistanceException;

/**
 * Directory INode class that has a quota restriction
 */
public class INodeDirectoryWithQuota extends INodeDirectory {
 
  /** Convert an existing directory inode to one with the given quota
   * 
   * @param nsQuota Namespace quota to be assigned to this inode
   * @param dsQuota Diskspace quota to be assigned to this indoe
   * @param other The other inode from which all other properties are copied
   */
  INodeDirectoryWithQuota(Long nsQuota, Long dsQuota,
      INodeDirectory other) throws PersistanceException {
    super(other);
    INode.DirCounts counts = new INode.DirCounts();
    other.spaceConsumedInTree(counts);
    createINodeAttributes(nsQuota, counts.getNsCount(), dsQuota, counts.getDsCount());
    setQuota(nsQuota, dsQuota);
  }
  
  /** constructor with no quota verification */
  public INodeDirectoryWithQuota(String name, PermissionStatus permissions) {
    super(name, permissions);
  }
  
  /** Get this directory's namespace quota
   * @return this directory's namespace quota
   */
  @Override
  public long getNsQuota() throws PersistanceException{
    return getINodeAttributes().getNsQuota();
  }
  
  /** Get this directory's diskspace quota
   * @return this directory's diskspace quota
   */
  @Override
  public long getDsQuota()throws PersistanceException {
    return getINodeAttributes().getDsQuota();
  }
  
  /** Set this directory's quota
   * 
   * @param nsQuota Namespace quota to be set
   * @param dsQuota diskspace quota to be set
   *                                
   */
  void setQuota(Long newNsQuota, Long newDsQuota) throws PersistanceException {
    getINodeAttributes().setNsQuota(newNsQuota);
    getINodeAttributes().setDsQuota(newDsQuota);
  }
  
  
  @Override
  DirCounts spaceConsumedInTree(DirCounts counts) throws PersistanceException{
    counts.nsCount += getINodeAttributes().getNsCount();
    counts.dsCount += getINodeAttributes().getDiskspace();
    return counts;
  }

  /** Get the number of names in the subtree rooted at this directory
   * @return the size of the subtree rooted at this directory
   */
  public Long numItemsInTree() throws PersistanceException{
    return getINodeAttributes().getNsCount();
  }
  
  public Long diskspaceConsumed() throws PersistanceException{
    return getINodeAttributes().getDiskspace();
  }
  
  /** Update the size of the tree
   * 
   * @param nsDelta the change of the tree size
   * @param dsDelta change to disk space occupied
   */
  void updateNumItemsInTree(Long nsDelta, Long dsDelta) throws PersistanceException {
    getINodeAttributes().setNsCount(getINodeAttributes().getNsCount() + nsDelta);
    getINodeAttributes().setDiskspace(getINodeAttributes().getDiskspace() + dsDelta);
  }
  
  /** Update the size of the tree
   * 
   * @param nsDelta the change of the tree size
   * @param dsDelta change to disk space occupied
   **/
  void unprotectedUpdateNumItemsInTree(Long nsDelta, Long dsDelta) throws PersistanceException {
    getINodeAttributes().setNsCount(getINodeAttributes().getNsCount() + nsDelta);
    getINodeAttributes().setDiskspace(getINodeAttributes().getDiskspace() + dsDelta);
  }
  
  /** 
   * Sets namespace and diskspace take by the directory rooted 
   * at this INode. This should be used carefully. It does not check 
   * for quota violations.
   * 
   * @param namespace size of the directory to be set
   * @param diskspace disk space take by all the nodes under this directory
   */
  public void setSpaceConsumed(Long namespace, Long diskspace) throws PersistanceException {
    getINodeAttributes().setNsCount(namespace);
    getINodeAttributes().setDiskspace(diskspace);
  }
  
  public void setSpaceConsumedNoPersistance(Long namespace, Long diskspace) throws PersistanceException{
    getINodeAttributes().setNsCountNoPersistance(namespace);
    getINodeAttributes().setDiskspaceNoPersistance(diskspace);
  }
  /** Verify if the namespace count disk space satisfies the quota restriction 
   * @throws QuotaExceededException if the given quota is less than the count
   */
  void verifyQuota(Long nsDelta, Long dsDelta) throws QuotaExceededException , PersistanceException{
    Long newCount = getINodeAttributes().getNsCount() + nsDelta;
    Long newDiskspace = getINodeAttributes().getDiskspace() + dsDelta;

    if (nsDelta>0 || dsDelta>0) {
      if (getINodeAttributes().getNsQuota() >= 0 && getINodeAttributes().getNsQuota() < newCount) {
        throw new NSQuotaExceededException(getINodeAttributes().getNsQuota(), newCount);
      }
      if (getINodeAttributes().getDsQuota() >= 0 && getINodeAttributes().getDsQuota() < newDiskspace) {
        throw new DSQuotaExceededException(getINodeAttributes().getDsQuota(), newDiskspace);
      }
    }
  }
 
  //START_HOP_CODE
  public static INodeDirectoryWithQuota createRootDir(PermissionStatus permissions) throws PersistanceException {
    INodeDirectoryWithQuota newRootINode = new INodeDirectoryWithQuota(ROOT_NAME, permissions);
    newRootINode.setIdNoPersistance(ROOT_ID);
    newRootINode.setParentIdNoPersistance(ROOT_PARENT_ID);
    return newRootINode;
  }

  public static INodeDirectoryWithQuota getRootDir() throws PersistanceException {
    return (INodeDirectoryWithQuota) EntityManager.find(INode.Finder.ByINodeID, ROOT_ID, INode.getPartitionKey(ROOT_NAME));
  }
  
  public INodeAttributes getINodeAttributes() throws PersistanceException{
    return EntityManager.find(INodeAttributes.Finder.ByPKey, (Integer)id);
  }
  
  private void createINodeAttributes(Long nsQuota, Long nsCount, Long dsQuota, Long diskspace) throws PersistanceException{
    INodeAttributes attr = new INodeAttributes(id, nsQuota, nsCount, dsQuota, diskspace);
    EntityManager.add(attr);
  }
  
  protected void persistAttributes() throws PersistanceException{
    getINodeAttributes().saveAttributes();
  }
  
  protected void removeAttributes() throws PersistanceException{
    getINodeAttributes().removeAttributes();
  }
  
  protected void changeAttributesPkNoPersistance(Integer id)throws PersistanceException{
    getINodeAttributes().setInodeIdNoPersistance(id);
  }
  //END_HOP_CODE
}
