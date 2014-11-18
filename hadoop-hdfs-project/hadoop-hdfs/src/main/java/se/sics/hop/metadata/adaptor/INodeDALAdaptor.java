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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryWithQuota;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.INodeFileUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.INodeSymlink;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import se.sics.hop.metadata.DALAdaptor;
import se.sics.hop.metadata.hdfs.dal.INodeDataAccess;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINode;
import se.sics.hop.exception.HopEnitityInitializationError;
import se.sics.hop.exception.StorageException;
import se.sics.hop.metadata.INodeIdentifier;
import se.sics.hop.metadata.hdfs.entity.hdfs.ProjectedINode;

/**
 *
 * @author salman
 */
public class INodeDALAdaptor extends DALAdaptor<INode, HopINode> implements INodeDataAccess<INode> {

  private INodeDataAccess<HopINode> dataAccess;

  public INodeDALAdaptor(INodeDataAccess<HopINode> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public INode indexScanfindInodeById(int inodeId) throws StorageException {
    return convertDALtoHDFS(dataAccess.indexScanfindInodeById(inodeId));
  }

  @Override
  public List<INode> indexScanFindInodesByParentId(int parentId) throws StorageException {
    List<INode> list = (List) convertDALtoHDFS(dataAccess.indexScanFindInodesByParentId(parentId));
    Collections.sort(list, INode.Order.ByName);
    return list;
  }

  @Override
  public List<ProjectedINode> findInodesForSubtreeOperationsWithReadLock(int parentId) throws StorageException {
    List<ProjectedINode> list = dataAccess.findInodesForSubtreeOperationsWithReadLock(parentId);
    Collections.sort(list);
    return list;
  }

  @Override
  public INode pkLookUpFindInodeByNameAndParentId(String name, int parentId) throws StorageException {
    return convertDALtoHDFS(dataAccess.pkLookUpFindInodeByNameAndParentId(name, parentId));
  }

  @Override
  public List<INode> getINodesPkBatched(String[] names, int[] parentIds) throws StorageException {
    return (List<INode>) convertDALtoHDFS(dataAccess.getINodesPkBatched(names, parentIds));
  }

  @Override
  public void prepare(Collection<INode> removed, Collection<INode> newed, Collection<INode> modified) throws StorageException {
    dataAccess.prepare(convertHDFStoDAL(removed), convertHDFStoDAL(newed), convertHDFStoDAL(modified));
  }

  @Override
  public int countAll() throws StorageException {
    return dataAccess.countAll();

  }

  @Override
  public List<INodeIdentifier> getAllINodeFiles(long startId, long endId) throws StorageException {
    return dataAccess.getAllINodeFiles(startId, endId);
  }
  
  @Override
  public boolean haveFilesWithIdsGreaterThan(long id) throws StorageException {
    return dataAccess.haveFilesWithIdsGreaterThan(id);
  }
  
  @Override
  public boolean haveFilesWithIdsBetween(long startId, long endId) throws StorageException {
    return dataAccess.haveFilesWithIdsBetween(startId, endId);
  }
  
  @Override
  public long getMinFileId() throws StorageException {
    return dataAccess.getMinFileId();
  }

  @Override
  public long getMaxFileId() throws StorageException {
    return dataAccess.getMaxFileId();
  }

  @Override
  public int countAllFiles() throws StorageException {
    return dataAccess.countAllFiles();
  }
   
  @Override
  public HopINode convertHDFStoDAL(INode inode) throws StorageException {
    HopINode hopINode = null;
    if (inode != null) {
      try {
        hopINode = new HopINode();
        hopINode.setModificationTime(inode.getModificationTime());
        hopINode.setAccessTime(inode.getAccessTime());
        hopINode.setName(inode.getLocalName());

        DataOutputBuffer permissionString = new DataOutputBuffer();
        inode.getPermissionStatus().write(permissionString);

        hopINode.setPermission(permissionString.getData());
        hopINode.setParentId(inode.getParentId());
        hopINode.setId(inode.getId());

        if (inode instanceof INodeDirectory) {
          hopINode.setUnderConstruction(false);
          hopINode.setDirWithQuota(false);
          hopINode.setDir(true);
        }
        if (inode instanceof INodeDirectoryWithQuota) {
          hopINode.setDir(true); //why was it false earlier?
          hopINode.setUnderConstruction(false);
          hopINode.setDirWithQuota(true);
        }
        if (inode instanceof INodeFile) {
          hopINode.setDir(false);
          hopINode.setUnderConstruction(inode.isUnderConstruction() ? true : false);
          hopINode.setDirWithQuota(false);
          hopINode.setHeader(((INodeFile) inode).getHeader());
          if (inode instanceof INodeFileUnderConstruction) {
            hopINode.setClientName(((INodeFileUnderConstruction) inode).getClientName());
            hopINode.setClientMachine(((INodeFileUnderConstruction) inode).getClientMachine());
            hopINode.setClientNode(((INodeFileUnderConstruction) inode).getClientNode() == null ? null : ((INodeFileUnderConstruction) inode).getClientNode().getXferAddr());
          }
          hopINode.setGenerationStamp(((INodeFile) inode).getGenerationStamp());
        }
        if (inode instanceof INodeSymlink) {
          hopINode.setDir(false);
          hopINode.setUnderConstruction(false);
          hopINode.setDirWithQuota(false);

          String linkValue = DFSUtil.bytes2String(((INodeSymlink) inode).getSymlink());
          hopINode.setSymlink(linkValue);
        }
        hopINode.setSubtreeLocked(inode.isSubtreeLocked());
        hopINode.setSubtreeLockOwner(inode.getSubtreeLockOwner());
      } catch (IOException e) {
        throw new HopEnitityInitializationError(e);
      }
    }
    return hopINode;
  }

  @Override
  public INode convertDALtoHDFS(HopINode hopINode) throws StorageException {
    INode inode = null;
    if (hopINode != null) {
      try {
        DataInputBuffer buffer = new DataInputBuffer();
        buffer.reset(hopINode.getPermission(), hopINode.getPermission().length);
        PermissionStatus ps = PermissionStatus.read(buffer);

        if (hopINode.isDir()) {
          if (hopINode.isDirWithQuota()) {
            inode = new INodeDirectoryWithQuota(hopINode.getName(), ps);
          } else {
            String iname = (hopINode.getName().length() == 0) ? INodeDirectory.ROOT_NAME : hopINode.getName();
            inode = new INodeDirectory(iname, ps);
          }

          inode.setAccessTimeNoPersistance(hopINode.getAccessTime());
          inode.setModificationTimeNoPersistance(hopINode.getModificationTime());
        } else if (hopINode.getSymlink() != null) {
          inode = new INodeSymlink(hopINode.getSymlink(), hopINode.getModificationTime(),
                  hopINode.getAccessTime(), ps);
        } else {
          if (hopINode.isUnderConstruction()) {
            DatanodeID dnID = (hopINode.getClientNode() == null
                    || hopINode.getClientNode().isEmpty()) ? null : new DatanodeID(hopINode.getClientNode());

            inode = new INodeFileUnderConstruction(ps,
                    INodeFile.getBlockReplication(hopINode.getHeader()),
                    INodeFile.getPreferredBlockSize(hopINode.getHeader()),
                    hopINode.getModificationTime(),
                    hopINode.getClientName(),
                    hopINode.getClientMachine(),
                    dnID);

            inode.setAccessTimeNoPersistance(hopINode.getAccessTime());
          } else {
            inode = new INodeFile(ps,
                    null,
                    INodeFile.getBlockReplication(hopINode.getHeader()),
                    hopINode.getModificationTime(),
                    hopINode.getAccessTime(),
                    INodeFile.getPreferredBlockSize(hopINode.getHeader()));
          }
          ((INodeFile) inode).setGenerationStampNoPersistence(hopINode.getGenerationStamp());
        }
        inode.setIdNoPersistance(hopINode.getId());
        inode.setLocalNameNoPersistance(hopINode.getName());
        inode.setParentIdNoPersistance(hopINode.getParentId());
        inode.setSubtreeLocked(hopINode.isSubtreeLocked());
        inode.setSubtreeLockOwner(hopINode.getSubtreeLockOwner());
      } catch (IOException e) {
        throw new HopEnitityInitializationError(e);
      }
    }
    return inode;
  }

}
