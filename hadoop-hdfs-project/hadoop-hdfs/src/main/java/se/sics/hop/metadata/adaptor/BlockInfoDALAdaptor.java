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

import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import se.sics.hop.metadata.DALAdaptor;
import se.sics.hop.metadata.hdfs.dal.BlockInfoDataAccess;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopBlockInfo;
import se.sics.hop.exception.StorageException;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class BlockInfoDALAdaptor extends DALAdaptor<BlockInfo, HopBlockInfo> implements BlockInfoDataAccess<BlockInfo>{

  private final BlockInfoDataAccess<HopBlockInfo> dataAccess;

  public BlockInfoDALAdaptor(BlockInfoDataAccess<HopBlockInfo> dataAccess) {
    this.dataAccess = dataAccess;
  }

  @Override
  public int countAll() throws StorageException {
    return dataAccess.countAll();
  }

   @Override
  public int countAllCompleteBlocks() throws StorageException {
    return dataAccess.countAllCompleteBlocks();
  }
  
  @Override
  public BlockInfo findById(long blockId, int inodeId) throws StorageException {
    return convertDALtoHDFS(dataAccess.findById(blockId, inodeId));
  }

  @Override
  public List<BlockInfo> findByInodeId(int id) throws StorageException {
    return (List<BlockInfo>) convertDALtoHDFS(dataAccess.findByInodeId(id));
  }

  
  @Override
  public List<BlockInfo> findByInodeIds(int[] inodeIds) throws StorageException {
    return (List<BlockInfo>) convertDALtoHDFS(dataAccess.findByInodeIds(inodeIds));
  }
  
  @Override
  public List<BlockInfo> findAllBlocks() throws StorageException {
    return (List<BlockInfo>) convertDALtoHDFS(dataAccess.findAllBlocks());
  }

  @Override
  public List<BlockInfo> findByStorageId(int storageId) throws StorageException {
    return (List<BlockInfo>) convertDALtoHDFS(dataAccess.findByStorageId(storageId));
  }

  @Override
  public List<BlockInfo> findByIds(long[] blockIds, int[] inodeIds) throws StorageException {
    return (List<BlockInfo>) convertDALtoHDFS(dataAccess.findByIds(blockIds, inodeIds));
  }

  @Override
  public List<Long> findByStorageIdOnlyIds(int storageId) throws StorageException {
    return dataAccess.findByStorageIdOnlyIds(storageId);
  }
  
  @Override
  public void prepare(Collection<BlockInfo> removed, Collection<BlockInfo> newed, Collection<BlockInfo> modified) throws StorageException {
    dataAccess.prepare(convertHDFStoDAL(removed), convertHDFStoDAL(newed), convertHDFStoDAL(modified));
  }

  @Override
  public HopBlockInfo convertHDFStoDAL(BlockInfo hdfsClass) throws StorageException {
    if(hdfsClass != null){
    HopBlockInfo hopBlkInfo = new HopBlockInfo(hdfsClass.getBlockId(), hdfsClass.getBlockIndex(), hdfsClass.getInodeId(), hdfsClass.getNumBytes(),
            hdfsClass.getGenerationStamp(), hdfsClass.getBlockUCState().ordinal(), hdfsClass.getTimestamp());
    if (hdfsClass instanceof BlockInfoUnderConstruction) {
      BlockInfoUnderConstruction ucBlock = (BlockInfoUnderConstruction) hdfsClass;
      hopBlkInfo.setPrimaryNodeIndex(ucBlock.getPrimaryNodeIndex());
      hopBlkInfo.setBlockRecoveryId(ucBlock.getBlockRecoveryId());
    }
    return hopBlkInfo;}
    else{
      return null;
    }
  }

  @Override
  public BlockInfo convertDALtoHDFS(HopBlockInfo dalClass) throws StorageException {
    if(dalClass != null){
    Block b = new Block(dalClass.getBlockId(), dalClass.getNumBytes(), dalClass.getGenerationStamp());
    BlockInfo blockInfo = null;

    if (dalClass.getBlockUCState() > 0) { //UNDER_CONSTRUCTION, UNDER_RECOVERY, COMMITED
      blockInfo = new BlockInfoUnderConstruction(b, dalClass.getInodeId());
      ((BlockInfoUnderConstruction) blockInfo).setBlockUCStateNoPersistance(HdfsServerConstants.BlockUCState.values()[dalClass.getBlockUCState()]);
      ((BlockInfoUnderConstruction) blockInfo).setPrimaryNodeIndexNoPersistance(dalClass.getPrimaryNodeIndex());
      ((BlockInfoUnderConstruction) blockInfo).setBlockRecoveryIdNoPersistance(dalClass.getBlockRecoveryId());
    } else if (dalClass.getBlockUCState() == HdfsServerConstants.BlockUCState.COMPLETE.ordinal()) {
      blockInfo = new BlockInfo(b, dalClass.getInodeId());
    }

    blockInfo.setINodeIdNoPersistance(dalClass.getInodeId());
    blockInfo.setTimestampNoPersistance(dalClass.getTimeStamp());
    blockInfo.setBlockIndexNoPersistance(dalClass.getBlockIndex());

    return blockInfo;
    }else{
      return null;
    }
  }
}
