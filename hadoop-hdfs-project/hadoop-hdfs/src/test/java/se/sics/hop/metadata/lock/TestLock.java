/*
 * Copyright 2014 Apache Software Foundation.
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
package se.sics.hop.metadata.lock;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.junit.Before;
import org.junit.Test;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.StorageInitializtionException;
import se.sics.hop.metadata.StorageFactory;
import se.sics.hop.metadata.context.PendingBlockContext;
import se.sics.hop.metadata.hdfs.dal.BlockInfoDataAccess;
import se.sics.hop.metadata.hdfs.dal.CorruptReplicaDataAccess;
import se.sics.hop.metadata.hdfs.dal.ExcessReplicaDataAccess;
import se.sics.hop.metadata.hdfs.dal.INodeDataAccess;
import se.sics.hop.metadata.hdfs.dal.InvalidateBlockDataAccess;
import se.sics.hop.metadata.hdfs.dal.LeaseDataAccess;
import se.sics.hop.metadata.hdfs.dal.LeasePathDataAccess;
import se.sics.hop.metadata.hdfs.dal.PendingBlockDataAccess;
import se.sics.hop.metadata.hdfs.dal.ReplicaDataAccess;
import se.sics.hop.metadata.hdfs.dal.ReplicaUnderConstructionDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.HopCorruptReplica;
import se.sics.hop.metadata.hdfs.entity.hop.HopExcessReplica;
import se.sics.hop.metadata.hdfs.entity.hop.HopIndexedReplica;
import se.sics.hop.metadata.hdfs.entity.hop.HopInvalidatedBlock;
import se.sics.hop.metadata.hdfs.entity.hop.HopLeasePath;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.transaction.handler.HDFSTransactionalRequestHandler;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.transaction.lock.TransactionLockTypes.INodeLockType;
import se.sics.hop.transaction.lock.TransactionLockTypes.INodeResolveType;
import se.sics.hop.transaction.lock.TransactionLockTypes.LockType;
import se.sics.hop.transaction.lock.TransactionLocks;

/**
 *
 * @author salman
 */
public class TestLock {

  HdfsConfiguration conf = null;

  @Before
  public void init() throws StorageException, PersistanceException, StorageInitializtionException {
    conf = new HdfsConfiguration();
    StorageFactory.setConfiguration(conf);
    StorageFactory.getConnector().formatStorage();
    insertData();

  }

  @Test
  public void testLock() throws Exception {
    final boolean resolveLink = false;
    HDFSTransactionalRequestHandler handler = new HDFSTransactionalRequestHandler(HDFSOperationType.START_FILE) {
      protected LinkedList<INode> preTxResolvedInodes = new LinkedList<INode>(); // For the operations requires to have inodes before starting transactions.  
      protected boolean[] isPreTxPathFullyResolved = new boolean[1];

      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        ReceivedDeletedBlockInfo rdbi = (ReceivedDeletedBlockInfo) getParams()[0];
        HDFSTransactionLockAcquirer tla = new HDFSTransactionLockAcquirer();
        tla.getLocks().
                addINode(TransactionLockTypes.INodeLockType.WRITE).
                addBlock(rdbi.getBlock().getBlockId()).
                addReplica().
                addExcess().
                addCorrupt().
                addUnderReplicatedBlock().
                addGenerationStamp(LockType.READ);
        if (!rdbi.isDeletedBlock()) {
          tla.getLocks().
                  addPendingBlock().
                  addReplicaUc().
                  addInvalidatedBlock();
        }
        return tla.acquireByBlock(inodeId);
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        return null;
      }
      long inodeId;

      @Override
      public void setUp() throws StorageException {
        ReceivedDeletedBlockInfo rdbi = (ReceivedDeletedBlockInfo) getParams()[0];
        FSNamesystem.LOG.debug("reported block id=" + rdbi.getBlock().getBlockId());
        if (rdbi.getBlock() instanceof BlockInfo) {
          inodeId = ((BlockInfo) rdbi.getBlock()).getInodeId();
        } else {
          inodeId = INodeUtil.findINodeIdByBlock(rdbi.getBlock().getBlockId());
        }
        if (inodeId == INodeFile.NON_EXISTING_ID) {
          FSNamesystem.LOG.error("Invalid State. deleted blk is not recognized. bid=" + rdbi.getBlock().getBlockId());
          //throw new TransactionLockAcquireFailure("Invalid State. deleted blk is not recognized. bid=" + rdbi.getBlock().getBlockId());
          // dont throw the exception. the cached is changed in a way that
          // it will bring in null values a the block will not be processed by the 
          // process perfom task because of the null values
          // if we throw the exception then the tx fails and rollback.
        }
      }
    };
    ReceivedDeletedBlockInfo test = new ReceivedDeletedBlockInfo(new Block(1,1,1), BlockStatus.DELETED_BLOCK, "");
    handler.setParams(test);
    for(int i = 0; i < 3; i++){
      handler.handle();
    }
    

  }

  private void insertData() throws StorageException, PersistanceException {
    System.out.println("Building the data...");
    List<INode> newFiles = new LinkedList<INode>();
    INodeFile root;
    root = new INodeFile(new PermissionStatus("salman", "usr", new FsPermission((short) 0777)), null, (short) 3, 0l, 0l, 0l);
    root.setIdNoPersistance(0);
    root.setLocalNameNoPersistance("/");
    root.setParentIdNoPersistance(-1);

    INodeFile dir;
    dir = new INodeFile(new PermissionStatus("salman", "usr", new FsPermission((short) 0777)), null, (short) 3, 0l, 0l, 0l);
    dir.setIdNoPersistance(1);
    dir.setLocalNameNoPersistance("test_dir");
    dir.setParentIdNoPersistance(0);

    INodeFile file;
    file = new INodeFile(new PermissionStatus("salman", "usr", new FsPermission((short) 0777)), null, (short) 3, 0l, 0l, 0l);
    file.setIdNoPersistance(2);
    file.setLocalNameNoPersistance("file1");
    file.setParentIdNoPersistance(1);

    newFiles.add(root);
    newFiles.add(dir);
    newFiles.add(file);
    StorageFactory.getConnector().beginTransaction();
    INodeDataAccess da = (INodeDataAccess) StorageFactory.getDataAccess(INodeDataAccess.class);
    da.prepare(new LinkedList<INode>(), newFiles, new LinkedList<INode>());
    StorageFactory.getConnector().commit();

    //............... Blocks ...............................................
    BlockInfo block = new BlockInfo(new BlockInfoUnderConstruction(new Block(1, 1, 1)));
    List<BlockInfo> newBlocks = new LinkedList<BlockInfo>();
    newBlocks.add(block);
    StorageFactory.getConnector().beginTransaction();
    BlockInfoDataAccess bda = (BlockInfoDataAccess) StorageFactory.getDataAccess(BlockInfoDataAccess.class);
    bda.prepare(new LinkedList<BlockInfo>(), newBlocks, new LinkedList<BlockInfo>());
    StorageFactory.getConnector().commit();


    //............... Replicas ...............................................
    List<HopIndexedReplica> replicas = new LinkedList<HopIndexedReplica>();
    replicas.add(new HopIndexedReplica(1, 1, 1));
    replicas.add(new HopIndexedReplica(1, 2, 2));
    replicas.add(new HopIndexedReplica(1, 3, 3));
    StorageFactory.getConnector().beginTransaction();
    ReplicaDataAccess rda = (ReplicaDataAccess) StorageFactory.getDataAccess(ReplicaDataAccess.class);
    rda.prepare(new LinkedList<HopIndexedReplica>(), replicas, new LinkedList<HopIndexedReplica>());
    StorageFactory.getConnector().commit();


    //............... Pending Replicas ...............................................
    List<PendingBlockInfo> pendingList = new LinkedList<PendingBlockInfo>();
    pendingList.add(new PendingBlockInfo(1, 1, 1));
    StorageFactory.getConnector().beginTransaction();
    PendingBlockDataAccess pda = (PendingBlockDataAccess) StorageFactory.getDataAccess(PendingBlockDataAccess.class);
    pda.prepare(new LinkedList<PendingBlockInfo>(), pendingList, new LinkedList<PendingBlockInfo>());
    StorageFactory.getConnector().commit();


    //............... lease ...............................................
    List<Lease> leases = new LinkedList<Lease>();
    leases.add(new Lease("client1", 1, 1));
    StorageFactory.getConnector().beginTransaction();
    LeaseDataAccess lda = (LeaseDataAccess) StorageFactory.getDataAccess(LeaseDataAccess.class);
    lda.prepare(new LinkedList<Lease>(), leases, new LinkedList<Lease>());
    StorageFactory.getConnector().commit();


    //............... lease Paths...............................................
    List<HopLeasePath> lpaths = new LinkedList<HopLeasePath>();
    lpaths.add(new HopLeasePath("/test_dir/file1", 1));
    StorageFactory.getConnector().beginTransaction();
    LeasePathDataAccess lpda = (LeasePathDataAccess) StorageFactory.getDataAccess(LeasePathDataAccess.class);
    lpda.prepare(new LinkedList<HopLeasePath>(), lpaths, new LinkedList<HopLeasePath>());
    StorageFactory.getConnector().commit();


    //............... Replica Under Construction ...............................................
    List<ReplicaUnderConstruction> replicasUC = new LinkedList<ReplicaUnderConstruction>();
    replicasUC.add(new ReplicaUnderConstruction(ReplicaState.FINALIZED, 1, 1, 1));
    StorageFactory.getConnector().beginTransaction();
    ReplicaUnderConstructionDataAccess rucda = (ReplicaUnderConstructionDataAccess) StorageFactory.getDataAccess(ReplicaUnderConstructionDataAccess.class);
    rucda.prepare(new LinkedList<ReplicaUnderConstruction>(), replicasUC, new LinkedList<ReplicaUnderConstruction>());
    StorageFactory.getConnector().commit();



    //............... Excess Replica ...............................................
    List<HopExcessReplica> erlist = new LinkedList<HopExcessReplica>();
    erlist.add(new HopExcessReplica(1, 1));
    StorageFactory.getConnector().beginTransaction();
    ExcessReplicaDataAccess erda = (ExcessReplicaDataAccess) StorageFactory.getDataAccess(ExcessReplicaDataAccess.class);
    erda.prepare(new LinkedList<HopExcessReplica>(), erlist, new LinkedList<HopExcessReplica>());
    StorageFactory.getConnector().commit();



    //............... Corrupted Replica ...............................................
    List<HopCorruptReplica> crlist = new LinkedList<HopCorruptReplica>();
    crlist.add(new HopCorruptReplica(1, 1));
    StorageFactory.getConnector().beginTransaction();
    CorruptReplicaDataAccess crda = (CorruptReplicaDataAccess) StorageFactory.getDataAccess(CorruptReplicaDataAccess.class);
    crda.prepare(new LinkedList<HopCorruptReplica>(), crlist, new LinkedList<HopCorruptReplica>());
    StorageFactory.getConnector().commit();
    
    
    
     //............... Invalidated Blocks ...............................................
    List<HopInvalidatedBlock> iblist = new LinkedList<HopInvalidatedBlock>();
    iblist.add(new HopInvalidatedBlock(1,1,1,1));
    StorageFactory.getConnector().beginTransaction();
    InvalidateBlockDataAccess ibda = (InvalidateBlockDataAccess) StorageFactory.getDataAccess(InvalidateBlockDataAccess.class);
    ibda.prepare(new LinkedList<HopInvalidatedBlock>(), iblist, new LinkedList<HopInvalidatedBlock>());
    StorageFactory.getConnector().commit();
  }
}
