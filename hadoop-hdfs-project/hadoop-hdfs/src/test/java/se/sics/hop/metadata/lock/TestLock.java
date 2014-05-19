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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.UnresolvedPathException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo.BlockStatus;
import org.junit.Before;
import org.junit.Test;
import se.sics.hop.Common;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.StorageInitializtionException;
import se.sics.hop.metadata.StorageFactory;
import se.sics.hop.metadata.Variables;
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
import se.sics.hop.metadata.hdfs.tabledef.INodeTableDef;
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
  INodeDirectory root, dir1, dir2, dir3, dir4, dir5, dir6;
  INodeFile file1, file2, file3;

  @Before
  public void init() throws StorageException, PersistanceException, StorageInitializtionException {
    conf = new HdfsConfiguration();
    StorageFactory.setConfiguration(conf);
    StorageFactory.getConnector().formatStorage();
    insertData();

  }

  @Test
  public void testPartitionKey() throws UnresolvedPathException, PersistanceException {
   
  }
  
  @Test
  public void testPartKeys() throws UnresolvedPathException, PersistanceException {
    LinkedList<INode> preTxResolvedInodes = new LinkedList<INode>();
    boolean[] isPreTxPathFullyResolved = new boolean[1];
    INodeUtil.resolvePathWithNoTransaction("/dir1/dir2/dir3/dir4/dir5/file1", true, preTxResolvedInodes, isPreTxPathFullyResolved);
    System.out.println("___resolved components are " + preTxResolvedInodes.size() + " isfullyResolved " + isPreTxPathFullyResolved[0]);
    for (INode inode : preTxResolvedInodes) {
      System.out.println(inode.getId());
    }


    System.out.println("____________________________________________________________");

    preTxResolvedInodes = new LinkedList<INode>();
    isPreTxPathFullyResolved = new boolean[1];
    INodeUtil.findPathINodesById(dir3.getId(), preTxResolvedInodes, isPreTxPathFullyResolved);
    for (INode inode : preTxResolvedInodes) {
      System.out.println(inode.getId());
    }
    System.out.println("___resolved components are " + preTxResolvedInodes.size() + " isfullyResolved " + isPreTxPathFullyResolved[0]);
  }

  @Test
  public void testLock() throws Exception {
    final String src = "/dir1/dir2/d3/d4/d5";
    final boolean resolveLink = false;
    HDFSTransactionalRequestHandler handler = new HDFSTransactionalRequestHandler(HDFSOperationType.START_FILE) {
      protected LinkedList<INode> preTxResolvedInodes = new LinkedList<INode>(); // For the operations requires to have inodes before starting transactions.  
      protected boolean[] isPreTxPathFullyResolved = new boolean[1];

      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        HDFSTransactionLockAcquirer tla = new HDFSTransactionLockAcquirer(preTxResolvedInodes, resolveLink);
        tla.getLocks().
                addINode(INodeResolveType.PATH_WITH_UNKNOWN_HEAD, TransactionLockTypes.INodeLockType.WRITE, new String[]{src}).
                addBlock().
                addReplica().
                addExcess().
                addCorrupt().
                addUnderReplicatedBlock().
                addGenerationStamp(LockType.READ).
                addPendingBlock().
                addReplicaUc().
                addInvalidatedBlock();
        return tla.acquire();
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        return null;
      }
      long inodeId;

      @Override
      public void setUp() throws StorageException, UnresolvedPathException, PersistanceException {
INodeUtil.resolvePathWithNoTransaction(src, resolveLink, preTxResolvedInodes, isPreTxPathFullyResolved);
      }
    };

    for (int i = 0; i < 1; i++) {
      handler.handle();
    }


  }

  private void insertData() throws StorageException, PersistanceException {
    System.out.println("Building the data...");
    int id = 100;
    List<INode> newFiles = new LinkedList<INode>();
    root = new INodeDirectory(new PermissionStatus("salman", "usr", new FsPermission((short) 0777)), 0l);
    root.setIdNoPersistance(INodeDirectory.ROOT_ID);
    root.setLocalNameNoPersistance(INodeDirectory.ROOT_NAME);
    root.setParentIdNoPersistance(-1);

    dir1 = new INodeDirectory(new PermissionStatus("salman", "usr", new FsPermission((short) 0777)), 0l);
    dir1.setIdNoPersistance(id++);
    dir1.setLocalNameNoPersistance("dir1");
    dir1.setParentIdNoPersistance(root.getId());

    dir2 = new INodeDirectory(new PermissionStatus("salman", "usr", new FsPermission((short) 0777)), 0l);
    dir2.setIdNoPersistance(id++);
    dir2.setLocalNameNoPersistance("dir2");
    dir2.setParentIdNoPersistance(dir1.getId());

    dir3 = new INodeDirectory(new PermissionStatus("salman", "usr", new FsPermission((short) 0777)), 0l);
    dir3.setIdNoPersistance(id++);
    dir3.setLocalNameNoPersistance("dir3");
    dir3.setParentIdNoPersistance(dir2.getId());

    dir4 = new INodeDirectory(new PermissionStatus("salman", "usr", new FsPermission((short) 0777)), 0l);
    dir4.setIdNoPersistance(id++);
    dir4.setLocalNameNoPersistance("dir4");
    dir4.setParentIdNoPersistance(dir3.getId());

    dir5 = new INodeDirectory(new PermissionStatus("salman", "usr", new FsPermission((short) 0777)), 0l);
    dir5.setIdNoPersistance(id++);
    dir5.setLocalNameNoPersistance("dir5");
    dir5.setParentIdNoPersistance(dir4.getId());
    
    dir6 = new INodeDirectory(new PermissionStatus("salman", "usr", new FsPermission((short) 0777)), 0l);
    dir6.setIdNoPersistance(id++);
    dir6.setLocalNameNoPersistance("dir6");
    dir6.setParentIdNoPersistance(dir5.getId());
    
    file1 = new INodeFile(new PermissionStatus("salman", "usr", new FsPermission((short) 0777)), null, (short) 3, 0l, 0l, 0l);
    file1.setIdNoPersistance(id++);
    file1.setLocalNameNoPersistance("file1");
    file1.setParentIdNoPersistance(dir5.getId());
    
    file2 = new INodeFile(new PermissionStatus("salman", "usr", new FsPermission((short) 0777)), null, (short) 3, 0l, 0l, 0l);
    file2.setIdNoPersistance(id++);
    file2.setLocalNameNoPersistance("file2");
    file2.setParentIdNoPersistance(dir6.getId());
    
    file3 = new INodeFile(new PermissionStatus("salman", "usr", new FsPermission((short) 0777)), null, (short) 3, 0l, 0l, 0l);
    file3.setIdNoPersistance(id++);
    file3.setLocalNameNoPersistance("file3");
    file3.setParentIdNoPersistance(dir4.getId());

    newFiles.add(root);
    newFiles.add(dir1);
    newFiles.add(dir2);
    newFiles.add(dir3);
    newFiles.add(dir4);
    newFiles.add(dir5);
    newFiles.add(dir6);
    newFiles.add(file1);
    newFiles.add(file2);
    newFiles.add(file3);

    
     //............... Blocks ...............................................
    BlockInfo block = new BlockInfo();
    block.setINodeIdNoPersistance(file1.getId());
    block.setPartKeyNoPersistance(INode.INVALID_PART_KEY);
    List<BlockInfo> newBlocks = new LinkedList<BlockInfo>();
    newBlocks.add(block);

    //............... Replicas ...............................................
    List<HopIndexedReplica> replicas = new LinkedList<HopIndexedReplica>();
    replicas.add(new HopIndexedReplica(1, 1, 1, 1, 1));
    replicas.add(new HopIndexedReplica(1, 1, 2, 1, 2));
    replicas.add(new HopIndexedReplica(1, 1, 3, 1, 3));
    
    //............... Pending Replicas ...............................................
    List<PendingBlockInfo> pendingList = new LinkedList<PendingBlockInfo>();
    pendingList.add(new PendingBlockInfo(1, 1,1,1, 1));

     //............... lease ...............................................
    List<Lease> leases = new LinkedList<Lease>();
    leases.add(new Lease("client1", 1, 1));
    
    //............... lease Paths...............................................
    List<HopLeasePath> lpaths = new LinkedList<HopLeasePath>();
    lpaths.add(new HopLeasePath("/dir/file1", 1));
    
    //............... Replica Under Construction ...............................................
    List<ReplicaUnderConstruction> replicasUC = new LinkedList<ReplicaUnderConstruction>();
    
    replicasUC.add(new ReplicaUnderConstruction(ReplicaState.FINALIZED, 1, 1, 1, 1, 1));
    
    
    //............... Excess Replica ...............................................
    List<HopExcessReplica> erlist = new LinkedList<HopExcessReplica>();
    erlist.add(new HopExcessReplica(1, 1,1));

    //............... Corrupted Replica ...............................................
    List<HopCorruptReplica> crlist = new LinkedList<HopCorruptReplica>();
    crlist.add(new HopCorruptReplica(1, 1,1,1));
  
    //............... Invalidated Blocks ...............................................
    List<HopInvalidatedBlock> iblist = new LinkedList<HopInvalidatedBlock>();
    iblist.add(new HopInvalidatedBlock(1, 1, 1));
    
    
    StorageFactory.getConnector().beginTransaction();    
 
    INodeDataAccess da = (INodeDataAccess) StorageFactory.getDataAccess(INodeDataAccess.class);
    da.prepare(new LinkedList<INode>(), newFiles, new LinkedList<INode>());
    
    BlockInfoDataAccess bda = (BlockInfoDataAccess) StorageFactory.getDataAccess(BlockInfoDataAccess.class);
    bda.prepare(new LinkedList<BlockInfo>(), newBlocks, new LinkedList<BlockInfo>());
    
    ReplicaDataAccess rda = (ReplicaDataAccess) StorageFactory.getDataAccess(ReplicaDataAccess.class);
    rda.prepare(new LinkedList<HopIndexedReplica>(), replicas, new LinkedList<HopIndexedReplica>());
    
    PendingBlockDataAccess pda = (PendingBlockDataAccess) StorageFactory.getDataAccess(PendingBlockDataAccess.class);
    pda.prepare(new LinkedList<PendingBlockInfo>(), pendingList, new LinkedList<PendingBlockInfo>());
    
    LeaseDataAccess lda = (LeaseDataAccess) StorageFactory.getDataAccess(LeaseDataAccess.class);
    lda.prepare(new LinkedList<Lease>(), leases, new LinkedList<Lease>());
    
     LeasePathDataAccess lpda = (LeasePathDataAccess) StorageFactory.getDataAccess(LeasePathDataAccess.class);
    lpda.prepare(new LinkedList<HopLeasePath>(), lpaths, new LinkedList<HopLeasePath>());
    
    ReplicaUnderConstructionDataAccess rucda = (ReplicaUnderConstructionDataAccess) StorageFactory.getDataAccess(ReplicaUnderConstructionDataAccess.class);
    rucda.prepare(new LinkedList<ReplicaUnderConstruction>(), replicasUC, new LinkedList<ReplicaUnderConstruction>());
    
    ExcessReplicaDataAccess erda = (ExcessReplicaDataAccess) StorageFactory.getDataAccess(ExcessReplicaDataAccess.class);
    erda.prepare(new LinkedList<HopExcessReplica>(), erlist, new LinkedList<HopExcessReplica>());
    
    CorruptReplicaDataAccess crda = (CorruptReplicaDataAccess) StorageFactory.getDataAccess(CorruptReplicaDataAccess.class);
    crda.prepare(new LinkedList<HopCorruptReplica>(), crlist, new LinkedList<HopCorruptReplica>());
    
    InvalidateBlockDataAccess ibda = (InvalidateBlockDataAccess) StorageFactory.getDataAccess(InvalidateBlockDataAccess.class);
    ibda.prepare(new LinkedList<HopInvalidatedBlock>(), iblist, new LinkedList<HopInvalidatedBlock>());
    
    StorageFactory.getConnector().commit();

  }
  
  @Test
  public void testReplicationIndex() throws IOException{
//    HDFSTransactionalRequestHandler handler = new HDFSTransactionalRequestHandler(HDFSOperationType.START_FILE) {
//      public TransactionLocks acquireLock() throws PersistanceException, IOException {
//        HDFSTransactionLockAcquirer tla = new HDFSTransactionLockAcquirer();
//        tla.getLocks().addUnderReplicatedBlockFindAll();
//        return tla.acquire();
//      }
//
//      @Override
//      public Object performTask() throws PersistanceException, IOException {
//        List<Integer> ints = new ArrayList<Integer>();
//        for(int i = 0; i < 5; i++){
//          ints.add(i*2);
//        }
//        Variables.setReplicationIndex(ints);
//        return null;
//      }
//    };
//   handler.handle();
   
   HDFSTransactionalRequestHandler handler2 = new HDFSTransactionalRequestHandler(HDFSOperationType.START_FILE) {
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        HDFSTransactionLockAcquirer tla = new HDFSTransactionLockAcquirer();
        tla.getLocks().addUnderReplicatedBlockFindAll();
        return tla.acquire();
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {

        List<Integer> ints = Variables.getReplicationIndex();
        
        for(int i = 0; i < 5; i++){
          System.out.println("index "+i+" value "+ints.get(i));
        }
        return null;
      }
    };
   handler2.handle();
  }
}
