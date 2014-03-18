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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.DataNodeTestUtils;
import org.apache.hadoop.hdfs.server.namenode.INode;
import se.sics.hop.metadata.lock.INodeUtil;
import se.sics.hop.metadata.lock.HDFSTransactionLockAcquirer;
import se.sics.hop.transaction.lock.TransactionLockTypes.INodeLockType;
import se.sics.hop.transaction.lock.TransactionLockTypes.LockType;
import se.sics.hop.transaction.lock.TransactionLocks;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.transaction.handler.HDFSTransactionalRequestHandler;
import se.sics.hop.exception.StorageException;
import org.junit.Test;

public class TestUnderReplicatedBlocks {
  @Test(timeout=300000) // 5 min timeout
  public void testSetrepIncWithUnderReplicatedBlocks() throws Exception {
    Configuration conf = new HdfsConfiguration();
    final short REPLICATION_FACTOR = 2;
    final String FILE_NAME = "/testFile";
    final Path FILE_PATH = new Path(FILE_NAME);
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(REPLICATION_FACTOR + 1).build();
    try {
      // create a file with one block with a replication factor of 2
      final FileSystem fs = cluster.getFileSystem();
      DFSTestUtil.createFile(fs, FILE_PATH, 1L, REPLICATION_FACTOR, 1L);
      DFSTestUtil.waitReplication(fs, FILE_PATH, REPLICATION_FACTOR);
      
      // remove one replica from the blocksMap so block becomes under-replicated
      // but the block does not get put into the under-replicated blocks queue
      final BlockManager bm = cluster.getNamesystem().getBlockManager();
      final ExtendedBlock b = DFSTestUtil.getFirstBlock(fs, FILE_PATH);

      HDFSTransactionalRequestHandler handler = new HDFSTransactionalRequestHandler(HDFSOperationType.TEST) {
        Long inodeID = null, pID = null;
        String name = null;

        @Override
        public void setUp() throws StorageException {
          Block blk = b.getLocalBlock();
          INode inode;
          if (blk instanceof BlockInfo) {
            inodeID = ((BlockInfo) blk).getInodeId();
            inode = INodeUtil.indexINodeScanById(((BlockInfo) blk).getInodeId());

          } else {
            inode = INodeUtil.findINodeByBlockId(blk.getBlockId());
          }

          if (inode != null) {
            name = inode.getLocalName();
            pID = inode.getParentId();
            inodeID = inode.getId();
          }
        }

        @Override
        public TransactionLocks acquireLock() throws PersistanceException, IOException {
          HDFSTransactionLockAcquirer tla = new HDFSTransactionLockAcquirer();
          tla.getLocks().
                  addINode(INodeLockType.WRITE).
                  addBlock(b.getBlockId()).
                  addReplica().
                  addInvalidatedBlock();
          return tla.acquireByBlock(inodeID, pID, name);
        }

        @Override
        public Object performTask() throws PersistanceException, IOException {
          DatanodeDescriptor dn = bm.blocksMap.nodeIterator(b.getLocalBlock()).next();
          bm.addToInvalidates(b.getLocalBlock(), dn);
          
          bm.blocksMap.removeNode(b.getLocalBlock(), dn);
          return dn;
        }
      };
      DatanodeDescriptor dn = (DatanodeDescriptor)handler.handle();
      
      //PATCH https://issues.apache.org/jira/browse/HDFS-4067
      // Compute the invalidate work in NN, and trigger the heartbeat from DN
      BlockManagerTestUtil.computeAllPendingWork(bm);
      DataNodeTestUtils.triggerHeartbeat(cluster.getDataNode(dn.getIpcPort()));
      // Wait to make sure the DataNode receives the deletion request 
      Thread.sleep(1000);
      
          
      HDFSTransactionalRequestHandler handler2 = new HDFSTransactionalRequestHandler(HDFSOperationType.TEST) {
        Long inodeID = null, pID = null;
        String name = null;

        @Override
        public void setUp() throws StorageException {
          Block blk = b.getLocalBlock();
          INode inode;
          if (blk instanceof BlockInfo) {
            inodeID = ((BlockInfo) blk).getInodeId();
            inode = INodeUtil.indexINodeScanById(((BlockInfo) blk).getInodeId());

          } else {
            inode = INodeUtil.findINodeByBlockId(blk.getBlockId());
          }

          if (inode != null) {
            name = inode.getLocalName();
            pID = inode.getParentId();
            inodeID = inode.getId();
          }
        }

        @Override
        public TransactionLocks acquireLock() throws PersistanceException, IOException {
          HDFSTransactionLockAcquirer tla = new HDFSTransactionLockAcquirer();
          tla.getLocks().
                  addINode(INodeLockType.WRITE).
                  addBlock(b.getBlockId()).
                  addReplica().
                  addInvalidatedBlock();
          return tla.acquireByBlock(inodeID, pID, name);
        }

        @Override
        public Object performTask() throws PersistanceException, IOException {
          DatanodeDescriptor dn = (DatanodeDescriptor)getParams()[0];
          // Remove the record from blocksMap
          bm.blocksMap.removeNode(b.getLocalBlock(), dn);
          return null;
        }
      };
      handler2.setParams(dn);
      handler2.handle();
      
      // increment this file's replication factor
      FsShell shell = new FsShell(conf);
      assertEquals(0, shell.run(new String[]{
          "-setrep", "-w", Integer.toString(1+REPLICATION_FACTOR), FILE_NAME}));
    } finally {
      cluster.shutdown();
    }
    
  }

}
