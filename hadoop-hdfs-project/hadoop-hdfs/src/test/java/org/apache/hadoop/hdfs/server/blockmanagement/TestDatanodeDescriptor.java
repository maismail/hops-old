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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import se.sics.hop.metadata.lock.TransactionLockAcquirer;
import se.sics.hop.metadata.lock.TransactionLockTypes.LockType;
import se.sics.hop.metadata.lock.HDFSTransactionLocks;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.transaction.handler.TransactionalRequestHandler;
import se.sics.hop.metadata.StorageFactory;
import org.junit.Test;

/**
 * This class tests that methods in DatanodeDescriptor
 */
public class TestDatanodeDescriptor {
  /**
   * Test that getInvalidateBlocks observes the maxlimit.
   */
  @Test
  public void testGetInvalidateBlocks() throws Exception {
    final int MAX_BLOCKS = 10;
    final int REMAINING_BLOCKS = 2;
    final int MAX_LIMIT = MAX_BLOCKS - REMAINING_BLOCKS;
    
    DatanodeDescriptor dd = DFSTestUtil.getLocalDatanodeDescriptor();
    ArrayList<Block> blockList = new ArrayList<Block>(MAX_BLOCKS);
    for (int i=0; i<MAX_BLOCKS; i++) {
      blockList.add(new Block(i, 0, GenerationStamp.FIRST_VALID_STAMP));
    }
    dd.addBlocksToBeInvalidated(blockList);
    Block[] bc = dd.getInvalidateBlocks(MAX_LIMIT);
    assertEquals(bc.length, MAX_LIMIT);
    bc = dd.getInvalidateBlocks(MAX_LIMIT);
    assertEquals(bc.length, REMAINING_BLOCKS);
  }
  
  @Test
  public void testBlocksCounter() throws Exception {
    StorageFactory.setConfiguration(new HdfsConfiguration());
    StorageFactory.getConnector().formatStorage();
    
    DatanodeDescriptor dd = DFSTestUtil.getLocalDatanodeDescriptor();
    assertEquals(0, dd.numBlocks());
    BlockInfo blk = new BlockInfo(new Block(1L));
    BlockInfo blk1 = new BlockInfo(new Block(2L));
    // add first block
    assertTrue(addBlock(dd, blk));
    assertEquals(1, dd.numBlocks());
    // remove a non-existent block
    assertFalse(removeBlock(dd, blk1));
    assertEquals(1, dd.numBlocks());
    // add an existent block
    assertFalse(addBlock(dd, blk));
    assertEquals(1, dd.numBlocks());
    // add second block
    assertTrue(addBlock(dd, blk1));
    assertEquals(2, dd.numBlocks());
    // remove first block
    assertTrue(removeBlock(dd, blk));
    assertEquals(1, dd.numBlocks());
    // remove second block
    assertTrue(removeBlock(dd, blk1));
    assertEquals(0, dd.numBlocks());    
  }
  
    private boolean addBlock(final DatanodeDescriptor dn, final BlockInfo blk) throws IOException{
     return (Boolean) new TransactionalRequestHandler(HDFSOperationType.TEST) {
      @Override
      public HDFSTransactionLocks acquireLock() throws PersistanceException, IOException {
        TransactionLockAcquirer tla = new TransactionLockAcquirer();
        tla.getLocks().
                addBlock(LockType.WRITE, blk.getBlockId()).
                addReplica(LockType.WRITE);
        return tla.acquire();
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        return dn.addBlock(blk);
      }
    }.handle();
  }
    
    private boolean removeBlock(final DatanodeDescriptor dn, final BlockInfo blk) throws IOException{
     return (Boolean) new TransactionalRequestHandler(HDFSOperationType.TEST) {
      @Override
      public HDFSTransactionLocks acquireLock() throws PersistanceException, IOException {
        TransactionLockAcquirer tla = new TransactionLockAcquirer();
        tla.getLocks().
                addBlock(LockType.WRITE, blk.getBlockId()).
                addReplica(LockType.WRITE);
        return tla.acquire();
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        return dn.removeBlock(blk);
      }
    }.handle();
  }
}
