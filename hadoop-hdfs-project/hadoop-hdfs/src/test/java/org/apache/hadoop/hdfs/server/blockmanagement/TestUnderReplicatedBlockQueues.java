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
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import se.sics.hop.metadata.lock.HDFSTransactionLockAcquirer;
import se.sics.hop.transaction.lock.TransactionLockTypes.LockType;
import se.sics.hop.transaction.lock.TransactionLocks;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.transaction.handler.HDFSTransactionalRequestHandler;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.metadata.StorageFactory;
import org.junit.Assert;
import org.junit.Test;

public class TestUnderReplicatedBlockQueues extends Assert {

  /**
   * Test that adding blocks with different replication counts puts them
   * into different queues
   * @throws Throwable if something goes wrong
   */
  @Test
  public void testBlockPriorities() throws Throwable  {
    StorageFactory.setConfiguration(new HdfsConfiguration());
    StorageFactory.getConnector().formatStorage();
    
    UnderReplicatedBlocks queues = new UnderReplicatedBlocks();
    Block block1 = add(new Block(1));
    Block block2 = add(new Block(2));
    Block block_very_under_replicated = add(new Block(3));
    Block block_corrupt = add(new Block(4));

    //add a block with a single entry
    assertAdded(queues, block1, 1, 0, 3);

    assertEquals(1, queues.getUnderReplicatedBlockCount());
    assertEquals(1, queues.size());
    assertInLevel(queues, block1, UnderReplicatedBlocks.QUEUE_HIGHEST_PRIORITY);
    //repeated additions fail
    assertFalse(add(queues, block1, 1, 0, 3));

    //add a second block with two replicas
    assertAdded(queues, block2, 2, 0, 3);
    assertEquals(2, queues.getUnderReplicatedBlockCount());
    assertEquals(2, queues.size());
    assertInLevel(queues, block2, UnderReplicatedBlocks.QUEUE_UNDER_REPLICATED);
    //now try to add a block that is corrupt
    assertAdded(queues, block_corrupt, 0, 0, 3);
    assertEquals(3, queues.size());
    assertEquals(2, queues.getUnderReplicatedBlockCount());
    assertEquals(1, queues.getCorruptBlockSize());
    assertInLevel(queues, block_corrupt,
                  UnderReplicatedBlocks.QUEUE_WITH_CORRUPT_BLOCKS);

    //insert a very under-replicated block
    assertAdded(queues, block_very_under_replicated, 4, 0, 25);
    assertInLevel(queues, block_very_under_replicated,
                  UnderReplicatedBlocks.QUEUE_VERY_UNDER_REPLICATED);

  }

  private void assertAdded(UnderReplicatedBlocks queues,
                           Block block,
                           int curReplicas,
                           int decomissionedReplicas,
                           int expectedReplicas) throws IOException {
    assertTrue("Failed to add " + block,
               add(queues, block,
                          curReplicas,
                          decomissionedReplicas,
                          expectedReplicas));
  }

  /**
   * Determine whether or not a block is in a level without changing the API.
   * Instead get the per-level iterator and run though it looking for a match.
   * If the block is not found, an assertion is thrown.
   *
   * This is inefficient, but this is only a test case.
   * @param queues queues to scan
   * @param block block to look for
   * @param level level to select
   */
  private void assertInLevel(UnderReplicatedBlocks queues,
                             Block block,
                             int level) {
    UnderReplicatedBlocks.BlockIterator bi = queues.iterator(level);
    while (bi.hasNext()) {
      Block next = bi.next();
      if (block.equals(next)) {
        return;
      }
    }
    fail("Block " + block + " not found in level " + level);
  }
  
  
  private Block add(final Block block) throws IOException {
    new HDFSTransactionalRequestHandler(HDFSOperationType.TEST) {
      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        return null;
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        EntityManager.add(new BlockInfo(block));
        return null;
      }
    }.handle();
    return block;
  }

  private boolean add(final UnderReplicatedBlocks queues, final Block block,
          final int curReplicas,
          final int decomissionedReplicas,
          final int expectedReplicas) throws IOException {
    return (Boolean) new HDFSTransactionalRequestHandler(HDFSOperationType.TEST) {
      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        HDFSTransactionLockAcquirer tla = new HDFSTransactionLockAcquirer();
        tla.getLocks().
                addBlock(LockType.READ_COMMITTED, block.getBlockId()).
                addUnderReplicatedBlock(LockType.WRITE);
        return tla.acquire();
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        return queues.add(block, curReplicas, decomissionedReplicas, expectedReplicas);
      }
    }.handle();
  }
}
