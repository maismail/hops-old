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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.Block;
import se.sics.hop.metadata.INodeIdentifier;
import se.sics.hop.exception.StorageException;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.metadata.StorageFactory;
import org.junit.Test;
import se.sics.hop.transaction.handler.HopsTransactionalRequestHandler;
import se.sics.hop.transaction.lock.HopsLockFactory;
import static se.sics.hop.transaction.lock.HopsLockFactory.BLK;
import se.sics.hop.common.INodeUtil;
import se.sics.hop.transaction.lock.TransactionLocks;


/**
 * This test makes sure that 
 *   CorruptReplicasMap::numBlocksWithCorruptReplicas and
 *   CorruptReplicasMap::getCorruptReplicaBlockIds
 *   return the correct values
 */
public class TestCorruptReplicaInfo {
  
  private static final Log LOG = 
                           LogFactory.getLog(TestCorruptReplicaInfo.class);
  
  private Map<Integer, BlockInfo> block_map =
    new HashMap<Integer, BlockInfo>();  
    
  // Allow easy block creation by block id
  // Return existing block if one with same block id already exists
  private BlockInfo getBlock(Integer block_id) {
    if (!block_map.containsKey(block_id)) {
      block_map.put(block_id, new BlockInfo(new Block(block_id,0,0),block_id));
    }
    
    return block_map.get(block_id);
  }
  
//  private BlockInfo getBlock(int block_id) {
//    return getBlock(block_id);
//  }
  
  @Test
  public void testCorruptReplicaInfo() throws IOException, 
                                       InterruptedException,
                                       StorageException {
    
      StorageFactory.setConfiguration(new HdfsConfiguration());
      StorageFactory.formatStorage();
      
      CorruptReplicasMap crm = new CorruptReplicasMap(null);
      
      // Make sure initial values are returned correctly
      assertEquals("Number of corrupt blocks must initially be 0", 0, crm.size());
      assertNull("Param n cannot be less than 0", crm.getCorruptReplicaBlockIds(-1, null));
      assertNull("Param n cannot be greater than 100", crm.getCorruptReplicaBlockIds(101, null));
      long[] l = crm.getCorruptReplicaBlockIds(0, null);
      assertNotNull("n = 0 must return non-null", l);
      assertEquals("n = 0 must return an empty list", 0, l.length);

      // create a list of block_ids. A list is used to allow easy validation of the
      // output of getCorruptReplicaBlockIds
      int NUM_BLOCK_IDS = 140;
      List<Integer> block_ids = new LinkedList<Integer>();
      for (int i=0;i<NUM_BLOCK_IDS;i++) {
        block_ids.add(i);
      }
      
      DatanodeDescriptor dn1 = DFSTestUtil.getLocalDatanodeDescriptor();
      DatanodeDescriptor dn2 = DFSTestUtil.getLocalDatanodeDescriptor();
      
   
      
      addToCorruptReplicasMap(crm, getBlock(0), dn1, "TEST");
      assertEquals("Number of corrupt blocks not returning correctly",
                   1, crm.size());
      addToCorruptReplicasMap(crm, getBlock(1), dn1, "TEST");
      assertEquals("Number of corrupt blocks not returning correctly",
                   2, crm.size());
      
      addToCorruptReplicasMap(crm, getBlock(1), dn2, "TEST");
      assertEquals("Number of corrupt blocks not returning correctly",
                   2, crm.size());
      
      removeFromCorruptReplicasMap(crm, getBlock(1));
      assertEquals("Number of corrupt blocks not returning correctly",
                   1, crm.size());
      
      removeFromCorruptReplicasMap(crm, getBlock(0));
      assertEquals("Number of corrupt blocks not returning correctly",
                   0, crm.size());
      
      for (Integer block_id: block_ids) {
        addToCorruptReplicasMap(crm, getBlock(block_id), dn1, "TEST");
      }
            
      assertEquals("Number of corrupt blocks not returning correctly",
                   NUM_BLOCK_IDS, crm.size());
      
      assertTrue("First five block ids not returned correctly ",
                Arrays.equals(new long[]{0,1,2,3,4},
                              crm.getCorruptReplicaBlockIds(5, null)));
                              
      LOG.info(crm.getCorruptReplicaBlockIds(10, 7L));
      LOG.info(block_ids.subList(7, 18));

      assertTrue("10 blocks after 7 not returned correctly ",
                Arrays.equals(new long[]{8,9,10,11,12,13,14,15,16,17},
                              crm.getCorruptReplicaBlockIds(10, 7L)));
      
  }
  
  private void addToCorruptReplicasMap(final CorruptReplicasMap crm, final BlockInfo blk, final DatanodeDescriptor dn, final String reason) throws IOException{
     new HopsTransactionalRequestHandler(HDFSOperationType.TEST_CORRUPT_REPLICA_INFO) {
       INodeIdentifier inodeIdentifier;

       @Override
       public void setUp() throws StorageException, IOException {
         inodeIdentifier = INodeUtil.resolveINodeFromBlock(blk);
       }

       @Override
       public void acquireLock(TransactionLocks locks) throws IOException {
         HopsLockFactory lf = HopsLockFactory.getInstance();
         locks.add(lf.getIndividualBlockLock(blk.getBlockId(), inodeIdentifier))
                 .add(lf.getBlockRelated(BLK.CR));
       }

       @Override
       public Object performTask() throws StorageException, IOException {
         crm.addToCorruptReplicasMap(blk, dn, reason);
         return null;
       }
    }.handle();
  }
  
  private void removeFromCorruptReplicasMap(final CorruptReplicasMap crm, final BlockInfo blk) throws IOException{
    new HopsTransactionalRequestHandler(HDFSOperationType.TEST_CORRUPT_REPLICA_INFO) {
       INodeIdentifier inodeIdentifier;

       @Override
       public void setUp() throws StorageException, IOException {
         inodeIdentifier = INodeUtil.resolveINodeFromBlock(blk);
       }

       @Override
       public void acquireLock(TransactionLocks locks) throws IOException {
         HopsLockFactory lf = HopsLockFactory.getInstance();
         locks.add(lf.getIndividualBlockLock(blk.getBlockId(), inodeIdentifier))
                 .add(lf.getBlockRelated(BLK.CR));
       }

       @Override
       public Object performTask() throws StorageException, IOException {
          crm.removeFromCorruptReplicasMap(blk);
        return null;
       }
    }.handle();
  }
}
