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
package se.sics.hop.transaction.lock;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import static junit.framework.Assert.assertEquals;
import junit.framework.TestCase;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Test;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.metadata.StorageFactory;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.transaction.handler.HopsTransactionalRequestHandler;
import se.sics.hop.transaction.lock.HopsLock;
import se.sics.hop.transaction.lock.HopsTransactionLocks;
import se.sics.hop.transaction.lock.TransactionLockTypes.*;
import se.sics.hop.transaction.lock.TransactionLocks;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class TestHopsTransactionalLockAcquirer extends TestCase{
  
  
  @Test
  public void testTransactionGetSortedLocks(){
    HopsTransactionLocks locks = new HopsTransactionLocks();
//    locks.addLock(new HopsIndividualBlockLock());
//    locks.addLock(new HopsINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, true, ""));
//    locks.addLock(new HopsLeaseLock());
//    locks.addLock(new HopsIndividualINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, true, ""));
    
    List<HopsLock> sortedLocks = locks.getSortedLocks();
    assertEquals(sortedLocks.get(0).getClass(), HopsINodeLock.class);
    assertEquals(sortedLocks.get(1).getClass(), HopsIndividualBlockLock.class);
  }
  
  @Test
  public void testAcquireINodeLock() throws Exception{
    StorageFactory.setConfiguration(new HdfsConfiguration());
    //StorageFactory.formatStorage();
   
    HopsTransactionalRequestHandler handler = new HopsTransactionalRequestHandler(HDFSOperationType.TEST) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException, ExecutionException {
        locks.add(new HopsINodeLock(INodeLockType.WRITE, INodeResolveType.PATH, true, "/f1/f2/test.txt"));
      }
      
      @Override
      public Object performTask() throws PersistanceException, IOException {
        return null;
      }
    };
    
    handler.handle();
  }
}
