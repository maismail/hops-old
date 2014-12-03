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
package se.sics.hop.transaction.handler;

import java.io.IOException;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import se.sics.hop.memcache.PathMemcache;
import se.sics.hop.transaction.TransactionInfo;
import se.sics.hop.transaction.lock.HopsTransactionalLockAcquirer;
import se.sics.hop.transaction.lock.TransactionLockAcquirer;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public abstract class HopsTransactionalRequestHandler extends TransactionalRequestHandler{

  private final String path;
  
  public HopsTransactionalRequestHandler(HDFSOperationType opType) {
    this(opType, null);
  }
  
  public HopsTransactionalRequestHandler(HDFSOperationType opType, String path) {
    super(opType);
    this.path = path;
  }

  @Override
  protected TransactionLockAcquirer newLockAcquirer() {
    return new HopsTransactionalLockAcquirer();
  }

  
  @Override
  protected Object execute(final Object namesystem) throws IOException {

    return super.execute(new TransactionInfo() {
      @Override
      public String getContextName(OperationType opType) {
        if (namesystem != null && namesystem instanceof FSNamesystem) {
          return "NN (" + ((FSNamesystem) namesystem).getNamenodeId() + ") " +
              opType.toString() + "[" + Thread.currentThread().getId() + "]";
        } else {
          return opType.toString();
        }
      }

      @Override
      public void performPostTransactionAction() throws IOException {
        if (namesystem != null && namesystem instanceof FSNamesystem) {
          ((FSNamesystem) namesystem).performPendingSafeModeOperation();
        }
      }
    });
  }

  @Override
  protected final void preTransactionSetup() throws IOException {
    if(path != null){
      PathMemcache.getInstance().get(path);
    }
    setUp();
  }

  public void setUp() throws IOException {

  }
}
