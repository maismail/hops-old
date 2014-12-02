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

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 * @author Steffen Grohsschmiedt <steffeng@sics.se>
 */
final class HopsTransactionalLockAcquirer extends TransactionLockAcquirer {

  private final HopsTransactionLocks locks;

  HopsTransactionalLockAcquirer() {
    locks = new HopsTransactionLocks();
  }

  @Override
  public void addLock(HopsLock lock) {
    locks.add(lock);
  }

  @Override
  public void acquire() throws Exception {
    for (HopsLock lock : locks.getSortedLocks()) {
      lock.acquire(locks);
    }
  }

  @Override
  public TransactionLocks getLocks() {
    return locks;
  }
}
