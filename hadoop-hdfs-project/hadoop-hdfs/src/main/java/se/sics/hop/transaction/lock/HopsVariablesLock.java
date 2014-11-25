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
import java.util.EnumMap;
import java.util.Map;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopVariable;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class HopsVariablesLock extends HopsLock {

  private final Map<HopVariable.Finder, TransactionLockTypes.LockType> variables;

  public HopsVariablesLock() {
    this.variables = new EnumMap<HopVariable.Finder, TransactionLockTypes.LockType>(HopVariable.Finder.class);
  }

  public HopsVariablesLock addVariable(HopVariable.Finder variableType, TransactionLockTypes.LockType lockType) {
    this.variables.put(variableType, lockType);
    return this;
  }

  @Override
  void acquire(TransactionLocks locks) throws IOException {
    for (Map.Entry<HopVariable.Finder, TransactionLockTypes.LockType> e : variables.entrySet()) {
      acquireLock(e.getValue(), e.getKey());
    }
  }

  TransactionLockTypes.LockType getVariableLockType(HopVariable.Finder variableType) {
    return variables.get(variableType);
  }

  @Override
  Type getType() {
    return Type.Variable;
  }
}
