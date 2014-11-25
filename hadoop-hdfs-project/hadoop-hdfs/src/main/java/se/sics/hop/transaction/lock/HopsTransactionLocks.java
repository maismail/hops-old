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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 * @author Steffen Grohsschmiedt <steffeng@sics.se>
 */
public class HopsTransactionLocks implements TransactionLocks{
 
  private final Map<HopsLock.Type, HopsLock> locks;
  
  public HopsTransactionLocks(){
    this.locks = new EnumMap<HopsLock.Type, HopsLock>(HopsLock.Type.class);
  }
  
  @Override
  public void addLock(HopsLock lock){
    if(locks.containsKey(lock.getType()))
      throw new IllegalArgumentException("you can't add the same lock twice!");
    
    locks.put(lock.getType(), lock);
  }
  
  @Override
  public HopsLock getLock(HopsLock.Type type) {
    return locks.get(type);
  }
    
  public List<HopsLock> getSortedLocks(){
    List<HopsLock> lks = new ArrayList<HopsLock>(locks.values());
    Collections.sort(lks);
    return lks;
  }


}
