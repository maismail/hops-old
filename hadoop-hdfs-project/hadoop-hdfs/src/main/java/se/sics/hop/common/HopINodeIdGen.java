/*
 * Copyright 2013 Apache Software Foundation.
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
package se.sics.hop.common;

import se.sics.hop.metadata.persistence.context.Variables;
import se.sics.hop.metadata.persistence.exceptions.PersistanceException;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class HopINodeIdGen {
  
  public static long getUniqueINodeID() throws PersistanceException{
    long lastInodeId =  Variables.getInodeId();
    lastInodeId++;
    Variables.setInodeId(lastInodeId);
    return lastInodeId;
  }
}
