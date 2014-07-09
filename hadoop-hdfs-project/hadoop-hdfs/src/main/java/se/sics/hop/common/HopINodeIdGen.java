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

import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import se.sics.hop.metadata.Variables;
import se.sics.hop.exception.PersistanceException;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class HopINodeIdGen {

  private static int BATCH_SIZE;
  private static int endId;
  private static int currentId;

  public static void setBatchSize(int batchSize) {
    BATCH_SIZE = batchSize;
    currentId = endId = 0;
  }

  public static int getUniqueINodeID() throws PersistanceException {
    if (needMoreIds(1)) {
      getMoreIds();
    }
    return ++currentId;
  }

  public static boolean needMoreIds(int expectedMaxNumberOfInodeIds) {
    if(expectedMaxNumberOfInodeIds > 1){
      return (currentId + expectedMaxNumberOfInodeIds) >= endId;
    }
    return currentId == endId;
  }

  private synchronized static void getMoreIds() throws PersistanceException {
    int startId = Variables.getInodeId();
    endId = startId + BATCH_SIZE;
    Variables.setInodeId(endId);
    currentId = startId;
  }
}
