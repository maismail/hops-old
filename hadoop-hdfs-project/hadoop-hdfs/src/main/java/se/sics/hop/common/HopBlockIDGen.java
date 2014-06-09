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

import se.sics.hop.exception.PersistanceException;
import se.sics.hop.metadata.Variables;

/**
 *
 * @author salman
 *
 * this function was previously in FSImage.java.
 *
 */
public class HopBlockIDGen {

  private static int BATCH_SIZE;
  private static long endId;
  private static long currentId;

  public static void setBatchSize(int batchSize) {
    BATCH_SIZE = batchSize;
    currentId = endId = 0;
  }

  public static long getUniqueBlockId() throws PersistanceException {
    if (needMoreIds()) {
      getMoreIds();
    }
    return ++currentId;
  }

  public static boolean needMoreIds() {
    return currentId == endId;
  }

  private static void getMoreIds() throws PersistanceException {
    long startId = Variables.getBlockId();
    endId = startId + BATCH_SIZE;
    Variables.setBlockId(endId);
    currentId = startId;
  }
}
