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

import java.io.IOException;
import se.sics.hop.metadata.Variables;

/**
 *
 * @author salman
 *
 * this function was previously in FSImage.java.
 *
 */
public class HopBlockIdGen {

  private static int BATCH_SIZE;
  private static CountersQueue cQ;
  
  public static void setBatchSize(int batchSize) {
    BATCH_SIZE = batchSize;
    cQ = new CountersQueue();
  }

  public static int getUniqueBlockId(){
    return (int) cQ.next();
  }

  public synchronized static boolean getMoreIdsIfNeeded(int threshold) throws IOException {
    if (!cQ.has(threshold)) {
      cQ.addCounter(Variables.incrementBlockIdCounter(BATCH_SIZE));
      return true;
    }
    return false;
  }
  
  static CountersQueue getCQ() {
    return cQ;
  }
}
