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
package se.sics.hop.metadata.persistence;

/**
 *
 * @author salman
 * FSImage had some function that returned Id of 
 * last successful Tx. It got last Tx IDs from the journal 
 * 
 * we have to get rid of this fun
 */
public class HOPTXnChkPtsIDs 
{
  private static long txID = 0;
  private static long chkPtID = 0;  
  private static long chkPtTime = System.currentTimeMillis();
  
  static public synchronized long getLastAppliedOrWrittenTxId() {
    return ++txID;
  }
  
  static public synchronized long getLastWrittenTxId()
  {
    return ++txID;
  }
  
  static public synchronized long getCurSegmentTxId()
  {
    return txID;
  }
  
  static public synchronized long getMostRecentCheckpointTxId() {
    return ++chkPtID;
  }
  
  static public synchronized long getMostRecentCheckpointTime()
  {
    long oldTime = chkPtTime;
    chkPtTime = System.currentTimeMillis();
    return oldTime;
  }
}
