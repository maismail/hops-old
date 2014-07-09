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
package se.sics.hop.common;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class IDsMonitor implements Runnable{
  
  private static final Log LOG = LogFactory.getLog(IDsMonitor.class);
  
  private static IDsMonitor instance = null;
  private Thread th = null;
  
  private int inodeIdsThreshold;
  private int blockIdsThreshold;
  private int checkInterval;
  
  private IDsMonitor(){
    
  }
  
  public static IDsMonitor getInstance(){
    if(instance == null){
      instance = new IDsMonitor();
    }
    return instance;
  }
  
  public void setConfiguration(int inodeIdsBatchSize, int blockIdsBatchSize, float inodeIdsThreshold, float blockIdsThreshold, int checkInterval){
    HopINodeIdGen.setBatchSize(inodeIdsBatchSize);
    HopBlockIdGen.setBatchSize(blockIdsBatchSize);
    this.inodeIdsThreshold = (int) (inodeIdsThreshold * inodeIdsBatchSize);
    this.blockIdsThreshold = (int) (blockIdsThreshold * blockIdsBatchSize);
    this.checkInterval = checkInterval;
  }
  
  public void start() {
    th = new Thread(this, "IDsMonitor");
    th.setDaemon(true);
    th.start();
  }
  
  @Override
  public void run() {
    while(true){
      try {
        if(HopINodeIdGen.getMoreIdsIfNeeded(inodeIdsThreshold)){
          LOG.debug("get more inode ids " + HopINodeIdGen.getCQ());
        }
        
        if(HopBlockIdGen.getMoreIdsIfNeeded(blockIdsThreshold)){
          LOG.debug("get more block ids " + HopBlockIdGen.getCQ());
        }
        Thread.sleep(checkInterval);
      } catch (InterruptedException ex) {
        LOG.warn("IDsMonitor interrupted: " + ex);
      } catch (IOException ex) {
        LOG.warn("IDsMonitor got exception: " + ex);
      }
    }
  }
  
}
