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
package se.sics.hop.metadata.lock;

import java.util.List;
import se.sics.hop.metadata.context.BlockPK;
import se.sics.hop.metadata.hdfs.entity.FinderType;
import se.sics.hop.metadata.hdfs.entity.hdfs.HopINodeCandidatePK;

/**
 *
 * @author salman
 */
class ParallelReadParams{
    List<BlockPK> blockIds;
    List<HopINodeCandidatePK> inodeIds;
    FinderType blockFinder;
    boolean isListBlockFinder;
    FinderType inodeFinder;
    FinderType defaultFinder;

    public ParallelReadParams(List<BlockPK> blockIds, FinderType blockFinder, boolean isListBlockFinder, List<HopINodeCandidatePK> inodeIds, FinderType inodeFinder, FinderType defFinder) {
      this.blockIds = blockIds;
      this.inodeIds = inodeIds;
      this.blockFinder = blockFinder;
      this.inodeFinder = inodeFinder;
      this.defaultFinder = defFinder;
      this.isListBlockFinder = isListBlockFinder;
    }

    public List<BlockPK> getBlockIds() {
      return blockIds;
    }

    public List<HopINodeCandidatePK> getInodeIds() {
      return inodeIds;
    }

    public FinderType getBlockFinder() {
      return blockFinder;
    }

    public FinderType getInodeFinder() {
      return inodeFinder;
    }

    public FinderType getDefaultFinder() {
      return defaultFinder;
    }
    
    public void clear(){
//      if(blockIds != null) blockIds.clear();
//      if(inodeIds != null) inodeIds.clear();
//      blockFinder = null;
//      isListBlockFinder  = false;
//      inodeFinder = null;
//      defaultFinder = null;
    }
  }