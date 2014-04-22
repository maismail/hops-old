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
package se.sics.hop.metadata.context;

/**
 *
 * @author salman
 */
public class BlockPK {

  public long id;
  public int inodeId;
  public int partKey;

  public BlockPK(long id, int inodeId, int partKey) {
    this.id = id;
    this.inodeId = inodeId;
    this.partKey = partKey;
  }
  
   @Override
  public boolean equals(Object obj) {
    if(!(obj  instanceof BlockPK)) {
      return false;
    }
    BlockPK other = (BlockPK) obj;
    if(other.id == id && other.partKey == partKey && other.inodeId == inodeId){
      return true;
    }
    return false;
  }
}