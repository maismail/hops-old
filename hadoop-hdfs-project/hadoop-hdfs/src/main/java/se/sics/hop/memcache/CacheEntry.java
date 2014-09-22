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
package se.sics.hop.memcache;

import java.io.Serializable;
import java.util.Arrays;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class CacheEntry implements Serializable{

  private int[] inodeIds;

  public CacheEntry(int[] inodeIds) {
    this.inodeIds = inodeIds;
  }

  public int getPartitionKey() {
    return inodeIds[inodeIds.length - 1];
  }

  public int[] getParentIds() {
    return Arrays.copyOfRange(inodeIds, 0, inodeIds.length - 1);
  }

  public int[] getInodeIds() {
    return inodeIds;
  }

  @Override
  public String toString() {
    return "CacheEntry{" + "inodeIds=" + Arrays.toString(inodeIds) + '}';
  }
}
