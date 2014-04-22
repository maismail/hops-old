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
public class INodePK {

  public int id;
  public int partKey;

  public INodePK(int id, int partKey) {
    this.id = id;
    this.partKey = partKey;
  }

  @Override
  public boolean equals(Object obj) {
    if(!(obj  instanceof INodePK)) {
      return false;
    }
    INodePK other = (INodePK) obj;
    if(other.id == id && other.partKey == partKey){
      return true;
    }
    return false;
  }
  
  
}
