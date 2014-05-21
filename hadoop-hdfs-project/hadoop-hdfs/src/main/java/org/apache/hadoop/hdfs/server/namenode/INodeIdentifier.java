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
package org.apache.hadoop.hdfs.server.namenode;

/**
 *
 * @author salman
 */
public class INodeIdentifier {
  int inodeID;

  public INodeIdentifier(int inodeID) {
    this.inodeID = inodeID;
  }

  public int getInodeId() {
    return inodeID;
  }

  public void setInode_id(int inodeID) {
    this.inodeID = inodeID;
  }  
}
