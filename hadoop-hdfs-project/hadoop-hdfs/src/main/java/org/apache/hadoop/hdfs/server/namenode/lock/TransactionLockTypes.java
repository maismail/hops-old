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
package org.apache.hadoop.hdfs.server.namenode.lock;

/**
 *
 * @author salman
 */
public class TransactionLockTypes {
    public enum LockType {

    WRITE, READ, READ_COMMITTED
  }

  public enum INodeLockType {

    WRITE,
    WRITE_ON_PARENT // Write lock on the parent of the last path component. This has the WRITE effect when using inode-id.
    , READ, READ_COMMITED // No Lock
  }

  public enum INodeResolveType {
    PATH // resolve only the given path
    , PATH_WITH_UNKNOWN_HEAD // resolve a path which some of its path components might not exist
    , PATH_AND_IMMEDIATE_CHILDREN // resolve path and find the given directory's children
    , PATH_AND_ALL_CHILDREN_RECURESIVELY // resolve the given path and find all the children recursively.
  }
  
    
}
