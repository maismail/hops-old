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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.INodeDirectoryWithQuota;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;
import org.junit.Before;
import org.junit.Test;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.StorageInitializtionException;
import se.sics.hop.metadata.StorageFactory;
import se.sics.hop.metadata.hdfs.dal.BlockInfoDataAccess;
import se.sics.hop.metadata.hdfs.dal.INodeDataAccess;
import se.sics.hop.metadata.hdfs.dal.PendingBlockDataAccess;
import se.sics.hop.metadata.hdfs.dal.ReplicaDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.HopIndexedReplica;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.transaction.handler.LightWeightRequestHandler;
import se.sics.hop.transaction.handler.RequestHandler;
import se.sics.hop.transaction.handler.RequestHandler.OperationType;

/**
 *
 * @author salman
 */
public class TestNDBSizer {

  HdfsConfiguration conf = null;
  INodeDirectory root, dir1, dir2, dir3, dir4, dir5, dir6;
  INodeFile file1, file2, file3;

  @Before
  public void init() throws IOException {
    conf = new HdfsConfiguration();
    StorageFactory.setConfiguration(conf);
    StorageFactory.getConnector().formatStorage();


  }

 // @Test
  public void testInsertData() throws Exception {
    insertData();
  }

  private void insertData() throws StorageException, PersistanceException, IOException {
    System.out.println("Building the data...");


    final int NUM_INODES = 500000;
    final int NUM_BLOCKS = 1200000;//2000000;
    final int NUM_REPLICAS = 3600000;//6000000;
    final int BATCH_SIZE = 5000;


    String filename = "";
    for(int i =0; i < 100; i++){
      filename+="-";
    }

    final List<INode> newFiles = new LinkedList<INode>();
    for (int i = 0; i < NUM_INODES; i++) {
      INodeDirectory dir = new INodeDirectoryWithQuota("", new PermissionStatus("salman", "usr", new FsPermission((short) 0777)));
      dir.setIdNoPersistance(i);
      dir.setLocalNameNoPersistance(filename+Integer.toString(i));
      dir.setParentIdNoPersistance(i);
      newFiles.add(dir);
      if (newFiles.size() >= BATCH_SIZE) {
        final int j = i;
        new LightWeightRequestHandler(HDFSOperationType.TEST) {
          @Override
          public Object performTask() throws PersistanceException, IOException {
            INodeDataAccess da = (INodeDataAccess) StorageFactory.getDataAccess(INodeDataAccess.class);
            da.prepare(new LinkedList<INode>(), newFiles, new LinkedList<INode>());
            newFiles.clear();
            showProgressBar("INodes", j,NUM_INODES);
            return null;
          }
        }.handle();

      }
    }


    System.out.println();


    final List<BlockInfo> newBlocks = new LinkedList<BlockInfo>();
    for (int i = 0; i < NUM_BLOCKS; i++) {
      BlockInfo block = new BlockInfo();
      block.setINodeIdNoPersistance(i);
      block.setBlockIdNoPersistance(i);
      newBlocks.add(block);
      if (newBlocks.size() >= BATCH_SIZE) {
        final int j = i;
        new LightWeightRequestHandler(HDFSOperationType.TEST) {
          @Override
          public Object performTask() throws PersistanceException, IOException {
            BlockInfoDataAccess bda = (BlockInfoDataAccess) StorageFactory.getDataAccess(BlockInfoDataAccess.class);
            bda.prepare(new LinkedList<BlockInfo>(), newBlocks, new LinkedList<BlockInfo>());
            newBlocks.clear();
            showProgressBar("Blocks", j,NUM_BLOCKS);
            return null;
          }
        }.handle();

      }
    }


    System.out.println();
    
    final List<HopIndexedReplica> replicas = new LinkedList<HopIndexedReplica>();
    for (int i = 0; i < NUM_REPLICAS; i++) {
      replicas.add(new HopIndexedReplica(i, i, i, i));
      if (replicas.size() >= BATCH_SIZE) {
        final int j = i;
        new LightWeightRequestHandler(HDFSOperationType.TEST) {
          @Override
          public Object performTask() throws PersistanceException, IOException {
            ReplicaDataAccess rda = (ReplicaDataAccess) StorageFactory.getDataAccess(ReplicaDataAccess.class);
            rda.prepare(new LinkedList<HopIndexedReplica>(), replicas, new LinkedList<HopIndexedReplica>());
            //StorageFactory.getConnector().commit();
            replicas.clear();
            showProgressBar("Replicas", j,NUM_REPLICAS);
            return null;
          }
        }.handle();

      }
    }
  }
  
  public static void showProgressBar(String msg, double achieved, double total) throws IOException{
    int percent = (int)((achieved/total)*(double)100);
    System.out.print("\r");
    StringBuffer bar = new StringBuffer(msg+" |");
    for(int i = 0; i < percent; i++){
      bar.append(".");
    }
    for(int i = 0 ; i < (100-percent);i++){
      bar.append(" ");
    }
    bar.append("|"+" "+percent+"%");   
   
    System.out.println(bar);
    //System.out.write((int)achieved);
  }
}
