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
package se.sics.hop.metadata.persistence.dalwrapper;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.io.DataOutputBuffer;
import se.sics.hop.metadata.persistence.DALWrapper;
import se.sics.hop.metadata.persistence.dal.BlockTokenKeyDataAccess;
import se.sics.hop.metadata.persistence.entity.hdfs.HopBlockKey;
import se.sics.hop.metadata.persistence.exceptions.StorageException;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class BlockTokenDALWrapper extends DALWrapper<BlockKey, HopBlockKey> {

  private final BlockTokenKeyDataAccess dataAccess;
  
  public BlockTokenDALWrapper(BlockTokenKeyDataAccess dataAccess){
    this.dataAccess = dataAccess;
  }
  
  
  public BlockKey findByKeyId(int id) throws StorageException {
    return convertDALtoHDFS(dataAccess.findByKeyId(id));
  }

  public BlockKey findByKeyType(short type) throws StorageException {
    return convertDALtoHDFS(dataAccess.findByKeyType(type));
  }

  public List<BlockKey> findAll() throws StorageException {
    return (List<BlockKey>) convertDALtoHDFS(dataAccess.findAll());
  }

  public void prepare(Collection<BlockKey> removed, Collection<BlockKey> newed, Collection<BlockKey> modified) throws StorageException {
   dataAccess.prepare(convertHDFStoDAL(removed), convertHDFStoDAL(newed), convertHDFStoDAL(modified));
  }

  public void removeAll() throws StorageException {
    dataAccess.removeAll();
  }
  
  @Override
  public HopBlockKey convertHDFStoDAL(BlockKey hdfsClass){
    try {
      DataOutputBuffer keyBytes = new DataOutputBuffer();
      hdfsClass.write(keyBytes);
      return new HopBlockKey(hdfsClass.getKeyId(), hdfsClass.getExpiryDate(), keyBytes.getData(), hdfsClass.getKeyType());
    } catch (IOException ex) {
      return null;
    }
  }

  @Override
  public BlockKey convertDALtoHDFS(HopBlockKey dalClass) {
    try {
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(dalClass.getKeyBytes()));
      BlockKey bKey = new BlockKey();
      bKey.readFields(dis);
      bKey.setKeyType(dalClass.getKeyType());
      return bKey;
    } catch (IOException ex) {
      return null;
    }
  }

}
