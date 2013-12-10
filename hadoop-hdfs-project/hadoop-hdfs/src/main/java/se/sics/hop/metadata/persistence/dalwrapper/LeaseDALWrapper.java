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

import java.util.Collection;
import org.apache.hadoop.hdfs.server.namenode.Lease;
import se.sics.hop.metadata.persistence.DALWrapper;
import se.sics.hop.metadata.persistence.dal.LeaseDataAccess;
import se.sics.hop.metadata.persistence.entity.hdfs.HopLease;
import se.sics.hop.metadata.persistence.exceptions.StorageException;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class LeaseDALWrapper extends DALWrapper<Lease, HopLease> implements LeaseDataAccess<Lease> {

  private final LeaseDataAccess<HopLease> dataAccess;

  public LeaseDALWrapper(LeaseDataAccess<HopLease> dataAcess) {
    this.dataAccess = dataAcess;
  }

  @Override
  public int countAll() throws StorageException {
    return dataAccess.countAll();
  }

  @Override
  public Collection<Lease> findByTimeLimit(long timeLimit) throws StorageException {
    return convertDALtoHDFS(dataAccess.findByTimeLimit(timeLimit));
  }

  @Override
  public Collection<Lease> findAll() throws StorageException {
    return convertDALtoHDFS(dataAccess.findAll());
  }

  @Override
  public Lease findByPKey(String holder) throws StorageException {
    return convertDALtoHDFS(dataAccess.findByPKey(holder));
  }

  @Override
  public Lease findByHolderId(int holderId) throws StorageException {
    return convertDALtoHDFS(dataAccess.findByHolderId(holderId));
  }

  @Override
  public void prepare(Collection<Lease> removed, Collection<Lease> newLeases, Collection<Lease> modified) throws StorageException {
    dataAccess.prepare(convertHDFStoDAL(removed), convertHDFStoDAL(newLeases), convertHDFStoDAL(modified));
  }

  @Override
  public void removeAll() throws StorageException {
    dataAccess.removeAll();
  }

  @Override
  public HopLease convertHDFStoDAL(Lease hdfsClass) {
    if (hdfsClass != null) {
      return new HopLease(hdfsClass.getHolder(), hdfsClass.getHolderID(), hdfsClass.getLastUpdate());
    } else {
      return null;
    }
  }

  @Override
  public Lease convertDALtoHDFS(HopLease dalClass) {
    if (dalClass != null) {
      return new Lease(dalClass.getHolder(), dalClass.getHolderId(), dalClass.getLastUpdate());
    } else {
      return null;
    }
  }
}
