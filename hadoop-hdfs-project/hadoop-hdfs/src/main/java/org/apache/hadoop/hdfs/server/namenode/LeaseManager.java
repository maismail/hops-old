/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.util.Time.now;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.util.Daemon;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.namenode.lock.INodeUtil;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockManager;
import org.apache.hadoop.hdfs.server.namenode.lock.TransactionLockManager.LockType;
import org.apache.hadoop.hdfs.server.namenode.persistance.EntityManager;
import org.apache.hadoop.hdfs.server.namenode.persistance.LightWeightRequestHandler;
import org.apache.hadoop.hdfs.server.namenode.persistance.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.RequestHandler.OperationType;
import org.apache.hadoop.hdfs.server.namenode.persistance.TransactionalRequestHandler;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.LeaseDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageFactory;

/**
 * LeaseManager does the lease housekeeping for writing on files.   
 * This class also provides useful static methods for lease recovery.
 * 
 * Lease Recovery Algorithm
 * 1) Namenode retrieves lease information
 * 2) For each file f in the lease, consider the last block b of f
 * 2.1) Get the datanodes which contains b
 * 2.2) Assign one of the datanodes as the primary datanode p

 * 2.3) p obtains a new generation stamp from the namenode
 * 2.4) p gets the block info from each datanode
 * 2.5) p computes the minimum block length
 * 2.6) p updates the datanodes, which have a valid generation stamp,
 *      with the new generation stamp and the minimum block length 
 * 2.7) p acknowledges the namenode the update results

 * 2.8) Namenode updates the BlockInfo
 * 2.9) Namenode removes f from the lease
 *      and removes the lease once all files have been removed
 * 2.10) Namenode commit changes to edit log
 */
/**
 * HOP Changes Added
 * @author Mahmoud Ismail <maism@kth.se>
 */
@InterfaceAudience.Private
public class LeaseManager {
  public static final Log LOG = LogFactory.getLog(LeaseManager.class);

  private final FSNamesystem fsnamesystem;

  private long softLimit = HdfsConstants.LEASE_SOFTLIMIT_PERIOD;
  private long hardLimit = HdfsConstants.LEASE_HARDLIMIT_PERIOD;
  
  //HOP: Removed all the datastructures (leases, sortedLeases, sortedLeasesByPath) and the synchronization keywards

  private Daemon lmthread;
  private volatile boolean shouldRunMonitor;

  LeaseManager(FSNamesystem fsnamesystem) {this.fsnamesystem = fsnamesystem;}

  Lease getLease(String holder) throws PersistanceException {
    return EntityManager.find(Lease.Finder.ByPKey, holder);
  }
  
  SortedSet<Lease> getSortedLeases() throws PersistanceException {
    return new TreeSet<Lease>(EntityManager.findList(Lease.Finder.All));
  }

  /** @return the lease containing src */
  public Lease getLeaseByPath(String src) throws PersistanceException {
    LeasePath leasePath = EntityManager.find(LeasePath.Finder.ByPKey, src);
    if (leasePath != null) {
      int holderID = leasePath.getHolderId();
      Lease lease = EntityManager.find(Lease.Finder.ByHolderId, holderID);
      return lease;
    } else {
      return null;
    }
  }

  /** @return the number of leases currently in the system */
  public int countLease() throws IOException {
     return (Integer) new LightWeightRequestHandler(OperationType.COUNT_LEASE) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        LeaseDataAccess da = (LeaseDataAccess) StorageFactory.getDataAccess(LeaseDataAccess.class);
        return da.countAll();
      }
    }.handle();
  }

  /** This method is never called in the stateless implementation 
   * @return the number of paths contained in all leases 
   */
  int countPath() throws PersistanceException {
    return EntityManager.count(Lease.Counter.All);
  }
  
  /**
   * Adds (or re-adds) the lease for the specified file.
   */
  Lease addLease(String holder, String src) throws PersistanceException {
    Lease lease = getLease(holder);
    if (lease == null) {
      int holderID = DFSUtil.getRandom().nextInt();
      lease = new Lease(holder, holderID, now());
      EntityManager.add(lease);
    } else {
      renewLease(lease);
    }

    LeasePath lPath = new LeasePath(src, lease.getHolderID());
    lease.addFirstPath(lPath);
    EntityManager.add(lPath);

    return lease;
  }

  /**
   * Remove the specified lease and src.
   */
  void removeLease(Lease lease, LeasePath src) throws PersistanceException {
    if (lease.removePath(src)) {
      EntityManager.remove(src);
    } else {
      LOG.error(src + " not found in lease.paths (=" + lease.getPaths() + ")");
    }

    if (!lease.hasPath()) {
      EntityManager.remove(lease);
    }
  }

  /**
   * Remove the lease for the specified holder and src
   */
  void removeLease(String holder, String src) throws PersistanceException {
    Lease lease = getLease(holder);
    if (lease != null) {
      removeLease(lease, new LeasePath(src, lease.getHolderID()));
    } else {
      LOG.warn("Removing non-existent lease! holder=" + holder +
          " src=" + src);
    }
  }

  //HOP: FIXME: Add our implementaion
  void removeAllLeases() {
    //sortedLeases.clear();
    //sortedLeasesByPath.clear();
    //leases.clear();
  }

  /**
   * Reassign lease for file src to the new holder.
   */
  Lease reassignLease(Lease lease, String src, String newHolder) throws PersistanceException {
    assert newHolder != null : "new lease holder is null";
    if (lease != null) {
      // Removing lease-path souldn't be persisted in entity-manager since we want to add it to another lease.
      if (!lease.removePath(new LeasePath(src, lease.getHolderID()))) {
        LOG.error(src + " not found in lease.paths (=" + lease.getPaths() + ")");
      }

      if (!lease.hasPath() && !lease.getHolder().equals(newHolder)) {
        EntityManager.remove(lease);

      }
    }

    Lease newLease = getLease(newHolder);
    LeasePath lPath = null;
    if (newLease == null) {
      int holderID = DFSUtil.getRandom().nextInt();
      newLease = new Lease(newHolder, holderID, now());
      EntityManager.add(newLease);
      lPath = new LeasePath(src, newLease.getHolderID());
      newLease.addFirstPath(lPath); // [lock] First time, so no need to look for lease-paths
    } else {
      renewLease(newLease);
      lPath = new LeasePath(src, newLease.getHolderID());
      newLease.addPath(lPath);
    }
    // update lease-paths' holder
    EntityManager.update(lPath);
    
    return newLease;
  }

  /**
   * Finds the pathname for the specified pendingFile
   */
  public String findPath(INodeFileUnderConstruction pendingFile)
      throws IOException, PersistanceException {
     assert pendingFile.isUnderConstruction();
    Lease lease = getLease(pendingFile.getClientName());
    if (lease != null) {
      String src = null;
      try {
        for (LeasePath lpath : lease.getPaths()) {
          if (fsnamesystem.dir.getINode(lpath.getPath()).getFullPathName().equals(pendingFile.getFullPathName())) {
            src = lpath.getPath();
            break;
          }
        }
      } catch (UnresolvedLinkException e) {
        throw new AssertionError("Lease files should reside on this FS");
      }
      if (src != null) {
        return src;
      }
    }
    throw new IOException("pendingFile (=" + pendingFile + ") not found."
            + "(lease=" + lease + ")");
  }

  /**
   * Renew the lease(s) held by the given client
   */
  void renewLease(String holder) throws PersistanceException {
    renewLease(getLease(holder));
  }
  void renewLease(Lease lease) throws PersistanceException {
    if (lease != null) {
      lease.setLastUpdate(now());
      EntityManager.update(lease);
    }
  }

  /**
   * Renew all of the currently open leases.
   */
  //HOP: FIXME: Add our implementaion
  void renewAllLeases() {
    //for (Lease l : leases.values()) {
    //  renewLease(l);
    //}
  }

  //HOP: method arguments changed for bug fix HDFS-4248
  void changeLease(String src, String dst) throws PersistanceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(getClass().getSimpleName() + ".changelease: " +
               " src=" + src + ", dest=" + dst);
    }

    final int len = src.length();
    for(Map.Entry<LeasePath, Lease> entry
        : findLeaseWithPrefixPath(src).entrySet()) {
      final LeasePath oldpath = entry.getKey();
      final Lease lease = entry.getValue();
      // replace stem of src with new destination
      final LeasePath newpath = new LeasePath(dst + oldpath.getPath().substring(len), lease.getHolderID());
      if (LOG.isDebugEnabled()) {
        LOG.debug("changeLease: replacing " + oldpath + " with " + newpath);
      }
      lease.replacePath(oldpath, newpath);
      EntityManager.remove(oldpath);
      EntityManager.add(newpath);
    }
    
  }

  void removeLeaseWithPrefixPath(String prefix) throws PersistanceException {
    for(Map.Entry<LeasePath, Lease> entry
        : findLeaseWithPrefixPath(prefix).entrySet()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(LeaseManager.class.getSimpleName()
            + ".removeLeaseWithPrefixPath: entry=" + entry);
      }
      removeLease(entry.getValue(), entry.getKey());    
    }
  }
  
  //HOP: bug fix changes HDFS-4242
  private Map<LeasePath, Lease> findLeaseWithPrefixPath(String prefix) throws PersistanceException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(LeaseManager.class.getSimpleName() + ".findLease: prefix=" + prefix);
    }

    Collection<LeasePath> leasePathSet = EntityManager.findList(LeasePath.Finder.ByPrefix, prefix);
    final Map<LeasePath, Lease> entries = new HashMap<LeasePath, Lease>();
    final int srclen = prefix.length();

    for (LeasePath lPath : leasePathSet) {
      if (!lPath.getPath().startsWith(prefix)) {
        LOG.warn("LeasePath fetched by prefix does not start with the prefix: \n"
                + "LeasePath: " + lPath + "\t Prefix: " + prefix);
        return entries;
      }
      if (lPath.getPath().length() == srclen || lPath.getPath().charAt(srclen) == Path.SEPARATOR_CHAR) {
        Lease lease = EntityManager.find(Lease.Finder.ByHolderId, lPath.getHolderId());
        entries.put(lPath, lease);
      }
    }
    return entries;
  }

  public void setLeasePeriod(long softLimit, long hardLimit) {
    this.softLimit = softLimit;
    this.hardLimit = hardLimit; 
  }
  
  /******************************************************
   * Monitor checks for leases that have expired,
   * and disposes of them.
   ******************************************************/
  //HOP: FIXME: needSync logic added for bug fix HDFS-4186
  class Monitor implements Runnable {
    final String name = getClass().getSimpleName();

    /** Check leases periodically. */
    @Override
    public void run() {
      for(; shouldRunMonitor && fsnamesystem.isRunning(); ) {
        //boolean needSync = false;
        try {
          fsnamesystem.writeLockInterruptibly();
          try {
            //HOP:
            /*if (!fsnamesystem.isInSafeMode()) {
              needSync = checkLeases();
            }*/
            if (!(Boolean) isInSafeModeHandler.handle()) {
               SortedSet<Lease> sortedLeases = (SortedSet<Lease>) findExpiredLeaseHandler.handle();
               if (sortedLeases != null) {
                 for (Lease expiredLease : sortedLeases) {
                   expiredLeaseHandler.setParams(expiredLease.getHolder()).handle();
                 }
               }
             }
          } catch (IOException ex) {
            LOG.error(ex);
          } finally {
            fsnamesystem.writeUnlock();
            //HOP:
            // lease reassignments should to be sync'ed.
            //if (needSync) {
            //  fsnamesystem.getEditLog().logSync();
            //}
          }
  
          Thread.sleep(HdfsServerConstants.NAMENODE_LEASE_RECHECK_INTERVAL);
        } catch(InterruptedException ie) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(name + " is interrupted", ie);
          }
        }
      }
    }
    
    TransactionalRequestHandler isInSafeModeHandler = new TransactionalRequestHandler(OperationType.PREPARE_LEASE_MANAGER_MONITOR) {
      
      @Override
      public Object performTask() throws PersistanceException, IOException {
        return fsnamesystem.isInSafeMode();
      }

      @Override
      public void acquireLock() throws PersistanceException, IOException {
        // TODO safemode 
      }
    };
    
    LightWeightRequestHandler findExpiredLeaseHandler = new LightWeightRequestHandler(OperationType.PREPARE_LEASE_MANAGER_MONITOR) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        long expiredTime = now() - hardLimit;
        LeaseDataAccess da = (LeaseDataAccess) StorageFactory.getDataAccess(LeaseDataAccess.class);
        return da.findByTimeLimit(expiredTime);
      }
    };
    
    TransactionalRequestHandler expiredLeaseHandler = new TransactionalRequestHandler(OperationType.LEASE_MANAGER_MONITOR) {

      @Override
      public Object performTask() throws PersistanceException, IOException {
        String holder = (String) getParams()[0];
        if (holder != null) {
          checkLeases(holder);
        }
        return null;
      }

      private SortedSet<String> leasePaths = null;
      @Override
      public void acquireLock() throws PersistanceException, IOException {
            String holder = (String) getParams()[0];
            TransactionLockManager tlm = new TransactionLockManager();
//FIXME            tlm.addINode(TransactionLockManager.INodeLockType.WRITE);
//FIXME            tlm.addBlock(TransactionLockManager.LockType.WRITE);
            tlm.addLease(TransactionLockManager.LockType.WRITE, holder);
            tlm.addNameNodeLease(LockType.WRITE);
            tlm.addLeasePath(TransactionLockManager.LockType.WRITE);
//FIXME            tlm.addReplica(TransactionLockManager.LockType.READ);
//FIXME            tlm.addCorrupt(TransactionLockManager.LockType.READ);
//FIXME            tlm.addExcess(TransactionLockManager.LockType.READ);
//FIXME            tlm.addReplicaUc(TransactionLockManager.LockType.READ);
//FIXME            tlm.addUnderReplicatedBlock(LockType.READ);
//FIXME            tlm.addGenerationStamp(LockType.WRITE);
            tlm.acquireByLease(leasePaths);
        }
      
      @Override
      public void setUp() throws StorageException
      {
        String holder = (String) getParams()[0];
        leasePaths = INodeUtil.findPathsByLeaseHolder(holder);
      }
    };
  }

//HOP: this method won't be needed
//  /**
//   * Get the list of inodes corresponding to valid leases.
//   * @return list of inodes
//   * @throws UnresolvedLinkException
//   */
//  //HOP: changed the implementation to work with entity manager
//  Map<String, INodeFileUnderConstruction> getINodesUnderConstruction() throws PersistanceException {
//    Map<String, INodeFileUnderConstruction> inodes =
//        new TreeMap<String, INodeFileUnderConstruction>();
//    Collection<LeasePath> leasesByPath = EntityManager.findList(LeasePath.Finder.All);
//    SortedSet<String> sortedLeasesByPath = new TreeSet<String>();
//    for(LeasePath p : leasesByPath)
//        sortedLeasesByPath.add(p.getPath());
//    for (String p : sortedLeasesByPath) {
//      // verify that path exists in namespace
//      try {
//        INode node = fsnamesystem.dir.getINode(p);
//        inodes.put(p, INodeFileUnderConstruction.valueOf(node, p));
//      } catch (IOException ioe) {
//        LOG.error(ioe);
//      }
//    }
//    return inodes;
//  }
  
  /** Check the leases beginning from the oldest.
   *  @return true is sync is needed.
   */
  private boolean checkLeases(String holder) throws PersistanceException {
    boolean needSync = false;
    assert fsnamesystem.hasWriteLock();
    
    Lease oldest = EntityManager.find(Lease.Finder.ByPKey, holder);

    if (oldest == null) {
      return needSync;
    }

    if (!expiredHardLimit(oldest)) {
      return needSync;
    }

    LOG.info("Lease " + oldest + " has expired hard limit");

    final List<LeasePath> removing = new ArrayList<LeasePath>();
    // need to create a copy of the oldest lease paths, becuase 
    // internalReleaseLease() removes paths corresponding to empty files,
    // i.e. it needs to modify the collection being iterated over
    // causing ConcurrentModificationException
    Collection<LeasePath> paths = oldest.getPaths();
    assert paths != null : "The lease " + oldest.toString() + " has no path.";
    LeasePath[] leasePaths = new LeasePath[paths.size()];
    paths.toArray(leasePaths);
    for (LeasePath lPath : leasePaths) {
      try {
        boolean leaseReleased = false;
        leaseReleased = fsnamesystem.internalReleaseLease(oldest, lPath.getPath(),
                HdfsServerConstants.NAMENODE_LEASE_HOLDER);
        if (leaseReleased) {
          LOG.info("Lease recovery for file " + lPath
                  + " is complete. File closed.");
          removing.add(lPath);
        } else {
          LOG.info("Started block recovery for file " + lPath
                  + " lease " + oldest);
        }
        
        // If a lease recovery happened, we need to sync later.
        if (!needSync && !leaseReleased) {
            needSync = true;
        }
            
      } catch (IOException e) {
        LOG.error("Cannot release the path " + lPath + " in the lease " + oldest, e);
        removing.add(lPath);
      }
    }

    for (LeasePath lPath : removing) {
      if (oldest.getPaths().contains(lPath)) {
        removeLease(oldest, lPath);
      }
    }
    
    return needSync;
  }

  void startMonitor() {
    Preconditions.checkState(lmthread == null,
        "Lease Monitor already running");
    shouldRunMonitor = true;
    lmthread = new Daemon(new Monitor());
    lmthread.start();
  }
  
  void stopMonitor() {
    if (lmthread != null) {
      shouldRunMonitor = false;
      try {
        lmthread.interrupt();
        lmthread.join(3000);
      } catch (InterruptedException ie) {
        LOG.warn("Encountered exception ", ie);
      }
      lmthread = null;
    }
  }

  /**
   * Trigger the currently-running Lease monitor to re-check
   * its leases immediately. This is for use by unit tests.
   */
  @VisibleForTesting
  void triggerMonitorCheckNow() {
    Preconditions.checkState(lmthread != null,
        "Lease monitor is not running");
    lmthread.interrupt();
  }
  
  private boolean expiredHardLimit(Lease lease) {
    return now() - lease.getLastUpdated() > hardLimit;
  }

  public boolean expiredSoftLimit(Lease lease) {
    return now() - lease.getLastUpdated() > softLimit;
  }
}
