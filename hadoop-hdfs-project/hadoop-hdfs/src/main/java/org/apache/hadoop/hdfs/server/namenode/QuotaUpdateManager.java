package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.Daemon;
import se.sics.hop.common.HopQuotaUpdateIdGen;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.INodeIdentifier;
import se.sics.hop.metadata.StorageFactory;
import se.sics.hop.metadata.hdfs.dal.QuotaUpdateDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.QuotaUpdate;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.transaction.handler.HopsTransactionalRequestHandler;
import se.sics.hop.transaction.handler.LightWeightRequestHandler;
import se.sics.hop.transaction.lock.HopsLockFactory;
import se.sics.hop.transaction.lock.SubtreeLockHelper;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.hadoop.util.ExitUtil.terminate;

public class QuotaUpdateManager {

  static final Log LOG = LogFactory.getLog(QuotaUpdateManager.class);

  private final FSNamesystem namesystem;

  private final int updateInterval;
  private final int updateLimit;

  private final Daemon updateThread = new Daemon(new QuotaUpdateMonitor());

  private final ConcurrentLinkedQueue<Iterator<Integer>> prioritizedUpdates =
      new ConcurrentLinkedQueue<Iterator<Integer>>();

  public QuotaUpdateManager(FSNamesystem namesystem, Configuration conf) {
    this.namesystem = namesystem;
    updateInterval =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_INTERVAL_KEY,
                    DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_INTERVAL_DEFAULT);
    updateLimit =
        conf.getInt(DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_LIMIT_KEY,
                    DFSConfigKeys.DFS_NAMENODE_QUOTA_UPDATE_LIMIT_DEFAULT);
  }

  public void activate() {
    updateThread.start();
  }

  public void close() {
    if (updateThread != null) {
      updateThread.interrupt();
      try {
        updateThread.join(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private int nextId() {
    return HopQuotaUpdateIdGen.getUniqueQuotaUpdateId();
  }

  public void addUpdate(final int inodeId, final long namespaceDelta, final long diskspaceDelta)
      throws StorageException, TransactionContextException {

    QuotaUpdate update = new QuotaUpdate(nextId(), inodeId, namespaceDelta, diskspaceDelta);
    EntityManager.add(update);
  }

  private class QuotaUpdateMonitor implements Runnable {
    @Override
    public void run() {
      long startTime;
      while (namesystem.isRunning()) {
        startTime = System.currentTimeMillis();
        try {
          if(namesystem.isLeader()){
            if (!prioritizedUpdates.isEmpty()) {
              Iterator<Integer> iterator = prioritizedUpdates.poll();
              while (iterator.hasNext()){
                processUpdates(iterator.next());
              }
              synchronized (iterator) {
                iterator.notify();
              }
            }
            processNextUpdateBatch();
          }
          long sleepDuration = updateInterval - (System.currentTimeMillis() - startTime);
          if (sleepDuration > 0) {
            Thread.sleep(updateInterval);
          }
        } catch (InterruptedException ie) {
          LOG.warn("QuotaUpdateMonitor thread received InterruptedException.", ie);
          break;
        } catch (StorageException e){
          LOG.warn("QuotaUpdateMonitor thread received StorageException.", e);
          break;
        }
        catch (Throwable t) {
          LOG.fatal("QuotaUpdateMonitor thread received Runtime exception. ", t);
          terminate(1, t);
        }
      }
    }
  }

  private final Comparator<QuotaUpdate> quotaUpdateComparator = new Comparator<QuotaUpdate>() {
    @Override
    public int compare(QuotaUpdate quotaUpdate, QuotaUpdate quotaUpdate2) {
      if (quotaUpdate.getInodeId() < quotaUpdate2.getInodeId()) {
        return -1;
      }
      if (quotaUpdate.getInodeId() > quotaUpdate2.getInodeId()) {
        return 1;
      }
      return 0;
    }
  };

  private void processUpdates(final Integer id) throws IOException {
    LightWeightRequestHandler findHandler = new LightWeightRequestHandler(HDFSOperationType.GET_UPDATES_FOR_ID) {
      @Override
      public Object performTask() throws IOException {
        QuotaUpdateDataAccess<QuotaUpdate> dataAccess = (QuotaUpdateDataAccess)
            StorageFactory.getDataAccess(QuotaUpdateDataAccess.class);
        return dataAccess.findByInodeId(id);
      }
    };

    List<QuotaUpdate> quotaUpdates = (List<QuotaUpdate>) findHandler.handle();
    applyBatchedUpdate(quotaUpdates);
  }

  private void processNextUpdateBatch() throws IOException {
    LightWeightRequestHandler findHandler = new LightWeightRequestHandler(HDFSOperationType.GET_NEXT_QUOTA_BATCH) {
      @Override
      public Object performTask() throws IOException {
        QuotaUpdateDataAccess<QuotaUpdate> dataAccess = (QuotaUpdateDataAccess)
            StorageFactory.getDataAccess(QuotaUpdateDataAccess.class);
        return dataAccess.findLimited(updateLimit);
      }
    };

    List<QuotaUpdate> quotaUpdates = (List<QuotaUpdate>) findHandler.handle();
    Collections.sort(quotaUpdates, quotaUpdateComparator);

    ArrayList<QuotaUpdate> batch = new ArrayList<QuotaUpdate>();
    for (QuotaUpdate update : quotaUpdates) {
      if (batch.size() == 0 || batch.get(0).getInodeId() == update.getInodeId()) {
        batch.add(update);
      } else {
        applyBatchedUpdate(batch);
        batch = new ArrayList<QuotaUpdate>();
        batch.add(update);
      }
    }

    if (batch.size() != 0) {
      applyBatchedUpdate(batch);
    }
  }

  private void applyBatchedUpdate(final List<QuotaUpdate> updates) throws IOException {
    if (updates.size() == 0) {
      return;
    }
    new HopsTransactionalRequestHandler(HDFSOperationType.APPLY_QUOTA_UPDATE) {
      INodeIdentifier iNodeIdentifier;
      @Override
      public void setUp() throws IOException {
        super.setUp();
        iNodeIdentifier = new INodeIdentifier(updates.get(0).getInodeId());
      }

      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        HopsLockFactory lf = HopsLockFactory.getInstance();
        locks.add(lf.getIndividualINodeLock(TransactionLockTypes.INodeLockType.WRITE, iNodeIdentifier));
      }

      @Override
      public Object performTask() throws IOException {
        INodeDirectory dir = (INodeDirectory) EntityManager.find(INode.Finder.ByINodeID, updates.get(0).getInodeId());
        if (dir != null && SubtreeLockHelper.isSubtreeLocked(
            dir.isSubtreeLocked(),
            dir.getSubtreeLockOwner(),
            namesystem.getNameNode().getActiveNamenodes().getActiveNodes())) {
          /*
           * We cannot process updates to keep move operations consistent. Otherwise the calculated size of the subtree
           * could differ from the view of the parent if outstanding quota updates are applied after being considered
           * by the QuotaCountingFileTree but before successfully moving the subtree.
           */
          return null;
        }

        long namespaceDelta = 0;
        long diskspaceDelta = 0;
        for (QuotaUpdate update : updates) {
          namespaceDelta += update.getNamespaceDelta();
          diskspaceDelta += update.getDiskspaceDelta();
          LOG.debug("handling " + update);
          EntityManager.remove(update);
        }

        if (dir == null) {
          LOG.debug("dropping update for " + updates.get(0) + " ns " + namespaceDelta + " ds " + diskspaceDelta
              + " because of deletion");
          return null;
        }
        if (namespaceDelta == 0 && diskspaceDelta == 0) {
          return null;
        }

        if (dir != null && dir.isQuotaSet()) {
          INodeDirectoryWithQuota quotaDir = (INodeDirectoryWithQuota) dir;
          INodeAttributes attributes = quotaDir.getINodeAttributes();
          attributes.setNsCount(attributes.getNsCount() + namespaceDelta);
          attributes.setDiskspace(attributes.getDiskspace() + diskspaceDelta);
          LOG.debug("applying aggregated update for directory " + dir.getId() + " with namespace delta "
              + namespaceDelta + " and diskspace delta " + diskspaceDelta);
        }

        if (dir != null && dir.getId() != INodeDirectory.ROOT_ID) {
          QuotaUpdate parentUpdate = new QuotaUpdate(
              nextId(),
              dir.getParentId(),
              namespaceDelta,
              diskspaceDelta);
          EntityManager.add(parentUpdate);
          LOG.debug("adding parent update " + parentUpdate);
        }
        return null;
      }
    }.handle(this);
  }

  /**
   * Ids which updates need to be applied. Note that children must occur before their parents
   * in order to guarantee that updates are applied completely.
   *
   * @param iterator Ids to be updates sorted from the leaves to the root of the subtree
   */
  void addPrioritizedUpdates(Iterator<Integer> iterator) {
    prioritizedUpdates.add(iterator);
  }
}
