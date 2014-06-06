package se.sics.hop.erasure_coding;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.StringUtils;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.metadata.StorageFactory;
import se.sics.hop.metadata.hdfs.dal.EncodingStatusDataAccess;
import se.sics.hop.metadata.lock.ErasureCodingTransactionLockAcquirer;
import se.sics.hop.metadata.lock.HDFSTransactionLockAcquirer;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.transaction.handler.EncodingStatusOperationType;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.transaction.handler.HDFSTransactionalRequestHandler;
import se.sics.hop.transaction.handler.LightWeightRequestHandler;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.hadoop.util.ExitUtil.terminate;

public class ErasureCodingManager extends Configured{

  static final Log LOG = LogFactory.getLog(ErasureCodingManager.class);

  public static final String ERASURE_CODING_ENABLED_KEY = "se.sics.hop.erasure_coding.enabled";
  public static final String PARITY_FOLDER = "se.sics.hop.erasure_coding.parity_folder";
  public static final String DEFAULT_PARITY_FOLDER = "/parity";
  public static final String ENCODING_MANAGER_CLASSNAME_KEY = "se.sics.hop.erasure_coding.encoding_manager";
  public static final String BLOCK_REPAIR_MANAGER_CLASSNAME_KEY = "se.sics.hop.erasure_coding.block_rapair_manager";
  public static final String RECHECK_INTERVAL_KEY = "se.sics.hop.erasure_coding.recheck_interval";
  public static final int DEFAULT_RECHECK_INTERVAL = 5 * 60 * 1000;
  public static final String ACTIVE_ENCODING_LIMIT_KEY = "se.sics.hop.erasure_coding.active_encoding_limit";
  public static final int DEFAULT_ACTIVE_ENCODING_LIMIT = 10;
  public static final String ACTIVE_REPAIR_LIMIT_KEY = "se.sics.hop.erasure_coding.active_repair_limit";
  public static final int DEFAULT_ACTIVE_REPAIR_LIMIT = 10;
  public static final String REPAIR_DELAY_KEY = "se.sics.hop.erasure_coding_repair_delay";
  public static final int DEFAULT_REPAIR_DELAY_KEY = 30 * 60 * 1000;
  public static final String ACTIVE_PARITY_REPAIR_LIMIT_KEY = "se.sics.hop.erasure_coding.active_parity_repair_limit";
  public static final int DEFAULT_ACTIVE_PARITY_REPAIR_LIMIT = 10;
  public static final String PARITY_REPAIR_DELAY_KEY = "se.sics.hop.erasure_coding_parity_repair_delay";
  public static final int DEFAULT_PARITY_REPAIR_DELAY_KEY = 30 * 60 * 1000;

  private final FSNamesystem namesystem;
  private final Daemon erasureCodingMonitorThread = new Daemon(new ErasureCodingMonitor());
  private EncodingManager encodingManager;
  private BlockRepairManager blockRepairManager;
  private String parityFolder;
  private final long recheckInterval;
  private final int activeEncodingLimit;
  private int activeEncodings = 0;
  private final int activeRepairLimit;
  private final int activeParityRepairLimit;
  private int activeRepairs = 0;
  private int activeParityRepairs = 0;
  private final int repairDelay;
  private final int parityRepairDelay;

  public ErasureCodingManager(FSNamesystem namesystem, Configuration conf) {
    super(conf);
    this.namesystem = namesystem;
    this.parityFolder = conf.get(PARITY_FOLDER, DEFAULT_PARITY_FOLDER);
    this.recheckInterval = conf.getInt(RECHECK_INTERVAL_KEY, DEFAULT_RECHECK_INTERVAL);
    this.activeEncodingLimit = conf.getInt(ACTIVE_ENCODING_LIMIT_KEY, DEFAULT_ACTIVE_ENCODING_LIMIT);
    this.activeRepairLimit = conf.getInt(ACTIVE_REPAIR_LIMIT_KEY, DEFAULT_ACTIVE_REPAIR_LIMIT);
    this.activeParityRepairLimit = conf.getInt(ACTIVE_PARITY_REPAIR_LIMIT_KEY, DEFAULT_ACTIVE_PARITY_REPAIR_LIMIT);
    this.repairDelay = conf.getInt(REPAIR_DELAY_KEY, DEFAULT_REPAIR_DELAY_KEY);
    this.parityRepairDelay = conf.getInt(PARITY_REPAIR_DELAY_KEY, DEFAULT_PARITY_REPAIR_DELAY_KEY);
  }

  private boolean loadRaidNodeClasses() {
    try {
      Class<?> encodingManagerClass = getConf().getClass(ENCODING_MANAGER_CLASSNAME_KEY, null);
      if (encodingManagerClass == null || !EncodingManager.class.isAssignableFrom(encodingManagerClass)) {
        throw new ClassNotFoundException(encodingManagerClass + " is not an implementation of " + EncodingManager.class.getCanonicalName());
      }
      Constructor<?> encodingManagerConstructor = encodingManagerClass.getConstructor(
          new Class[] {Configuration.class} );
      encodingManager = (EncodingManager) encodingManagerConstructor.newInstance(getConf());

      Class<?> blockRepairManagerClass = getConf().getClass(BLOCK_REPAIR_MANAGER_CLASSNAME_KEY, null);
      if (blockRepairManagerClass == null || !BlockRepairManager.class.isAssignableFrom(blockRepairManagerClass)) {
        throw new ClassNotFoundException(blockRepairManagerClass + " is not an implementation of " + BlockRepairManager.class.getCanonicalName());
      }
      Constructor<?> blockRepairManagerConstructor = blockRepairManagerClass.getConstructor(
          new Class[] {Configuration.class} );
      blockRepairManager = (BlockRepairManager) blockRepairManagerConstructor.newInstance(getConf());
    } catch (Exception e) {
      LOG.error("Could not load erasure coding classes", e);
      return false;
    }

    return true;
  }

  public void activate() {
    if (!loadRaidNodeClasses()) {
      LOG.error("ErasureCodingMonitor not started. An error occurred during the loading of the encoding library.");
      return;
    }

    erasureCodingMonitorThread.start();
    LOG.info("ErasureCodingMonitor started");
  }

  public void close() {
    try {
      if (erasureCodingMonitorThread != null) {
        erasureCodingMonitorThread.interrupt();
        erasureCodingMonitorThread.join(3000);
      }
    } catch (InterruptedException ie) {
    }
    LOG.info("ErasureCodingMonitor stopped");
  }

  public static boolean isErasureCodingEnabled(Configuration conf) {
    return conf.getBoolean(ERASURE_CODING_ENABLED_KEY, false);
  }

  private class ErasureCodingMonitor implements Runnable {

    @Override
    public void run() {
      while (namesystem.isRunning()) {
        try {
          try {
            if (namesystem.isInSafeMode()) {
              continue;
            }
          } catch (IOException e) {
            LOG.info("In safe mode skipping this round");
          }
          if(namesystem.isLeader()){
            checkPotentiallyFixedFiles();
            checkPotentiallyFixedParityFiles();
            checkActiveEncodings();
            scheduleEncodings();
            checkActiveRepairs();
            scheduleSourceRepairs();
            scheduleParityRepairs();
          }
          try {
            Thread.sleep(recheckInterval);
          } catch (InterruptedException ie) {
            LOG.warn("ErasureCodingMonitor thread received InterruptedException.", ie);
            break;
          }
        } catch (Throwable e) {
          LOG.error("DEBUG_DEATH caused by", e);
          // TODO STEFFEN - This is for debbugin purposes only!
          System.exit(-1);
        }
      }
    }
  }

  private void checkPotentiallyFixedFiles() {
    LightWeightRequestHandler findHandler = new LightWeightRequestHandler(
        EncodingStatusOperationType.FIND_POTENTIALLY_FIXED) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        EncodingStatusDataAccess<EncodingStatus> dataAccess = (EncodingStatusDataAccess)
            StorageFactory.getDataAccess(EncodingStatusDataAccess.class);
        return dataAccess.findPotentiallyFixed(Long.MAX_VALUE);
      }
    };

    try {
      final Collection<EncodingStatus> potentiallyFixed = (Collection<EncodingStatus>) findHandler.handle();
      for (final EncodingStatus encodingStatus : potentiallyFixed) {
        LOG.info("Checking if source file was fixed for id " + encodingStatus.getInodeId());
        final String path = namesystem.getPath(encodingStatus.getInodeId());
        if (path == null) {
          continue;
        }

        HDFSTransactionalRequestHandler handler =
            new HDFSTransactionalRequestHandler(HDFSOperationType.GET_INODE) {
              @Override
              public TransactionLocks acquireLock() throws PersistanceException, IOException {
                ErasureCodingTransactionLockAcquirer tla = new ErasureCodingTransactionLockAcquirer();
                tla.getLocks().
                    addINode(TransactionLockTypes.INodeResolveType.PATH,
                        TransactionLockTypes.INodeLockType.WRITE, new String[]{path}).
                    addBlock().
                    addReplica().
                    addExcess().
                    addCorrupt().
                    addReplicaUc();
                tla.getLocks().addEncodingStatusLock(encodingStatus.getInodeId());
                return tla.acquire();
              }

              @Override
              public Object performTask() throws PersistanceException, IOException {
                if (namesystem.isFileCorrupt(path) == false) {
                  EncodingStatus status = EntityManager.find(EncodingStatus.Finder.ByInodeId,
                      encodingStatus.getInodeId());
                  status.setStatus(EncodingStatus.Status.ENCODED);
                  status.setStatusModificationTime(System.currentTimeMillis());
                  EntityManager.update(status);
                } else {
                  EncodingStatus status = EntityManager.find(EncodingStatus.Finder.ByInodeId,
                      encodingStatus.getInodeId());
                  status.setStatus(EncodingStatus.Status.REPAIR_REQUESTED);
                  status.setStatusModificationTime(System.currentTimeMillis());
                  EntityManager.update(status);
                }
                return null;
              }
            };
        handler.handle(this);
      }
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  private void checkPotentiallyFixedParityFiles() {
    LightWeightRequestHandler findHandler = new LightWeightRequestHandler(
        EncodingStatusOperationType.FIND_POTENTIALLY_FIXED_PARITIES) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        EncodingStatusDataAccess<EncodingStatus> dataAccess = (EncodingStatusDataAccess)
            StorageFactory.getDataAccess(EncodingStatusDataAccess.class);
        return dataAccess.findPotentiallyFixedParities(Long.MAX_VALUE);
      }
    };

    try {
      final Collection<EncodingStatus> potentiallyFixed = (Collection<EncodingStatus>) findHandler.handle();
      for (final EncodingStatus encodingStatus : potentiallyFixed) {
        LOG.info("Checking if parity file was fixed for id " + encodingStatus.getParityInodeId());
        final String path = parityFolder + "/" + encodingStatus.getParityFileName();

        HDFSTransactionalRequestHandler handler =
            new HDFSTransactionalRequestHandler(HDFSOperationType.GET_INODE) {
              @Override
              public TransactionLocks acquireLock() throws PersistanceException, IOException {
                ErasureCodingTransactionLockAcquirer tla = new ErasureCodingTransactionLockAcquirer();
                tla.getLocks().
                    addINode(TransactionLockTypes.INodeResolveType.PATH,
                        TransactionLockTypes.INodeLockType.WRITE, new String[]{path}).
                    addBlock().
                    addReplica().
                    addExcess().
                    addCorrupt().
                    addReplicaUc();
                tla.getLocks().addEncodingStatusLock(encodingStatus.getParityInodeId());
                return tla.acquire();
              }

              @Override
              public Object performTask() throws PersistanceException, IOException {
                if (namesystem.isFileCorrupt(path) == false) {
                  EncodingStatus status = EntityManager.find(EncodingStatus.Finder.ByParityInodeId,
                      encodingStatus.getParityInodeId());
                  status.setParityStatus(EncodingStatus.ParityStatus.HEALTHY);
                  status.setParityStatusModificationTime(System.currentTimeMillis());
                  EntityManager.update(status);
                } else {
                  EncodingStatus status = EntityManager.find(EncodingStatus.Finder.ByParityInodeId,
                      encodingStatus.getParityInodeId());
                  status.setParityStatus(EncodingStatus.ParityStatus.REPAIR_REQUESTED);
                  status.setParityStatusModificationTime(System.currentTimeMillis());
                  EntityManager.update(status);
                }
                return null;
              }
            };
        handler.handle(this);
      }
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  private void checkActiveEncodings() {
    List<Report> reports = encodingManager.computeReports();
    for (Report report : reports) {
      switch (report.getStatus()) {
        case ACTIVE:
          break;
        case FINISHED:
          LOG.info("Encoding finished for " + report.getFilePath());
          finalizeEncoding(report.getFilePath());
          activeEncodings--;
          break;
        case FAILED:
          LOG.info("Encoding failed for " + report.getFilePath());
          // This would be a problem with multiple name nodes as it is not atomic. Only one thread here.
          updateEncodingStatus(report.getFilePath(), EncodingStatus.Status.ENCODING_FAILED);
          updateEncodingStatus(report.getFilePath(), EncodingStatus.ParityStatus.REPAIR_FAILED);
          activeEncodings--;
          break;
        case CANCELED:
          LOG.info("Encoding canceled for " + report.getFilePath());
          updateEncodingStatus(report.getFilePath(), EncodingStatus.Status.ENCODING_CANCELED);
          activeEncodings--;
          break;
      }
    }
  }

  private void finalizeEncoding(final String path) {
    try {
      final EncodingStatus status = namesystem.getEncodingStatus(path);
      final int parityInodeId = namesystem.findInodeId(parityFolder + "/" + status.getParityFileName());

      HDFSTransactionalRequestHandler handler =
          new HDFSTransactionalRequestHandler(HDFSOperationType.GET_INODE) {
            @Override
            public TransactionLocks acquireLock() throws PersistanceException, IOException {
              ErasureCodingTransactionLockAcquirer tla = new ErasureCodingTransactionLockAcquirer();
              tla.getLocks().
                  addEncodingStatusLock(status.getInodeId()).
                  addINode(TransactionLockTypes.INodeResolveType.PATH,
                      TransactionLockTypes.INodeLockType.WRITE, new String[]{path});
              return tla.acquire();
            }

            @Override
            public Object performTask() throws PersistanceException, IOException {
              // Should be necessary for atomicity
              EncodingStatus encodingStatus = EntityManager.find(EncodingStatus.Finder.ByInodeId, status.getInodeId());
              encodingStatus.setStatus(EncodingStatus.Status.ENCODED);
              encodingStatus.setStatusModificationTime(System.currentTimeMillis());
              encodingStatus.setParityInodeId(parityInodeId);
              encodingStatus.setParityStatus(EncodingStatus.ParityStatus.HEALTHY);
              encodingStatus.setParityStatusModificationTime(System.currentTimeMillis());
              EntityManager.update(encodingStatus);
              return null;
            }
          };
      handler.handle(this);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  private void updateEncodingStatus(String filePath, EncodingStatus.Status status) {
    try {
      int id = namesystem.findInodeId(filePath);
      namesystem.updateEncodingStatus(filePath, id, status);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  private void updateEncodingStatus(String filePath, EncodingStatus.ParityStatus status) {
    try {
      int id = namesystem.findInodeId(filePath);
      namesystem.updateEncodingStatus(filePath, id, status);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  private void scheduleEncodings() {
    final int limit = activeEncodingLimit - activeEncodings;
    if (limit <= 0) {
      return;
    }

    LightWeightRequestHandler findHandler = new LightWeightRequestHandler(
        EncodingStatusOperationType.FIND_REQUESTED_ENCODINGS) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        EncodingStatusDataAccess<EncodingStatus> dataAccess = (EncodingStatusDataAccess)
            StorageFactory.getDataAccess(EncodingStatusDataAccess.class);
        return dataAccess.findRequestedEncodings(limit);
      }
    };

    try {
      Collection<EncodingStatus> requestedEncodings = (Collection<EncodingStatus>) findHandler.handle();

      for (EncodingStatus encodingStatus : requestedEncodings) {
        INode iNode = namesystem.findInode(encodingStatus.getInodeId());
        if (iNode == null) {
          LOG.error("findInode returned null for id " + encodingStatus.getInodeId());
          continue;
        }
        if (iNode.isUnderConstruction()) {
          // It might still be written to the file
          LOG.info("Still under construction. Encoding not scheduled for " + iNode.getId());
          continue;
        }

        String path = namesystem.getPath(iNode);
        if (iNode == null) {
          continue;
        }

        LOG.info("Schedule encoding for " + path);
        UUID parityFileName = UUID.randomUUID();
        encodingManager.encodeFile(encodingStatus.getEncodingPolicy(), new Path(path),
            new Path(parityFolder + "/" + parityFileName.toString()));
        namesystem.updateEncodingStatus(path, encodingStatus.getInodeId(), EncodingStatus.Status.ENCODING_ACTIVE,
            parityFileName.toString());
        activeEncodings++;
      }
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  private void checkActiveRepairs() {
    List<Report> reports = blockRepairManager.computeReports();
    for (Report report : reports) {
      switch (report.getStatus()) {
        case ACTIVE:
          break;
        case FINISHED:
          // TODO STEFFEN - I think we actually need to check if the file was repaired. A block might have been lost during repair.
          if (isParityFile(report.getFilePath())) {
            updateEncodingStatus(report.getFilePath(), EncodingStatus.ParityStatus.HEALTHY);
            activeParityRepairs--;
          } else {
            updateEncodingStatus(report.getFilePath(), EncodingStatus.Status.ENCODED);
            activeRepairs--;
          }
          break;
        case FAILED:
          if (isParityFile(report.getFilePath())) {
            updateEncodingStatus(report.getFilePath(), EncodingStatus.ParityStatus.REPAIR_FAILED);
            activeParityRepairs--;
          } else {
            updateEncodingStatus(report.getFilePath(), EncodingStatus.Status.REPAIR_FAILED);
            activeRepairs--;
          }
          break;
        case CANCELED:
          if (isParityFile(report.getFilePath())) {
            updateEncodingStatus(report.getFilePath(), EncodingStatus.ParityStatus.REPAIR_CANCELED);
            activeParityRepairs--;
          } else {
            updateEncodingStatus(report.getFilePath(), EncodingStatus.Status.REPAIR_CANCELED);
            activeRepairs--;
          }
          break;
      }
    }
  }

  private void scheduleSourceRepairs() {
    final int limit = activeRepairLimit - activeRepairs;
    if (limit <= 0) {
      return;
    }

    LightWeightRequestHandler findHandler = new LightWeightRequestHandler(
        EncodingStatusOperationType.FIND_REQUESTED_REPAIRS) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        EncodingStatusDataAccess<EncodingStatus> dataAccess = (EncodingStatusDataAccess)
            StorageFactory.getDataAccess(EncodingStatusDataAccess.class);
        return dataAccess.findRequestedRepairs(limit);
      }
    };

    try {
      Collection<EncodingStatus> requestedRepairs = (Collection<EncodingStatus>) findHandler.handle();
      for (EncodingStatus encodingStatus : requestedRepairs) {
        if (System.currentTimeMillis() - encodingStatus.getStatusModificationTime() < repairDelay) {
          continue;
        }

        if (encodingStatus.isParityRepairActive()) {
          continue;
        }

        INode iNode = namesystem.findInode(encodingStatus.getInodeId());
        String path = namesystem.getPath(iNode);
        if (iNode == null) {
          continue;
        }
        blockRepairManager.repairSourceBlocks(encodingStatus.getEncodingPolicy().getCodec(), new Path(path),
            new Path(parityFolder + "/" + encodingStatus.getParityFileName()));
        namesystem.updateEncodingStatus(path, iNode.getId(), EncodingStatus.Status.REPAIR_ACTIVE);
        activeRepairs++;
      }
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  private void scheduleParityRepairs() {
    final int limit = activeParityRepairLimit - activeParityRepairs;
    if (limit <= 0) {
      return;
    }

    LightWeightRequestHandler findHandler = new LightWeightRequestHandler(
        EncodingStatusOperationType.FIND_REQUESTED_PARITY_REPAIRS) {
      @Override
      public Object performTask() throws PersistanceException, IOException {
        EncodingStatusDataAccess<EncodingStatus> dataAccess = (EncodingStatusDataAccess)
            StorageFactory.getDataAccess(EncodingStatusDataAccess.class);
        return dataAccess.findRequestedParityRepairs(limit);
      }
    };

    try {
      Collection<EncodingStatus> requestedRepairs = (Collection<EncodingStatus>) findHandler.handle();
      for (EncodingStatus encodingStatus : requestedRepairs) {
        if (System.currentTimeMillis() - encodingStatus.getParityStatusModificationTime() < parityRepairDelay) {
          continue;
        }

        if (encodingStatus.getStatus().equals(EncodingStatus.Status.ENCODED) == false) {
          // Only repair parity for non-broken source files. Otherwise repair source file first.
          continue;
        }

        INode iNode = namesystem.findInode(encodingStatus.getInodeId());
        String path = namesystem.getPath(iNode);
        if (iNode == null) {
          continue;
        }
        blockRepairManager.repairParityBlocks(encodingStatus.getEncodingPolicy().getCodec(), new Path(path),
            new Path(parityFolder + "/" + encodingStatus.getParityFileName()));
        namesystem.updateEncodingStatus(path, iNode.getId(), EncodingStatus.ParityStatus.REPAIR_ACTIVE);
        activeRepairs++;
      }
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
    }
  }

  public boolean isParityFile(String src) {
    Pattern pattern = Pattern.compile(parityFolder + ".*");
    Matcher matcher = pattern.matcher(src);
    if (matcher.matches()) {
      return true;
    }
    return false;
  }
}
