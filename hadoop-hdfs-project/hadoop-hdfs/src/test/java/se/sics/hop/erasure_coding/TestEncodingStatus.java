package se.sics.hop.erasure_coding;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import se.sics.hop.DALDriver;
import se.sics.hop.DALStorageFactory;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.StorageInitializtionException;
import se.sics.hop.metadata.StorageFactory;
import se.sics.hop.metadata.hdfs.dal.EncodingStatusDataAccess;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.transaction.handler.EncodingStatusOperationType;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.transaction.handler.HopsTransactionalRequestHandler;
import se.sics.hop.transaction.handler.LightWeightRequestHandler;
import se.sics.hop.transaction.lock.HopsLockFactory;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

public class TestEncodingStatus extends TestCase {

  static {
    try {
      final DALStorageFactory sf = DALDriver.load("se.sics.hop.metadata.ndb.NdbStorageFactory");
      Properties conf = new Properties();
      conf.load(ClassLoader.getSystemResourceAsStream("ndb-config.properties"));
      sf.setConfiguration(conf);
    } catch (StorageInitializtionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      StorageFactory.setConfiguration(new Configuration());
    } catch (StorageInitializtionException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testAddAndFindEncodingStatus() throws IOException {
    final EncodingPolicy policy = new EncodingPolicy("codec", (short) 1);
    final EncodingStatus statusToAdd = new EncodingStatus(1, EncodingStatus.Status.ENCODING_REQUESTED, policy, 1L);

    HopsTransactionalRequestHandler addReq = new HopsTransactionalRequestHandler(HDFSOperationType.ADD_ENCODING_STATUS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {

      }

      @Override
      public Object performTask() throws StorageException, IOException {
        EntityManager.add(statusToAdd);
        return null;
      }
    };
    addReq.handle();

    HopsTransactionalRequestHandler findReq = new HopsTransactionalRequestHandler(
        HDFSOperationType.FIND_ENCODING_STATUS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        HopsLockFactory lf = HopsLockFactory.getInstance();
        Integer id = (Integer) getParams()[0];
        locks.add(lf.getIndivdualEncodingStatusLock(
            TransactionLockTypes.LockType.READ_COMMITTED, id));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        Integer id = (Integer) getParams()[0];
        return EntityManager.find(EncodingStatus.Finder.ByInodeId, id);
      }
    };
    findReq.setParams(statusToAdd.getInodeId());
    EncodingStatus foundStatus = (EncodingStatus) findReq.handle();
    assertNotNull(foundStatus);
    assertEquals(statusToAdd.getInodeId(), foundStatus.getInodeId());
    assertEquals(statusToAdd.getStatus(), foundStatus.getStatus());
    assertEquals(statusToAdd.getEncodingPolicy(), foundStatus.getEncodingPolicy());
    assertEquals(statusToAdd.getStatusModificationTime(), foundStatus.getStatusModificationTime());

    // Cleanup
    HopsTransactionalRequestHandler delReq = new HopsTransactionalRequestHandler(HDFSOperationType.DELETE_ENCODING_STATUS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {

      }

      @Override
      public Object performTask() throws StorageException, IOException {
        EntityManager.remove(statusToAdd);
        return null;
      }
    };
    delReq.handle();

    findReq.setParams(statusToAdd.getInodeId());
    assertNull(findReq.handle());
  }

  @Test
  public void testUpdateEncodingStatus() throws IOException {
    final EncodingPolicy policy = new EncodingPolicy("codec", (short) 1);
    final EncodingStatus statusToAdd = new EncodingStatus(1, EncodingStatus.Status.ENCODING_REQUESTED, policy, 1L);

    HopsTransactionalRequestHandler addReq = new HopsTransactionalRequestHandler(HDFSOperationType.ADD_ENCODING_STATUS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {

      }

      @Override
      public Object performTask() throws StorageException, IOException {
        EntityManager.add(statusToAdd);
        return null;
      }
    };
    addReq.handle();

    final EncodingPolicy policy1 = new EncodingPolicy("codec2", (short) 2);
    final EncodingStatus updatedStatus = new EncodingStatus(1, EncodingStatus.Status.ENCODING_ACTIVE, policy1, 2L);

    HopsTransactionalRequestHandler updateReq = new HopsTransactionalRequestHandler(
        HDFSOperationType.UPDATE_ENCODING_STATUS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        HopsLockFactory lf = HopsLockFactory.getInstance();
        Integer id = (Integer) getParams()[0];
        locks.add(lf.getIndivdualEncodingStatusLock(
            TransactionLockTypes.LockType.WRITE, id));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        Integer id = (Integer) getParams()[0];
        EntityManager.update(updatedStatus);
        return null;
      }
    };
    updateReq.setParams(updatedStatus.getInodeId());
    updateReq.handle();

    HopsTransactionalRequestHandler findReq = new HopsTransactionalRequestHandler(
        HDFSOperationType.FIND_ENCODING_STATUS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {
        HopsLockFactory lf = HopsLockFactory.getInstance();
        Integer id = (Integer) getParams()[0];
        locks.add(lf.getIndivdualEncodingStatusLock(
            TransactionLockTypes.LockType.READ_COMMITTED, id));
      }

      @Override
      public Object performTask() throws StorageException, IOException {
        Integer id = (Integer) getParams()[0];
        return EntityManager.find(EncodingStatus.Finder.ByInodeId, id);
      }
    };
    findReq.setParams(statusToAdd.getInodeId());
    EncodingStatus foundStatus = (EncodingStatus) findReq.handle();
    assertNotNull(foundStatus);
    assertEquals(updatedStatus.getInodeId(), foundStatus.getInodeId());
    assertEquals(updatedStatus.getStatus(), foundStatus.getStatus());
    assertEquals(updatedStatus.getEncodingPolicy(), foundStatus.getEncodingPolicy());
    assertEquals(updatedStatus.getStatusModificationTime(), foundStatus.getStatusModificationTime());

    // Cleanup
    HopsTransactionalRequestHandler delReq = new HopsTransactionalRequestHandler(HDFSOperationType.DELETE_ENCODING_STATUS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {

      }

      @Override
      public Object performTask() throws StorageException, IOException {
        EntityManager.remove(statusToAdd);
        return null;
      }
    };
    delReq.handle();

    findReq.setParams(statusToAdd.getInodeId());
    assertNull(findReq.handle());
  }

  @Test
  public void testCountEncodingRequested() throws IOException {
    final EncodingPolicy policy = new EncodingPolicy("codec", (short) 1);
    final ArrayList<EncodingStatus> statusToAdd = new ArrayList<EncodingStatus>();
    statusToAdd.add(new EncodingStatus(1, EncodingStatus.Status.ENCODING_REQUESTED,policy, 1L));
    statusToAdd.add(new EncodingStatus(2, EncodingStatus.Status.ENCODED, policy, 1L));
    statusToAdd.add(new EncodingStatus(3, EncodingStatus.Status.REPAIR_ACTIVE, policy, 1L));
    statusToAdd.add(new EncodingStatus(4, EncodingStatus.Status.REPAIR_ACTIVE, policy, 1L));
    statusToAdd.add(new EncodingStatus(5, EncodingStatus.Status.ENCODING_REQUESTED, policy, 1L));

    HopsTransactionalRequestHandler addReq = new HopsTransactionalRequestHandler(HDFSOperationType.ADD_ENCODING_STATUS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {

      }

      @Override
      public Object performTask() throws StorageException, IOException {
        for (EncodingStatus status : statusToAdd) {
          EntityManager.add(status);
        }
        return null;
      }
    };
    addReq.handle();

    HopsTransactionalRequestHandler countReq = new HopsTransactionalRequestHandler(
        HDFSOperationType.COUNT_REQUESTED_ENCODINGS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {

      }

      @Override
      public Object performTask() throws StorageException, IOException {
        return EntityManager.count(EncodingStatus.Counter.RequestedEncodings);
      }
    };
    assertEquals(count(statusToAdd, EncodingStatus.Status.ENCODING_REQUESTED), (int) (Integer) countReq.handle());

    // Cleanup
    HopsTransactionalRequestHandler delReq = new HopsTransactionalRequestHandler(HDFSOperationType.DELETE_ENCODING_STATUS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {

      }

      @Override
      public Object performTask() throws StorageException, IOException {
        for (EncodingStatus status : statusToAdd) {
          EntityManager.remove(status);
        }
        return null;
      }
    };
    delReq.handle();
  }

  @Test
  public void testFindEncodingRequested() throws IOException {
    final EncodingPolicy policy = new EncodingPolicy("codec", (short) 1);
    final ArrayList<EncodingStatus> statusToAdd = new ArrayList<EncodingStatus>();
    statusToAdd.add(new EncodingStatus(1, EncodingStatus.Status.ENCODING_REQUESTED, policy, 1L));
    statusToAdd.add(new EncodingStatus(2, EncodingStatus.Status.ENCODED, policy, 1L));
    statusToAdd.add(new EncodingStatus(3, EncodingStatus.Status.REPAIR_ACTIVE, policy, 1L));
    statusToAdd.add(new EncodingStatus(4, EncodingStatus.Status.REPAIR_ACTIVE, policy, 1L));
    statusToAdd.add(new EncodingStatus(5, EncodingStatus.Status.ENCODING_REQUESTED, policy, 1L));

    HopsTransactionalRequestHandler addReq = new HopsTransactionalRequestHandler(HDFSOperationType.ADD_ENCODING_STATUS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {

      }

      @Override
      public Object performTask() throws StorageException, IOException {
        for (EncodingStatus status : statusToAdd) {
          EntityManager.add(status);
        }
        return null;
      }
    };
    addReq.handle();

    LightWeightRequestHandler findReq = new LightWeightRequestHandler(
        EncodingStatusOperationType.FIND_BY_INODE_ID) {

      @Override
      public Object performTask() throws StorageException, IOException {
        EncodingStatusDataAccess dataAccess = (EncodingStatusDataAccess) StorageFactory.getDataAccess(EncodingStatusDataAccess.class);
        Long limit = (Long) getParams()[0];
        return dataAccess.findRequestedEncodings(limit);
      }
    };
    findReq.setParams(Long.MAX_VALUE);
    Collection<EncodingStatus> foundStatus = (Collection<EncodingStatus>) findReq.handle();
    assertEquals(count(statusToAdd, EncodingStatus.Status.ENCODING_REQUESTED),
        count(foundStatus, EncodingStatus.Status.ENCODING_REQUESTED));

    long limit = 1;
    findReq.setParams(limit);
    foundStatus = (Collection<EncodingStatus>) findReq.handle();
    assertEquals(count(foundStatus, EncodingStatus.Status.ENCODING_REQUESTED), limit);

    // Cleanup
    HopsTransactionalRequestHandler delReq = new HopsTransactionalRequestHandler(HDFSOperationType.DELETE_ENCODING_STATUS) {
      @Override
      public void acquireLock(TransactionLocks locks) throws IOException {

      }

      @Override
      public Object performTask() throws StorageException, IOException {
        for (EncodingStatus status : statusToAdd) {
          EntityManager.remove(status);
        }
        return null;
      }
    };
    delReq.handle();
  }

  private int count(Collection<EncodingStatus> collection, EncodingStatus.Status status) {
    int count = 0;
    for (EncodingStatus encodingStatus : collection) {
      if (encodingStatus.getStatus().equals(status)) {
        count++;
      }
    }
    return count;
  }
}
