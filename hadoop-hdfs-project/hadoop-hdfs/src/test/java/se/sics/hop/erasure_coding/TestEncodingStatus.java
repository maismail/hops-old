package se.sics.hop.erasure_coding;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import se.sics.hop.DALDriver;
import se.sics.hop.DALStorageFactory;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.exception.StorageInitializtionException;
import se.sics.hop.metadata.StorageFactory;
import se.sics.hop.metadata.lock.ErasureCodingTransactionLockAcquirer;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.transaction.handler.EncodingStatusOperationType;
import se.sics.hop.transaction.handler.TransactionalRequestHandler;
import se.sics.hop.transaction.lock.TransactionLockTypes;
import se.sics.hop.transaction.lock.TransactionLocks;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;

public class TestEncodingStatus extends TestCase {

  static {
    try {
      addToClassPath("/home/steffeng/Repositories/hop/hop-metadata-dal-impl-ndb/target/hop-metadata-dal-impl-ndb-1.0-SNAPSHOT-jar-with-dependencies.jar");
      final DALStorageFactory sf = DALDriver.load("se.sics.hop.metadata.ndb.NdbStorageFactory");
      sf.setConfiguration("ndb-config.properties");
    } catch (StorageInitializtionException e) {
      e.printStackTrace();
    }

    try {
      StorageFactory.setConfiguration(new Configuration());
    } catch (StorageInitializtionException e) {
      e.printStackTrace();
    }
  }

  //[M]: just for testing purposes
  private static void addToClassPath(String s) throws StorageInitializtionException {
    try {
      File f = new File(s);
      URL u = f.toURI().toURL();
      URLClassLoader urlClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
      Class urlClass = URLClassLoader.class;
      Method method = urlClass.getDeclaredMethod("addURL", new Class[]{URL.class});
      method.setAccessible(true);
      method.invoke(urlClassLoader, new Object[]{u});
    } catch (MalformedURLException ex) {
      throw new StorageInitializtionException(ex);
    } catch (IllegalAccessException ex) {
      throw new StorageInitializtionException(ex);
    } catch (IllegalArgumentException ex) {
      throw new StorageInitializtionException(ex);
    } catch (InvocationTargetException ex) {
      throw new StorageInitializtionException(ex);
    } catch (NoSuchMethodException ex) {
      throw new StorageInitializtionException(ex);
    } catch (SecurityException ex) {
      throw new StorageInitializtionException(ex);
    }
  }

  @Test
  public void testAddAndFindEncodingStatus() throws IOException {
    final EncodingStatus statusToAdd = new EncodingStatus(1, EncodingStatus.Status.ENCODING_REQUESTED, "codec", 1);

    TransactionalRequestHandler addReq = new TransactionalRequestHandler(EncodingStatusOperationType.ADD) {
      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        return null;
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        EntityManager.add(statusToAdd);
        return null;
      }
    };
    addReq.handle();

    TransactionalRequestHandler findReq = new TransactionalRequestHandler(
        EncodingStatusOperationType.FIND_BY_INODE_ID) {
      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        long id = (Long) getParams()[0];
        ErasureCodingTransactionLockAcquirer ctla = new ErasureCodingTransactionLockAcquirer();
        ctla.getLocks().addEncodingStatusLock(TransactionLockTypes.LockType.READ, id);
        return ctla.acquire();
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        Long id = (Long) getParams()[0];
        return EntityManager.find(EncodingStatus.Finder.ByInodeId, id);
      }
    };
    findReq.setParams(statusToAdd.getInodeId());
    EncodingStatus foundStatus = (EncodingStatus) findReq.handle();
    assertNotNull(foundStatus);
    assertEquals(statusToAdd.getInodeId(), foundStatus.getInodeId());
    assertEquals(statusToAdd.getStatus(), foundStatus.getStatus());
    assertEquals(statusToAdd.getCodec(), foundStatus.getCodec());
    assertEquals(statusToAdd.getModificationTime(), foundStatus.getModificationTime());

    // Cleanup
    TransactionalRequestHandler delReq = new TransactionalRequestHandler(EncodingStatusOperationType.DELETE) {
      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        return null;
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
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
    final EncodingStatus statusToAdd = new EncodingStatus(1, EncodingStatus.Status.ENCODING_REQUESTED, "codec", 1);

    TransactionalRequestHandler addReq = new TransactionalRequestHandler(EncodingStatusOperationType.ADD) {
      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        return null;
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        EntityManager.add(statusToAdd);
        return null;
      }
    };
    addReq.handle();

    final EncodingStatus updatedStatus = new EncodingStatus(1, EncodingStatus.Status.ENCODING_ACTIVE, "codec2", 2);

    TransactionalRequestHandler updateReq = new TransactionalRequestHandler(
        EncodingStatusOperationType.UPDATE) {
      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        long id = (Long) getParams()[0];
        ErasureCodingTransactionLockAcquirer ctla = new ErasureCodingTransactionLockAcquirer();
        ctla.getLocks().addEncodingStatusLock(TransactionLockTypes.LockType.READ, id);
        return ctla.acquire();
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        Long id = (Long) getParams()[0];
        EntityManager.update(updatedStatus);
        return null;
      }
    };
    updateReq.setParams(updatedStatus.getInodeId());
    updateReq.handle();

    TransactionalRequestHandler findReq = new TransactionalRequestHandler(
        EncodingStatusOperationType.FIND_BY_INODE_ID) {
      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        long id = (Long) getParams()[0];
        ErasureCodingTransactionLockAcquirer ctla = new ErasureCodingTransactionLockAcquirer();
        ctla.getLocks().addEncodingStatusLock(TransactionLockTypes.LockType.READ, id);
        return ctla.acquire();
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        Long id = (Long) getParams()[0];
        return EntityManager.find(EncodingStatus.Finder.ByInodeId, id);
      }
    };
    findReq.setParams(statusToAdd.getInodeId());
    EncodingStatus foundStatus = (EncodingStatus) findReq.handle();
    assertNotNull(foundStatus);
    assertEquals(updatedStatus.getInodeId(), foundStatus.getInodeId());
    assertEquals(updatedStatus.getStatus(), foundStatus.getStatus());
    assertEquals(updatedStatus.getCodec(), foundStatus.getCodec());
    assertEquals(updatedStatus.getModificationTime(), foundStatus.getModificationTime());

    // Cleanup
    TransactionalRequestHandler delReq = new TransactionalRequestHandler(EncodingStatusOperationType.DELETE) {
      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        return null;
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
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
    final ArrayList<EncodingStatus> statusToAdd = new ArrayList<EncodingStatus>();
    statusToAdd.add(new EncodingStatus(1, EncodingStatus.Status.ENCODING_REQUESTED, "codec", 1));
    statusToAdd.add(new EncodingStatus(2, EncodingStatus.Status.ENCODED, "codec", 1));
    statusToAdd.add(new EncodingStatus(3, EncodingStatus.Status.REPAIR_ACTIVE, "codec", 1));
    statusToAdd.add(new EncodingStatus(4, EncodingStatus.Status.REPAIR_ACTIVE, "codec", 1));
    statusToAdd.add(new EncodingStatus(5, EncodingStatus.Status.ENCODING_REQUESTED, "codec", 1));

    TransactionalRequestHandler addReq = new TransactionalRequestHandler(EncodingStatusOperationType.ADD) {
      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        return null;
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        for (EncodingStatus status : statusToAdd) {
          EntityManager.add(status);
        }
        return null;
      }
    };
    addReq.handle();

    TransactionalRequestHandler countReq = new TransactionalRequestHandler(
        EncodingStatusOperationType.COUNT_REQUESTED_ENCODINGS) {
      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        return null;
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        return EntityManager.count(EncodingStatus.Counter.RequestedEncodings);
      }
    };
    assertEquals(count(statusToAdd, EncodingStatus.Status.ENCODING_REQUESTED), (int) (Integer) countReq.handle());

    // Cleanup
    TransactionalRequestHandler delReq = new TransactionalRequestHandler(EncodingStatusOperationType.DELETE) {
      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        return null;
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
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
    final ArrayList<EncodingStatus> statusToAdd = new ArrayList<EncodingStatus>();
    statusToAdd.add(new EncodingStatus(1, EncodingStatus.Status.ENCODING_REQUESTED, "codec", 1));
    statusToAdd.add(new EncodingStatus(2, EncodingStatus.Status.ENCODED, "codec", 1));
    statusToAdd.add(new EncodingStatus(3, EncodingStatus.Status.REPAIR_ACTIVE, "codec", 1));
    statusToAdd.add(new EncodingStatus(4, EncodingStatus.Status.REPAIR_ACTIVE, "codec", 1));
    statusToAdd.add(new EncodingStatus(5, EncodingStatus.Status.ENCODING_REQUESTED, "codec", 1));

    TransactionalRequestHandler addReq = new TransactionalRequestHandler(EncodingStatusOperationType.ADD) {
      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        return null;
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        for (EncodingStatus status : statusToAdd) {
          EntityManager.add(status);
        }
        return null;
      }
    };
    addReq.handle();

    TransactionalRequestHandler findReq = new TransactionalRequestHandler(
        EncodingStatusOperationType.FIND_BY_INODE_ID) {
      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        long id = (Long) getParams()[0];
        ErasureCodingTransactionLockAcquirer ctla = new ErasureCodingTransactionLockAcquirer();
        ctla.getLocks().addEncodingStatusLock(TransactionLockTypes.LockType.READ, id);
        return ctla.acquire();
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        Long limit = (Long) getParams()[0];
        return EntityManager.findList(EncodingStatus.Finder.LimitedByStatusRequestEncodings, limit);
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
    TransactionalRequestHandler delReq = new TransactionalRequestHandler(EncodingStatusOperationType.DELETE) {
      @Override
      public TransactionLocks acquireLock() throws PersistanceException, IOException {
        return null;
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
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
