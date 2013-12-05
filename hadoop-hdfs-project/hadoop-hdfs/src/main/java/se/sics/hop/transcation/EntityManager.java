package se.sics.hop.transcation;

import java.util.Collection;
import java.util.Map;
import se.sics.hop.metadata.persistence.lock.TransactionLockAcquirer;
import se.sics.hop.metadata.persistence.lock.TransactionLocks;
import se.sics.hop.metadata.persistence.context.TransactionContext;
import se.sics.hop.metadata.persistence.context.entity.EntityContext;
import se.sics.hop.metadata.persistence.StorageConnector;
import se.sics.hop.metadata.persistence.exceptions.StorageException;
import se.sics.hop.metadata.persistence.StorageFactory;
import se.sics.hop.metadata.persistence.CounterType;
import se.sics.hop.metadata.persistence.FinderType;
import se.sics.hop.metadata.persistence.StorageConnector;
import se.sics.hop.metadata.persistence.exceptions.PersistanceException;

/**
 *
 * @author kamal hakimzadeh <kamal@sics.se>
 */
public class EntityManager {

  private EntityManager() {
  }
  private static ThreadLocal<TransactionContext> contexts = new ThreadLocal<TransactionContext>();
  private static StorageConnector connector = StorageFactory.getConnector();

  private static TransactionContext context() {
    TransactionContext context = contexts.get();

    if (context == null) {
      Map<Class, EntityContext> storageMap = StorageFactory.createEntityContexts();
      context = new TransactionContext(connector, storageMap);
      contexts.set(context);
    }
    return context;
  }

  public static void begin() throws StorageException {
    context().begin();
  }

  public static void preventStorageCall() {
    context().preventStorageCall();
  }

  public static void commit(TransactionLocks tlm) throws StorageException {
    context().commit(tlm);
  }

  public static void rollback() throws StorageException {
    context().rollback();
  }

  public static <T> void remove(T obj) throws PersistanceException {
    context().remove(obj);
  }

  public static void removeAll(Class type) throws PersistanceException {
    context().removeAll(type);
  }

  public static <T> Collection<T> findList(FinderType<T> finder, Object... params) throws PersistanceException {
    return context().findList(finder, params);
  }

  public static <T> T find(FinderType<T> finder, Object... params) throws PersistanceException {
    return context().find(finder, params);
  }

  public static int count(CounterType counter, Object... params) throws PersistanceException {
    return context().count(counter, params);
  }

  public static <T> void update(T entity) throws PersistanceException {
    context().update(entity);
  }

  public static <T> void add(T entity) throws PersistanceException {
    context().add(entity);
  }
  
  public static void writeLock() {
    EntityContext.setLockMode(EntityContext.LockMode.WRITE_LOCK);
    connector.writeLock();
  }

  public static void readLock() {
    EntityContext.setLockMode(EntityContext.LockMode.READ_LOCK);
    connector.readLock();
  }

  public static void readCommited() {
    EntityContext.setLockMode(EntityContext.LockMode.READ_COMMITTED);
    connector.readCommitted();
  }
  
  public static void setPartitionKey(Class name, Object key) {
    connector.setPartitionKey(name, key);
  }

  /**
   * Clears transaction context's in-memory data
   */
  public static void clearContext() {
    context().clearContext();
  }
}
