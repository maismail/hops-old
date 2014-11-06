package se.sics.hop.metadata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoUnderConstruction;
import org.apache.hadoop.hdfs.server.blockmanagement.PendingBlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.ReplicaUnderConstruction;
import org.apache.hadoop.hdfs.server.namenode.*;
import se.sics.hop.DALDriver;
import se.sics.hop.DALStorageFactory;
import se.sics.hop.StorageConnector;
import se.sics.hop.common.IDsMonitor;
import se.sics.hop.erasure_coding.EncodingStatus;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.StorageInitializtionException;
import se.sics.hop.log.NDCWrapper;
import se.sics.hop.memcache.PathMemcache;
import se.sics.hop.metadata.adaptor.*;
import se.sics.hop.metadata.context.*;
import se.sics.hop.metadata.hdfs.dal.*;
import se.sics.hop.metadata.hdfs.entity.EntityContext;
import se.sics.hop.metadata.hdfs.entity.hop.*;
import se.sics.hop.metadata.hdfs.entity.hop.var.*;
import se.sics.hop.metadata.lock.HDFSTransactionLockAcquirer;
import se.sics.hop.transaction.ContextInitializer;
import se.sics.hop.transaction.EntityManager;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Hooman <hooman@sics.se>
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class StorageFactory {

  private static boolean isDALInitialized = false;
  private static DALStorageFactory dStorageFactory;
  private static Map<Class, EntityDataAccess> dataAccessAdaptors = new HashMap<Class, EntityDataAccess>();
  
  public static StorageConnector getConnector() {
    return dStorageFactory.getConnector();
  }

  public static void setConfiguration(Configuration conf) throws IOException {
    IDsMonitor.getInstance().setConfiguration(conf);
    PathMemcache.getInstance().setConfiguration(conf);
    HDFSTransactionLockAcquirer.setConfiguration(conf);
    NDCWrapper.enableNDC(conf.getBoolean(DFSConfigKeys.DFS_NDC_ENABLED_KEY, DFSConfigKeys.DFS_NDC_ENABLED_DEFAULT));
    if (!isDALInitialized) {
      Variables.registerDefaultValues();
      addToClassPath(conf.get(DFSConfigKeys.DFS_STORAGE_DRIVER_JAR_FILE, DFSConfigKeys.DFS_STORAGE_DRIVER_JAR_FILE_DEFAULT));
      dStorageFactory = DALDriver.load(conf.get(DFSConfigKeys.DFS_STORAGE_DRIVER_CLASS, DFSConfigKeys.DFS_STORAGE_DRIVER_CLASS_DEFAULT));
      dStorageFactory.setConfiguration(conf.get(DFSConfigKeys.DFS_STORAGE_DRIVER_CONFIG_FILE, DFSConfigKeys.DFS_STORAGE_DRIVER_CONFIG_FILE_DEFAULT));
      initDataAccessWrappers();
      EntityManager.setContextInitializer(getContextInitializer());
      isDALInitialized = true;
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
  
  private static void initDataAccessWrappers() {
    dataAccessAdaptors.clear();
    dataAccessAdaptors.put(BlockInfoDataAccess.class, new BlockInfoDALAdaptor((BlockInfoDataAccess) getDataAccess(BlockInfoDataAccess.class)));
    dataAccessAdaptors.put(ReplicaUnderConstructionDataAccess.class, new ReplicaUnderConstructionDALAdaptor((ReplicaUnderConstructionDataAccess) getDataAccess(ReplicaUnderConstructionDataAccess.class)));
    dataAccessAdaptors.put(LeaseDataAccess.class, new LeaseDALAdaptor((LeaseDataAccess) getDataAccess(LeaseDataAccess.class)));
    dataAccessAdaptors.put(PendingBlockDataAccess.class, new PendingBlockInfoDALAdaptor((PendingBlockDataAccess) getDataAccess(PendingBlockDataAccess.class)));
    dataAccessAdaptors.put(INodeDataAccess.class, new INodeDALAdaptor((INodeDataAccess) getDataAccess(INodeDataAccess.class)));
    dataAccessAdaptors.put(INodeAttributesDataAccess.class, new INodeAttributeDALAdaptor((INodeAttributesDataAccess) getDataAccess(INodeAttributesDataAccess.class)));
    dataAccessAdaptors.put(EncodingStatusDataAccess.class, new EncodingStatusDALAdaptor((EncodingStatusDataAccess) getDataAccess(EncodingStatusDataAccess.class)));
  }

  private static ContextInitializer getContextInitializer() {
    return new ContextInitializer() {
      @Override
      public Map<Class, EntityContext> createEntityContexts() {
        Map<Class, EntityContext> entityContexts = new HashMap<Class, EntityContext>();

        BlockInfoContext bic = new BlockInfoContext((BlockInfoDataAccess) getDataAccess(BlockInfoDataAccess.class));
        entityContexts.put(BlockInfo.class, bic);
        entityContexts.put(BlockInfoUnderConstruction.class, bic);
        entityContexts.put(ReplicaUnderConstruction.class, new ReplicaUnderConstructionContext((ReplicaUnderConstructionDataAccess) getDataAccess(ReplicaUnderConstructionDataAccess.class)));
        entityContexts.put(HopIndexedReplica.class, new ReplicaContext((ReplicaDataAccess) getDataAccess(ReplicaDataAccess.class)));
        entityContexts.put(HopExcessReplica.class, new ExcessReplicaContext((ExcessReplicaDataAccess) getDataAccess(ExcessReplicaDataAccess.class)));
        entityContexts.put(HopInvalidatedBlock.class, new InvalidatedBlockContext((InvalidateBlockDataAccess) getDataAccess(InvalidateBlockDataAccess.class)));
        entityContexts.put(Lease.class, new LeaseContext((LeaseDataAccess) getDataAccess(LeaseDataAccess.class)));
        entityContexts.put(HopLeasePath.class, new LeasePathContext((LeasePathDataAccess) getDataAccess(LeasePathDataAccess.class)));
        entityContexts.put(PendingBlockInfo.class, new PendingBlockContext((PendingBlockDataAccess) getDataAccess(PendingBlockDataAccess.class)));

        INodeContext inodeContext = new INodeContext((INodeDataAccess) getDataAccess(INodeDataAccess.class));
        entityContexts.put(INode.class, inodeContext);
        entityContexts.put(INodeDirectory.class, inodeContext);
        entityContexts.put(INodeFile.class, inodeContext);
        entityContexts.put(INodeDirectoryWithQuota.class, inodeContext);
        entityContexts.put(INodeSymlink.class, inodeContext);
        entityContexts.put(INodeFileUnderConstruction.class, inodeContext);

        entityContexts.put(HopCorruptReplica.class, new CorruptReplicaContext((CorruptReplicaDataAccess) getDataAccess(CorruptReplicaDataAccess.class)));
        entityContexts.put(HopUnderReplicatedBlock.class, new UnderReplicatedBlockContext((UnderReplicatedBlockDataAccess) getDataAccess(UnderReplicatedBlockDataAccess.class)));
        VariableContext variableContext = new VariableContext((VariableDataAccess) getDataAccess(VariableDataAccess.class));
        entityContexts.put(HopVariable.class, variableContext);
        entityContexts.put(HopIntVariable.class, variableContext);
        entityContexts.put(HopLongVariable.class, variableContext);
        entityContexts.put(HopByteArrayVariable.class, variableContext);
        entityContexts.put(HopStringVariable.class, variableContext);
        entityContexts.put(HopArrayVariable.class, variableContext);
        entityContexts.put(HopLeader.class, new LeaderContext((LeaderDataAccess) getDataAccess(LeaderDataAccess.class)));
        entityContexts.put(INodeAttributes.class, new INodeAttributesContext((INodeAttributesDataAccess) getDataAccess(INodeAttributesDataAccess.class)));

        entityContexts.put(EncodingStatus.class, new EncodingStatusContext((EncodingStatusDataAccess) getDataAccess(EncodingStatusDataAccess.class)));
        entityContexts.put(QuotaUpdate.class, new QuotaUpdateContext((QuotaUpdateDataAccess) getDataAccess(QuotaUpdateDataAccess.class)));

        return entityContexts;
      }

      @Override
      public StorageConnector getConnector() {
        return dStorageFactory.getConnector();
      }
    };
  }

  public static EntityDataAccess getDataAccess(Class type) {
    if (dataAccessAdaptors.containsKey(type)) {
      return dataAccessAdaptors.get(type);
    }
    return dStorageFactory.getDataAccess(type);
  }
  
  public static boolean formatStorage() throws StorageException{
    PathMemcache.getInstance().flush();
    return dStorageFactory.getConnector().formatStorage();
  }
  
  public static boolean formatStorage(Class<? extends EntityDataAccess>... das) throws StorageException{
    PathMemcache.getInstance().flush();
    return dStorageFactory.getConnector().formatStorage(das);
  }
}
