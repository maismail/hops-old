package se.sics.hop.metadata.persistence;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import se.sics.hop.metadata.persistence.context.entity.ReplicaUnderConstructionContext;
import se.sics.hop.metadata.persistence.context.entity.ExcessReplicaContext;
import se.sics.hop.metadata.persistence.context.entity.LeaderContext;
import se.sics.hop.metadata.persistence.entity.hop.HopLeader;
import se.sics.hop.metadata.persistence.context.entity.LeaseContext;
import se.sics.hop.metadata.persistence.context.entity.EntityContext;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.hdfs.server.namenode.*;
import se.sics.hop.metadata.persistence.context.Variables;
import se.sics.hop.metadata.persistence.dalwrapper.LeaseDALWrapper;
import se.sics.hop.metadata.persistence.context.entity.BlockInfoContext;
import se.sics.hop.metadata.persistence.context.entity.CorruptReplicaContext;
import se.sics.hop.metadata.persistence.context.entity.INodeAttributesContext;
import se.sics.hop.metadata.persistence.context.entity.INodeContext;
import se.sics.hop.metadata.persistence.context.entity.InvalidatedBlockContext;
import se.sics.hop.metadata.persistence.context.entity.LeasePathContext;
import se.sics.hop.metadata.persistence.context.entity.PendingBlockContext;
import se.sics.hop.metadata.persistence.context.entity.ReplicaContext;
import se.sics.hop.metadata.persistence.context.entity.UnderReplicatedBlockContext;
import se.sics.hop.metadata.persistence.context.entity.VariableContext;
import se.sics.hop.metadata.persistence.dal.BlockInfoDataAccess;
import se.sics.hop.metadata.persistence.dal.CorruptReplicaDataAccess;
import se.sics.hop.metadata.persistence.dal.EntityDataAccess;
import se.sics.hop.metadata.persistence.dal.ExcessReplicaDataAccess;
import se.sics.hop.metadata.persistence.dal.INodeAttributesDataAccess;
import se.sics.hop.metadata.persistence.dal.INodeDataAccess;
import se.sics.hop.metadata.persistence.dal.InvalidateBlockDataAccess;
import se.sics.hop.metadata.persistence.dal.LeaderDataAccess;
import se.sics.hop.metadata.persistence.dal.LeaseDataAccess;
import se.sics.hop.metadata.persistence.dal.LeasePathDataAccess;
import se.sics.hop.metadata.persistence.dal.PendingBlockDataAccess;
import se.sics.hop.metadata.persistence.dal.ReplicaDataAccess;
import se.sics.hop.metadata.persistence.dal.ReplicaUnderConstructionDataAccess;
import se.sics.hop.metadata.persistence.dal.UnderReplicatedBlockDataAccess;
import se.sics.hop.metadata.persistence.dal.VariableDataAccess;
import se.sics.hop.metadata.persistence.dalwrapper.BlockInfoDALWrapper;
import se.sics.hop.metadata.persistence.dalwrapper.INodeAttributeDALWrapper;
import se.sics.hop.metadata.persistence.dalwrapper.INodeDALWrapper;
import se.sics.hop.metadata.persistence.dalwrapper.PendingBlockInfoDALWrapper;
import se.sics.hop.metadata.persistence.dalwrapper.ReplicaUnderConstructionDALWrapper;
import se.sics.hop.metadata.persistence.entity.hop.HopCorruptReplica;
import se.sics.hop.metadata.persistence.entity.hop.HopExcessReplica;
import se.sics.hop.metadata.persistence.entity.hop.HopIndexedReplica;
import se.sics.hop.metadata.persistence.entity.hop.HopInvalidatedBlock;
import se.sics.hop.metadata.persistence.entity.hop.HopLeasePath;
import se.sics.hop.metadata.persistence.entity.hop.var.HopLongVariable;
import se.sics.hop.metadata.persistence.entity.hop.HopUnderReplicatedBlock;
import se.sics.hop.metadata.persistence.entity.hop.var.HopArrayVariable;
import se.sics.hop.metadata.persistence.entity.hop.var.HopByteArrayVariable;
import se.sics.hop.metadata.persistence.entity.hop.var.HopStringVariable;
import se.sics.hop.metadata.persistence.entity.hop.var.HopVariable;
import se.sics.hop.metadata.persistence.exceptions.StorageInitializtionException;

/**
 *
 * @author Hooman <hooman@sics.se>
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class StorageFactory {

  private static boolean isInitialized = false;
  private static DALStorageFactory dStorageFactory;
  private static Map<Class, EntityDataAccess> dataAccessWrappers = new HashMap<Class, EntityDataAccess>();

  public static StorageConnector getConnector() {
    return dStorageFactory.getConnector();
  }

  public static void setConfiguration(Configuration conf) throws StorageInitializtionException {
    if (isInitialized) {
      return;
    }
    Variables.registerDefaultValues();
    addToClassPath(conf.get(DFSConfigKeys.DFS_STORAGE_DRIVER_JAR_FILE, DFSConfigKeys.DFS_STORAGE_DRIVER_JAR_FILE_DEFAULT));
    dStorageFactory = DALDriver.load(conf.get(DFSConfigKeys.DFS_STORAGE_DRIVER_CLASS, DFSConfigKeys.DFS_STORAGE_DRIVER_CLASS_DEFAULT));
    dStorageFactory.setConfiguration(conf.get(DFSConfigKeys.DFS_STORAGE_DRIVER_CONFIG_FILE, DFSConfigKeys.DFS_STORAGE_DRIVER_CONFIG_FILE_DEFAULT));
    initDataAccessWrappers();
    isInitialized = true;
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
    dataAccessWrappers.clear();
    dataAccessWrappers.put(BlockInfoDataAccess.class, new BlockInfoDALWrapper((BlockInfoDataAccess) getDataAccess(BlockInfoDataAccess.class)));
    dataAccessWrappers.put(ReplicaUnderConstructionDataAccess.class, new ReplicaUnderConstructionDALWrapper((ReplicaUnderConstructionDataAccess) getDataAccess(ReplicaUnderConstructionDataAccess.class)));
    dataAccessWrappers.put(LeaseDataAccess.class, new LeaseDALWrapper((LeaseDataAccess) getDataAccess(LeaseDataAccess.class)));
    dataAccessWrappers.put(PendingBlockDataAccess.class, new PendingBlockInfoDALWrapper((PendingBlockDataAccess) getDataAccess(PendingBlockDataAccess.class)));
    dataAccessWrappers.put(INodeDataAccess.class, new INodeDALWrapper((INodeDataAccess) getDataAccess(INodeDataAccess.class)));
    dataAccessWrappers.put(INodeAttributesDataAccess.class, new INodeAttributeDALWrapper((INodeAttributesDataAccess) getDataAccess(INodeAttributesDataAccess.class)));
  }

  public static Map<Class, EntityContext> createEntityContexts() {
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
    entityContexts.put(HopLongVariable.class, variableContext);
    entityContexts.put(HopByteArrayVariable.class, variableContext);
    entityContexts.put(HopStringVariable.class, variableContext);
    entityContexts.put(HopArrayVariable.class, variableContext);
    entityContexts.put(HopLeader.class, new LeaderContext((LeaderDataAccess) getDataAccess(LeaderDataAccess.class)));
    entityContexts.put(INodeAttributes.class, new INodeAttributesContext((INodeAttributesDataAccess) getDataAccess(INodeAttributesDataAccess.class)));


    return entityContexts;
  }

  public static EntityDataAccess getDataAccess(Class type) {
    if (dataAccessWrappers.containsKey(type)) {
      return dataAccessWrappers.get(type);
    }
    return dStorageFactory.getDataAccess(type);
  }
}
