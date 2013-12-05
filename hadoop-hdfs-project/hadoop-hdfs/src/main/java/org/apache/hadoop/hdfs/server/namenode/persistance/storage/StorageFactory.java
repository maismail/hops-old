package org.apache.hadoop.hdfs.server.namenode.persistance.storage;

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
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.server.blockmanagement.*;
import org.apache.hadoop.hdfs.server.namenode.*;
import se.sics.hop.metadata.persistence.entity.hop.HopVariable;
import se.sics.hop.metadata.persistence.context.Variables;
import se.sics.hop.metadata.persistence.dalwrapper.LeaseDALWrapper;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.*;
import se.sics.hop.metadata.persistence.DALDriver;
import se.sics.hop.metadata.persistence.DALStorageFactory;
import se.sics.hop.metadata.persistence.StorageConnector;
import se.sics.hop.metadata.persistence.context.entity.BlockInfoContext;
import se.sics.hop.metadata.persistence.context.entity.BlockTokenKeyContext;
import se.sics.hop.metadata.persistence.context.entity.CorruptReplicaContext;
import se.sics.hop.metadata.persistence.context.entity.InvalidatedBlockContext;
import se.sics.hop.metadata.persistence.context.entity.LeasePathContext;
import se.sics.hop.metadata.persistence.context.entity.PendingBlockContext;
import se.sics.hop.metadata.persistence.context.entity.ReplicaContext;
import se.sics.hop.metadata.persistence.context.entity.UnderReplicatedBlockContext;
import se.sics.hop.metadata.persistence.context.entity.VariableContext;
import se.sics.hop.metadata.persistence.dal.BlockInfoDataAccess;
import se.sics.hop.metadata.persistence.dal.BlockTokenKeyDataAccess;
import se.sics.hop.metadata.persistence.dal.CorruptReplicaDataAccess;
import se.sics.hop.metadata.persistence.dal.EntityDataAccess;
import se.sics.hop.metadata.persistence.dal.ExcessReplicaDataAccess;
import se.sics.hop.metadata.persistence.dal.InvalidateBlockDataAccess;
import se.sics.hop.metadata.persistence.dal.LeaderDataAccess;
import se.sics.hop.metadata.persistence.dal.LeaseDataAccess;
import se.sics.hop.metadata.persistence.dal.LeasePathDataAccess;
import se.sics.hop.metadata.persistence.dal.PendingBlockDataAccess;
import se.sics.hop.metadata.persistence.dal.ReplicaDataAccess;
import se.sics.hop.metadata.persistence.dal.ReplicaUnderConstructionDataAccess;
import se.sics.hop.metadata.persistence.dal.StorageInfoDataAccess;
import se.sics.hop.metadata.persistence.dal.UnderReplicatedBlockDataAccess;
import se.sics.hop.metadata.persistence.dal.VariableDataAccess;
import se.sics.hop.metadata.persistence.dalwrapper.BlockInfoDALWrapper;
import se.sics.hop.metadata.persistence.dalwrapper.BlockTokenDALWrapper;
import se.sics.hop.metadata.persistence.dalwrapper.PendingBlockInfoDALWrapper;
import se.sics.hop.metadata.persistence.dalwrapper.ReplicaUnderConstructionDALWrapper;
import se.sics.hop.metadata.persistence.dalwrapper.StorageInfoDALWrapper;
import se.sics.hop.metadata.persistence.entity.hop.HopCorruptReplica;
import se.sics.hop.metadata.persistence.entity.hop.HopExcessReplica;
import se.sics.hop.metadata.persistence.entity.hop.HopIndexedReplica;
import se.sics.hop.metadata.persistence.entity.hop.HopInvalidatedBlock;
import se.sics.hop.metadata.persistence.entity.hop.HopLeasePath;
import se.sics.hop.metadata.persistence.entity.hop.HopUnderReplicatedBlock;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class StorageFactory {

//  private static StorageConnector defaultStorage;
//  private static BlockInfoDataAccess blockInfoDataAccess;
//  private static CorruptReplicaDataAccess corruptReplicaDataAccess;
//  private static ExcessReplicaDataAccess excessReplicaDataAccess;
//  private static InodeDataAccess inodeDataAccess;
//  private static InvalidateBlockDataAccess invalidateBlockDataAccess;
//  private static LeaseDataAccess leaseDataAccess;
//  private static LeasePathDataAccess leasePathDataAccess;
//  private static PendingBlockDataAccess pendingBlockDataAccess;
//  private static ReplicaDataAccess replicaDataAccess;
//  private static ReplicaUnderConstruntionDataAccess replicaUnderConstruntionDataAccess;
//  private static UnderReplicatedBlockDataAccess underReplicatedBlockDataAccess;
//  private static VariablesDataAccess variablesDataAccess;
//  private static LeaderDataAccess leaderDataAccess;
//  private static BlockTokenKeyDataAccess blockTokenKeyDataAccess;
//  private static StorageInfoDataAccess storageInfoDataAccess;
//  private static INodeAttributesDataAccess iNodeAttributesDataAccess;
//  private static Map<Class, EntityDataAccess> dataAccessMap = new HashMap<Class, EntityDataAccess>();
  private static boolean isInitialized = false;
  private static DALStorageFactory dStorageFactory;
  private static StorageInfoDALWrapper storageInfoDAL;
  
//  private static void initDataAccessMap() {
//    dataAccessMap.put(blockInfoDataAccess.getClass().getSuperclass(), blockInfoDataAccess);
//    dataAccessMap.put(corruptReplicaDataAccess.getClass().getSuperclass(), corruptReplicaDataAccess);
//    dataAccessMap.put(excessReplicaDataAccess.getClass().getSuperclass(), excessReplicaDataAccess);
//    dataAccessMap.put(inodeDataAccess.getClass().getSuperclass(), inodeDataAccess);
//    dataAccessMap.put(invalidateBlockDataAccess.getClass().getSuperclass(), invalidateBlockDataAccess);
//    dataAccessMap.put(leaseDataAccess.getClass().getSuperclass(), leaseDataAccess);
//    dataAccessMap.put(leasePathDataAccess.getClass().getSuperclass(), leasePathDataAccess);
//    dataAccessMap.put(pendingBlockDataAccess.getClass().getSuperclass(), pendingBlockDataAccess);
//    dataAccessMap.put(replicaDataAccess.getClass().getSuperclass(), replicaDataAccess);
//    dataAccessMap.put(replicaUnderConstruntionDataAccess.getClass().getSuperclass(), replicaUnderConstruntionDataAccess);
//    dataAccessMap.put(underReplicatedBlockDataAccess.getClass().getSuperclass(), underReplicatedBlockDataAccess);
//    dataAccessMap.put(variablesDataAccess.getClass().getSuperclass(), variablesDataAccess);
//    dataAccessMap.put(leaderDataAccess.getClass().getSuperclass(), leaderDataAccess);
//    dataAccessMap.put(blockTokenKeyDataAccess.getClass().getSuperclass(), blockTokenKeyDataAccess);
//    dataAccessMap.put(storageInfoDataAccess.getClass().getSuperclass(), storageInfoDataAccess);
//    dataAccessMap.put(iNodeAttributesDataAccess.getClass().getSuperclass(), iNodeAttributesDataAccess);
//  }

  public static StorageConnector getConnector() {
    return dStorageFactory.getConnector();
  }

  public static void setConfiguration(Configuration conf) {
    if(isInitialized)  return;
     dStorageFactory = DALDriver.load("", "");
     dStorageFactory.setConfiguration(null);
    
//    Variables.registerDefaultValues();
//    String storageType = conf.get(DFSConfigKeys.DFS_STORAGE_TYPE_KEY, 
//            DFSConfigKeys.DFS_STORAGE_TYPE_DEFAULT);
//    if (storageType.equals(DerbyConnector.DERBY_EMBEDDED)
//            || storageType.equals(DerbyConnector.DERBY_NETWORK_SERVER)) {
//      defaultStorage = DerbyConnector.INSTANCE;
//      defaultStorage.setConfiguration(conf);
////      blockInfoDataAccess = new BlockInfoDerby();
////      corruptReplicaDataAccess = new CorruptReplicaDerby();
////      excessReplicaDataAccess = new ExcessReplicaDerby();
////      inodeDataAccess = new InodeDerby();
////      invalidateBlockDataAccess = new InvalidatedBlockDerby();
////      leaseDataAccess = new LeaseDerby();
////      leasePathDataAccess = new LeasePathDerby();
////      pendingBlockDataAccess = new PendingBlockDerby();
////      replicaDataAccess = new ReplicaDerby();
////      replicaUnderConstruntionDataAccess = new ReplicaUnderConstructionDerby();
////      underReplicatedBlockDataAccess = new UnderReplicatedBlockDerby();
////      leaderDataAccess = new LeaderDerby();
////      // TODO[Hooman]: Add derby data access for block token key.
////      // TODO[Hooman]: Add derby data access for block generation stamp.
////      // TODO[Hooman]: Add derby data access for storage info
////      // TODO[Salman]: Add derby data access for INodeAttributes
//    } else if (storageType.equals("clusterj")) {
//      defaultStorage = ClusterjConnector.INSTANCE;
//      MysqlServerConnector.INSTANCE.setConfiguration(conf);
//      defaultStorage.setConfiguration(conf);
//      blockInfoDataAccess = new BlockInfoClusterj();
//      corruptReplicaDataAccess = new CorruptReplicaClusterj();
//      excessReplicaDataAccess = new ExcessReplicaClusterj();
//      inodeDataAccess = new InodeClusterj();
//      invalidateBlockDataAccess = new InvalidatedBlockClusterj();
//      leaseDataAccess = new LeaseClusterj();
//      leasePathDataAccess = new LeasePathClusterj();
//      pendingBlockDataAccess = new PendingBlockClusterj();
//      replicaDataAccess = new ReplicaClusterj();
//      replicaUnderConstruntionDataAccess = new ReplicaUnderConstructionClusterj();
//      underReplicatedBlockDataAccess = new UnderReplicatedBlockClusterj();
//      variablesDataAccess = new VariablesClusterj();
//      leaderDataAccess = new LeaderClusterj();
//      blockTokenKeyDataAccess = new BlockTokenKeyClusterj();
//      storageInfoDataAccess = new StorageInfoClusterj();
//      iNodeAttributesDataAccess = new INodeAttributesClusterj();
//    }
//
//    initDataAccessMap();
    isInitialized = true;
  }

  public static Map<Class, EntityContext> createEntityContexts() {
    Map<Class, EntityContext> entityContexts = new HashMap<Class, EntityContext>();
    BlockInfoContext bic = new BlockInfoContext(new BlockInfoDALWrapper((BlockInfoDataAccess) getDataAccess(BlockInfoDataAccess.class)));
    entityContexts.put(BlockInfo.class, bic);
    entityContexts.put(BlockInfoUnderConstruction.class, bic);
    entityContexts.put(ReplicaUnderConstruction.class, new ReplicaUnderConstructionContext(new ReplicaUnderConstructionDALWrapper((ReplicaUnderConstructionDataAccess)getDataAccess(ReplicaUnderConstructionDataAccess.class))));
    entityContexts.put(HopIndexedReplica.class, new ReplicaContext((ReplicaDataAccess) getDataAccess(ReplicaDataAccess.class)));
    entityContexts.put(HopExcessReplica.class, new ExcessReplicaContext((ExcessReplicaDataAccess) getDataAccess(ExcessReplicaDataAccess.class)));
    entityContexts.put(HopInvalidatedBlock.class, new InvalidatedBlockContext((InvalidateBlockDataAccess) getDataAccess(InvalidateBlockDataAccess.class)));
    entityContexts.put(Lease.class, new LeaseContext(new LeaseDALWrapper((LeaseDataAccess)getDataAccess(LeaseDataAccess.class))));
    entityContexts.put(HopLeasePath.class, new LeasePathContext((LeasePathDataAccess)getDataAccess(LeasePathDataAccess.class)));
    entityContexts.put(PendingBlockInfo.class, new PendingBlockContext(new PendingBlockInfoDALWrapper((PendingBlockDataAccess)getDataAccess(PendingBlockDataAccess.class))));
//    InodeContext inodeContext = new InodeContext(inodeDataAccess);
//    entityContexts.put(INode.class, inodeContext);
//    entityContexts.put(INodeDirectory.class, inodeContext);
//    entityContexts.put(INodeFile.class, inodeContext);
//    entityContexts.put(INodeDirectoryWithQuota.class, inodeContext);
//    entityContexts.put(INodeSymlink.class, inodeContext);
//    entityContexts.put(INodeFileUnderConstruction.class, inodeContext);
    entityContexts.put(HopCorruptReplica.class, new CorruptReplicaContext((CorruptReplicaDataAccess)getDataAccess(CorruptReplicaDataAccess.class)));
    entityContexts.put(HopUnderReplicatedBlock.class, new UnderReplicatedBlockContext((UnderReplicatedBlockDataAccess) getDataAccess(UnderReplicatedBlockDataAccess.class)));
    entityContexts.put(HopVariable.class, new VariableContext((VariableDataAccess) getDataAccess(VariableDataAccess.class)));
    entityContexts.put(HopLeader.class, new LeaderContext((LeaderDataAccess)getDataAccess(LeaderDataAccess.class)));
    entityContexts.put(BlockKey.class, new BlockTokenKeyContext(new BlockTokenDALWrapper((BlockTokenKeyDataAccess) getDataAccess(BlockTokenKeyDataAccess.class))));
//    entityContexts.put(INodeAttributes.class, new INodeAttributesContext(iNodeAttributesDataAccess));
    return entityContexts;
  }

  public static EntityDataAccess getDataAccess(Class type) {
//    return dataAccessMap.get(type);
    return dStorageFactory.getDataAccess(type);
  }
  
  public static StorageInfoDALWrapper getStorageInfoDataAccess(){
    if(storageInfoDAL == null){
      storageInfoDAL = new StorageInfoDALWrapper((StorageInfoDataAccess)getDataAccess(StorageInfoDataAccess.class));
    }
    return storageInfoDAL;
  }
  
}
