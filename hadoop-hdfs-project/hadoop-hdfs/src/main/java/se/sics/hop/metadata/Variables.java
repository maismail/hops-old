package se.sics.hop.metadata;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import se.sics.hop.common.CountersQueue;
import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.dal.VariableDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopArrayVariable;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopByteArrayVariable;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopIntVariable;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopLongVariable;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopVariable;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.transaction.handler.LightWeightRequestHandler;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class Variables {

  public static CountersQueue.Counter incrementBlockIdCounter(final int increment) throws IOException {
    return (CountersQueue.Counter) new LightWeightRequestHandler(HDFSOperationType.UPDATE_BLOCK_ID_COUNTER) {
      @Override
      public Object performTask() throws IOException {
        VariableDataAccess vd = (VariableDataAccess) StorageFactory.getDataAccess(VariableDataAccess.class);
        StorageFactory.getConnector().writeLock();
        long oldValue = ((HopLongVariable) vd.getVariable(HopVariable.Finder.BlockID)).getValue();
        long newValue = oldValue + increment;
        vd.setVariable(new HopLongVariable(HopVariable.Finder.BlockID, newValue));
        StorageFactory.getConnector().readCommitted();
        return new CountersQueue.Counter(oldValue, newValue);
      }
    }.handle();
  }

  public static CountersQueue.Counter incrementINodeIdCounter(final int increment) throws IOException {
    return (CountersQueue.Counter) new LightWeightRequestHandler(HDFSOperationType.UPDATE_INODE_ID_COUNTER) {
      @Override
      public Object performTask() throws IOException {
        VariableDataAccess vd = (VariableDataAccess) StorageFactory.getDataAccess(VariableDataAccess.class);
        StorageFactory.getConnector().writeLock();
        int oldValue = ((HopIntVariable) vd.getVariable(HopVariable.Finder.INodeID)).getValue();
        int newValue = oldValue + increment;
        vd.setVariable(new HopIntVariable(HopVariable.Finder.INodeID, newValue));
        StorageFactory.getConnector().readCommitted();
        return new CountersQueue.Counter(oldValue, newValue);
      }
    }.handle();
  }

  public static void resetMisReplicatedIndex() throws IOException{
    incrementMisReplicatedIndex(0);
  }
  
  public static Long incrementMisReplicatedIndex(final int increment) throws IOException {
    return (Long) new LightWeightRequestHandler(HDFSOperationType.INCREMENT_MIS_REPLICATED_FILES_INDEX) {
      @Override
      public Object performTask() throws IOException {
        VariableDataAccess vd = (VariableDataAccess) StorageFactory.getDataAccess(VariableDataAccess.class);
        StorageFactory.getConnector().writeLock();
        HopLongVariable var = (HopLongVariable) vd.getVariable(HopVariable.Finder.MisReplicatedFilesIndex);
        long oldValue = var == null ? 0 : var.getValue();
        long newValue = increment == 0 ? 0 : oldValue + increment;
        vd.setVariable(new HopLongVariable(HopVariable.Finder.MisReplicatedFilesIndex, newValue));
        StorageFactory.getConnector().readCommitted();
        return newValue;
      }
    }.handle();
  }
  
  public static void enterClusterSafeMode() throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.ENTER_CLUSTER_SAFE_MODE) {
      @Override
      public Object performTask() throws IOException {
        VariableDataAccess vd = (VariableDataAccess) StorageFactory.getDataAccess(VariableDataAccess.class);
        StorageFactory.getConnector().writeLock();
        vd.setVariable(new HopIntVariable(HopVariable.Finder.ClusterInSafeMode, 1));
        StorageFactory.getConnector().readCommitted();
        return null;
      }
    }.handle();
  }
  
    public static void exitClusterSafeMode() throws IOException {
    new LightWeightRequestHandler(HDFSOperationType.EXIT_CLUSTER_SAFE_MODE) {
      @Override
      public Object performTask() throws IOException {
        VariableDataAccess vd = (VariableDataAccess) StorageFactory.getDataAccess(VariableDataAccess.class);
        StorageFactory.getConnector().writeLock();
        vd.setVariable(new HopIntVariable(HopVariable.Finder.ClusterInSafeMode, 0));
        StorageFactory.getConnector().readCommitted();
        return null;
      }
    }.handle();
  }
    
  public static boolean isClusterInSafeMode() throws IOException {
    Boolean safemode = (Boolean) new LightWeightRequestHandler(HDFSOperationType.GET_CLUSTER_SAFE_MODE) {
      @Override
      public Object performTask() throws IOException {
        VariableDataAccess vd = (VariableDataAccess) StorageFactory.getDataAccess(VariableDataAccess.class);
        StorageFactory.getConnector().readLock();
        HopIntVariable var = (HopIntVariable) vd.getVariable(HopVariable.Finder.ClusterInSafeMode);
        StorageFactory.getConnector().readCommitted();
        return var.getValue() == 1;
      }
    }.handle();
    return safemode;
  }
  
  public static CountersQueue.Counter incrementQuotaUpdateIdCounter(final int increment) throws IOException {
    return (CountersQueue.Counter) new LightWeightRequestHandler(HDFSOperationType.UPDATE_INODE_ID_COUNTER) {
      @Override
      public Object performTask() throws IOException {
        VariableDataAccess vd = (VariableDataAccess) StorageFactory.getDataAccess(VariableDataAccess.class);
        StorageFactory.getConnector().writeLock();
        int oldValue = ((HopIntVariable) vd.getVariable(HopVariable.Finder.QuotaUpdateID)).getValue();
        int newValue = oldValue + increment;
        vd.setVariable(new HopIntVariable(HopVariable.Finder.QuotaUpdateID, newValue));
        return new CountersQueue.Counter(oldValue, newValue);
      }
    }.handle();
  }

  public static void setReplicationIndex(List<Integer> indeces) throws
      StorageException, TransactionContextException {
    updateVariable(new HopArrayVariable(HopVariable.Finder.ReplicationIndex, indeces));
  }

  public static List<Integer> getReplicationIndex() throws
      StorageException, TransactionContextException {
    return (List<Integer>) ((HopArrayVariable) getVariable(HopVariable.Finder.ReplicationIndex)).getVarsValue();
  }

  public static void setStorageInfo(StorageInfo storageInfo) throws
      StorageException, TransactionContextException {
    List<Object> vals = new ArrayList<Object>();
    vals.add(storageInfo.getLayoutVersion());
    vals.add(storageInfo.getNamespaceID());
    vals.add(storageInfo.getClusterID());
    vals.add(storageInfo.getCTime());
    vals.add(storageInfo.getBlockPoolId());
    updateVariable(new HopArrayVariable(HopVariable.Finder.StorageInfo, vals));
  }

  public static StorageInfo getStorageInfo()
      throws StorageException, TransactionContextException {
    HopArrayVariable var = (HopArrayVariable) getVariable(HopVariable.Finder.StorageInfo);
    List<Object> vals = (List<Object>) var.getVarsValue();
    return new StorageInfo((Integer) vals.get(0), (Integer) vals.get(1), (String) vals.get(2), (Long) vals.get(3), (String) vals.get(4));
  }

  
  public static void updateBlockTokenKeys(BlockKey curr, BlockKey next) throws
      IOException {
    updateBlockTokenKeys(curr, next, null);
  }
  
  public static void updateBlockTokenKeys(BlockKey curr, BlockKey next, BlockKey simple) throws
      IOException {
    HopArrayVariable arr = new HopArrayVariable(HopVariable.Finder.BlockTokenKeys);
    arr.addVariable(serializeBlockKey(curr, HopVariable.Finder.BTCurrKey));
    arr.addVariable(serializeBlockKey(next, HopVariable.Finder.BTNextKey));
    if (simple != null) {
      arr.addVariable(serializeBlockKey(simple, HopVariable.Finder.BTSimpleKey));
    }
    updateVariable(arr);
  }
  
  public static Map<Integer, BlockKey> getAllBlockTokenKeysByID() throws IOException {
    return getAllBlockTokenKeys(true, false);
  }

  public static Map<Integer, BlockKey> getAllBlockTokenKeysByType() throws IOException {
    return getAllBlockTokenKeys(false, false);
  }

  public static Map<Integer, BlockKey> getAllBlockTokenKeysByIDLW() throws IOException {
    return getAllBlockTokenKeys(true, true);
  }

  public static Map<Integer, BlockKey> getAllBlockTokenKeysByTypeLW() throws IOException {
    return getAllBlockTokenKeys(false, true);
  }

  public static int getSIdCounter()
      throws StorageException, TransactionContextException {
    return (Integer) getVariable(HopVariable.Finder.SIdCounter).getValue();
  }

  public static void setSIdCounter(int sid)
      throws StorageException, TransactionContextException {
    updateVariable(new HopIntVariable(HopVariable.Finder.SIdCounter, sid));
  }
    
  public static long getMaxNNID()
      throws StorageException, TransactionContextException {
      return (Long) getVariable(HopVariable.Finder.MaxNNID).getValue();
  }
  
  public static void setMaxNNID(long val)
      throws StorageException, TransactionContextException {
      updateVariable(new HopLongVariable(HopVariable.Finder.MaxNNID, val));
  }
  
  private static Map<Integer, BlockKey> getAllBlockTokenKeys(boolean useKeyId, boolean leightWeight) throws IOException {
    List<HopVariable> vars = (List<HopVariable>) (leightWeight ? getVariableLightWeight(HopVariable.Finder.BlockTokenKeys).getValue() 
            : getVariable(HopVariable.Finder.BlockTokenKeys).getValue());
    Map<Integer, BlockKey> keys = new HashMap<Integer, BlockKey>();
    for (HopVariable var : vars) {
      BlockKey key = deserializeBlockKey((HopByteArrayVariable) var);
      int mapKey = useKeyId ? key.getKeyId() : key.getKeyType().ordinal();
      keys.put(mapKey, key);
    }
    return keys;
  }
  
  private static HopByteArrayVariable serializeBlockKey(BlockKey key, HopVariable.Finder keyType) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(os);
    key.write(dos);
    dos.flush();
    return new HopByteArrayVariable(keyType, os.toByteArray());
  }

  private static BlockKey deserializeBlockKey(HopByteArrayVariable var) throws IOException{
    ByteArrayInputStream is = new ByteArrayInputStream((byte[]) var.getValue());
    DataInputStream dis =  new DataInputStream(is);
    BlockKey key = new BlockKey();
    key.readFields(dis);
    switch(var.getType()){
      case BTCurrKey:
        key.setKeyType(BlockKey.KeyType.CurrKey);
        break;
      case BTNextKey:
        key.setKeyType(BlockKey.KeyType.NextKey);
        break;
      case BTSimpleKey:
        key.setKeyType(BlockKey.KeyType.SimpleKey);
    }
    return key;
  }
  
  private static HopVariable getVariableLightWeight(final HopVariable.Finder varType) throws IOException {
    return (HopVariable) new LightWeightRequestHandler(HDFSOperationType.GET_VARIABLE) {
      @Override
      public Object performTask() throws IOException {
        VariableDataAccess vd = (VariableDataAccess) StorageFactory.getDataAccess(VariableDataAccess.class);
        return vd.getVariable(varType);
      }
    }.handle();
  }
    
  private static void updateVariable(HopVariable var) throws
      StorageException, TransactionContextException {
    EntityManager.update(var);
  }

  private static HopVariable getVariable(HopVariable.Finder varType) throws
      StorageException, TransactionContextException {
    return EntityManager.find(varType);
  }
  
  public static void registerDefaultValues() {
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.BlockID, new HopLongVariable(0).getBytes());
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.INodeID, new HopIntVariable(2).getBytes()); // 1 is taken by the root and zero is parent of the root
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.ReplicationIndex, new HopArrayVariable(Arrays.asList(0, 0, 0, 0, 0)).getBytes());
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.SIdCounter, new HopIntVariable(0).getBytes());
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.MaxNNID, new HopLongVariable(0).getBytes());
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.MisReplicatedFilesIndex, new HopLongVariable(0).getBytes());
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.ClusterInSafeMode, new HopIntVariable(1).getBytes());
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.QuotaUpdateID, new HopIntVariable(0).getBytes());
  }
}
