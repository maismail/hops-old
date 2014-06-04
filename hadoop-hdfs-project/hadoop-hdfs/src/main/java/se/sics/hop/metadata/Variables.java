package se.sics.hop.metadata;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopVariable;
import se.sics.hop.transaction.EntityManager;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import se.sics.hop.metadata.hdfs.dal.VariableDataAccess;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopArrayVariable;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopByteArrayVariable;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopLongVariable;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopIntVariable;
import se.sics.hop.transaction.handler.HDFSOperationType;
import se.sics.hop.transaction.handler.LightWeightRequestHandler;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class Variables {

  public static void setBlockId(long blockId) throws PersistanceException {
    updateVariable(new HopLongVariable(HopVariable.Finder.BlockID, blockId));
  }

  public static long getBlockId() throws PersistanceException {
    return (Long) getVariable(HopVariable.Finder.BlockID).getValue();
  }

  public static void setInodeId(long inodeId) throws PersistanceException {
    updateVariable(new HopLongVariable(HopVariable.Finder.INodeID, inodeId));
  }

  public static long getInodeId() throws PersistanceException {
    return (Long) getVariable(HopVariable.Finder.INodeID).getValue();
  }

  public static void setGenerationStamp(long stamp) throws PersistanceException {
    updateVariable(new HopLongVariable(HopVariable.Finder.GenerationStamp, new Long(stamp)));
  }

  public static long getGenerationStamp() throws PersistanceException {
    return (Long) getVariable(HopVariable.Finder.GenerationStamp).getValue();
  }

  public static void setReplicationIndex(List<Integer> indeces) throws PersistanceException {
    updateVariable(new HopArrayVariable(HopVariable.Finder.ReplicationIndex, indeces));
  }

  public static List<Integer> getReplicationIndex() throws PersistanceException {
    return (List<Integer>) ((HopArrayVariable) getVariable(HopVariable.Finder.ReplicationIndex)).getVarsValue();
  }

  public static void setStorageInfo(StorageInfo storageInfo) throws PersistanceException {
    List<Object> vals = new ArrayList<Object>();
    vals.add(storageInfo.getLayoutVersion());
    vals.add(storageInfo.getNamespaceID());
    vals.add(storageInfo.getClusterID());
    vals.add(storageInfo.getCTime());
    vals.add(storageInfo.getBlockPoolId());
    updateVariable(new HopArrayVariable(HopVariable.Finder.StorageInfo, vals));
  }

  public static StorageInfo getStorageInfo() throws PersistanceException {
    HopArrayVariable var = (HopArrayVariable) getVariable(HopVariable.Finder.StorageInfo);
    List<Object> vals = (List<Object>) var.getVarsValue();
    return new StorageInfo((Integer) vals.get(0), (Integer) vals.get(1), (String) vals.get(2), (Long) vals.get(3), (String) vals.get(4));
  }

  
  public static void updateBlockTokenKeys(BlockKey curr, BlockKey next) throws PersistanceException, IOException {
    updateBlockTokenKeys(curr, next, null);
  }
  
  public static void updateBlockTokenKeys(BlockKey curr, BlockKey next, BlockKey simple) throws PersistanceException, IOException {
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

  public static int getSIdCounter() throws PersistanceException {
    return (Integer) getVariable(HopVariable.Finder.SIdCounter).getValue();
  }

  public static void setSIdCounter(int sid) throws PersistanceException {
    updateVariable(new HopIntVariable(HopVariable.Finder.SIdCounter, sid));
  }
    
  public static long getMaxNNID() throws PersistanceException {
      return (Long) getVariable(HopVariable.Finder.MaxNNID).getValue();
  }
  
  public static void setMaxNNID(long val) throws PersistanceException {
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
      public Object performTask() throws PersistanceException, IOException {
        VariableDataAccess vd = (VariableDataAccess) StorageFactory.getDataAccess(VariableDataAccess.class);
        return vd.getVariable(varType);
      }
    }.handle();
  }
    
  private static void updateVariable(HopVariable var) throws PersistanceException {
    EntityManager.update(var);
  }

  private static HopVariable getVariable(HopVariable.Finder varType) throws PersistanceException {
    return EntityManager.find(varType);
  }
  
  public static void registerDefaultValues() {
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.GenerationStamp, new HopLongVariable(GenerationStamp.FIRST_VALID_STAMP).getBytes());
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.BlockID, new HopLongVariable(0).getBytes());
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.INodeID, new HopLongVariable(2).getBytes()); // 1 is taken by the root and zero is parent of the root
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.ReplicationIndex, new HopArrayVariable(Arrays.asList(0, 0, 0, 0, 0)).getBytes());
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.SIdCounter, new HopIntVariable(0).getBytes());
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.MaxNNID, new HopLongVariable(0).getBytes());
  }
}
