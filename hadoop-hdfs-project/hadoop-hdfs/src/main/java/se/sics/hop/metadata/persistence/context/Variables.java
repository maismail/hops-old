package se.sics.hop.metadata.persistence.context;

import java.util.Arrays;
import java.util.List;
import se.sics.hop.metadata.persistence.entity.hop.HopVariable;
import se.sics.hop.transcation.EntityManager;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import se.sics.hop.metadata.persistence.entity.hop.HopIntArrayVariable;
import se.sics.hop.metadata.persistence.entity.hop.HopLongVariable;
import se.sics.hop.metadata.persistence.exceptions.PersistanceException;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class Variables {
  
  public static void setBlockId(long blockId) throws PersistanceException {
    updateVariable(new HopLongVariable(HopVariable.Finder.BlockID, blockId));
  }
  
  public static long getBlockId() throws PersistanceException {
    return new HopLongVariable(getVariable(HopVariable.Finder.BlockID)).getLongValue();
  }
  
  public static void setInodeId(long inodeId) throws PersistanceException {
    updateVariable(new HopLongVariable(HopVariable.Finder.INodeID, inodeId));
  }
  
  public static long getInodeId() throws PersistanceException {
    return new HopLongVariable(getVariable(HopVariable.Finder.INodeID)).getLongValue();
  }
  
  public static void setGenerationStamp(long stamp) throws PersistanceException {
    updateVariable(new HopLongVariable(HopVariable.Finder.GenerationStamp, new Long(stamp)));
  }
  
  public static long getGenerationStamp() throws PersistanceException {
    return new HopLongVariable(getVariable(HopVariable.Finder.GenerationStamp)).getLongValue();
  }
  
  public static void setReplicationIndex(List<Integer> indeces) throws PersistanceException {
    updateVariable(new HopIntArrayVariable(HopVariable.Finder.ReplicationIndex, indeces));
  }
  
  public static List<Integer> getReplicationIndex() throws PersistanceException {
    return new HopIntArrayVariable(getVariable(HopVariable.Finder.ReplicationIndex)).getIntListValue();
  }
  
  private static void updateVariable(HopVariable var) throws PersistanceException {
    EntityManager.update(var);
  }
  
  private static HopVariable getVariable(HopVariable.Finder varType) throws PersistanceException {
    return EntityManager.find(varType);
  }
  
  public static void registerDefaultValues() {
    HopLongVariable.registerVariableDefaultValue(HopVariable.Finder.GenerationStamp, GenerationStamp.FIRST_VALID_STAMP);
    HopLongVariable.registerVariableDefaultValue(HopVariable.Finder.BlockID, 0);
    HopLongVariable.registerVariableDefaultValue(HopVariable.Finder.INodeID, 1);
    HopIntArrayVariable.registerVariableDefaultValue(HopVariable.Finder.ReplicationIndex, Arrays.asList(0, 0, 0, 0, 0));
  }
}
