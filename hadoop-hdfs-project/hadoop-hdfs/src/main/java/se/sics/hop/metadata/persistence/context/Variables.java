package se.sics.hop.metadata.persistence.context;

import java.util.Arrays;
import java.util.List;
import se.sics.hop.metadata.persistence.entity.hop.var.HopVariable;
import se.sics.hop.transcation.EntityManager;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import se.sics.hop.metadata.persistence.entity.hop.var.HopArrayVariable;
import se.sics.hop.metadata.persistence.entity.hop.var.HopLongVariable;
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

  private static void updateVariable(HopVariable var) throws PersistanceException {
    EntityManager.update(var);
  }

  private static HopVariable getVariable(HopVariable.Finder varType) throws PersistanceException {
    return EntityManager.find(varType);
  }

  public static void registerDefaultValues() {
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.GenerationStamp, new HopLongVariable(GenerationStamp.FIRST_VALID_STAMP).getBytes());
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.BlockID, new HopLongVariable(0).getBytes());
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.INodeID, new HopLongVariable(1).getBytes());
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.ReplicationIndex, new HopArrayVariable(Arrays.asList(0, 0, 0, 0, 0)).getBytes());
  }
}
