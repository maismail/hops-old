package se.sics.hop.metadata.persistence.context;

import se.sics.hop.metadata.persistence.entity.hop.HopVariable;
import se.sics.hop.transcation.EntityManager;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import se.sics.hop.metadata.persistence.exceptions.PersistanceException;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class Variables {

  public static void setBlockId(long blockId) throws PersistanceException {
    updateVariable(new HopVariable(HopVariable.Finder.BlockID, new Long(blockId)));
  }

  public static long getBlockId() throws PersistanceException {
    return getVariable(HopVariable.Finder.BlockID).getValue();
  }

  public static void setGenerationStamp(long stamp) throws PersistanceException {
    updateVariable(new HopVariable(HopVariable.Finder.GenerationStamp, new Long(stamp)));
  }

  public static long getGenerationStamp() throws PersistanceException {
    return getVariable(HopVariable.Finder.GenerationStamp).getValue();
  }

  private static void updateVariable(HopVariable var) throws PersistanceException {
    EntityManager.update(var);
  }

  private static HopVariable getVariable(HopVariable.Finder varType) throws PersistanceException {
    return EntityManager.find(varType);
  }

  public static void registerDefaultValues() {
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.GenerationStamp, GenerationStamp.FIRST_VALID_STAMP);
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.BlockID, 0);
  }
}
