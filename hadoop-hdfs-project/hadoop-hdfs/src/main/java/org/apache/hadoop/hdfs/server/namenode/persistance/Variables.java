package org.apache.hadoop.hdfs.server.namenode.persistance;

import se.sics.hop.metadata.persistence.EntityManager;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class Variables {

  public static void setBlockId(long blockId) throws PersistanceException {
    updateVariable(new Variable(Variable.Finder.BlockID, new Long(blockId)));
  }

  public static long getBlockId() throws PersistanceException {
    return getVariable(Variable.Finder.BlockID).getValue();
  }

  public static void setGenerationStamp(long stamp) throws PersistanceException {
    updateVariable(new Variable(Variable.Finder.GenerationStamp, new Long(stamp)));
  }

  public static long getGenerationStamp() throws PersistanceException {
    return getVariable(Variable.Finder.GenerationStamp).getValue();
  }

  private static void updateVariable(Variable var) throws PersistanceException {
    EntityManager.update(var);
  }

  private static Variable getVariable(Variable.Finder varType) throws PersistanceException {
    return EntityManager.find(varType);
  }

  public static void registerDefaultValues() {
    Variable.registerVariableDefaultValue(Variable.Finder.GenerationStamp, GenerationStamp.FIRST_VALID_STAMP);
    Variable.registerVariableDefaultValue(Variable.Finder.BlockID, 0);
  }
}
