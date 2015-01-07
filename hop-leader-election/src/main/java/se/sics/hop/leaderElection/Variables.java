package se.sics.hop.leaderElection;

import se.sics.hop.exception.StorageException;
import se.sics.hop.exception.TransactionContextException;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopVariable;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.metadata.hdfs.entity.hop.var.HopLongVariable;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class Variables {

  public static long getMaxNNID() throws TransactionContextException, StorageException {
    return (Long) getVariable(HopVariable.Finder.MaxNNID).getValue();
  }

  public static void setMaxNNID(long val) throws TransactionContextException, StorageException {
    updateVariable(new HopLongVariable(HopVariable.Finder.MaxNNID, val));
  }

  private static void updateVariable(HopVariable var) throws TransactionContextException, StorageException {
    EntityManager.update(var);
  }

  private static HopVariable getVariable(HopVariable.Finder varType) throws TransactionContextException, StorageException {
    return EntityManager.find(varType);
  }

  public static void registerDefaultValues() {
    HopVariable.registerVariableDefaultValue(HopVariable.Finder.MaxNNID,
            new HopLongVariable(0).getBytes());
  }
}
