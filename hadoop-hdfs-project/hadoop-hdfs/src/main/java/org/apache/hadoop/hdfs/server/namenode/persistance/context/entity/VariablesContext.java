package org.apache.hadoop.hdfs.server.namenode.persistance.context.entity;

import se.sics.hop.metadata.persistence.context.entity.EntityContext;
import java.util.Collection;
import java.util.EnumMap;
import se.sics.hop.metadata.persistence.lock.TransactionLockTypes;
import se.sics.hop.metadata.persistence.lock.TransactionLocks;
import se.sics.hop.metadata.persistence.CounterType;
import se.sics.hop.metadata.persistence.FinderType;
import se.sics.hop.metadata.persistence.exceptions.PersistanceException;
import org.apache.hadoop.hdfs.server.namenode.persistance.Variable;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.VariablesDataAccess;
import se.sics.hop.metadata.persistence.context.LockUpgradeException;
import se.sics.hop.metadata.persistence.exceptions.StorageException;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class VariablesContext extends EntityContext<Variable> {

  private EnumMap<Variable.Finder, Variable> variables = new EnumMap<Variable.Finder, Variable>(Variable.Finder.class);
  private EnumMap<Variable.Finder, Variable> modifiedVariables = new EnumMap<Variable.Finder, Variable>(Variable.Finder.class);
  private VariablesDataAccess da;

  public VariablesContext(VariablesDataAccess da) {
    this.da = da;
  }

  @Override
  public void add(Variable entity) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void clear() {
    log("CLEARING THE VARIABLES CONTEXT");
    storageCallPrevented = false;
    variables.clear();
    modifiedVariables.clear();
  }

  @Override
  public int count(CounterType<Variable> counter, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Variable find(FinderType<Variable> finder, Object... params) throws PersistanceException {
    Variable.Finder varType = (Variable.Finder) finder;
    Variable var = null;
    if (variables.containsKey(varType)) {
      log("find-" + varType.toString(), CacheHitState.HIT);
      var = variables.get(varType);
    } else {
      log("find-" + varType.toString(), CacheHitState.LOSS);
      aboutToAccessStorage();
      var = da.getVariable(varType);
      variables.put(varType, var);
    }
    return var;
  }

  @Override
  public Collection<Variable> findList(FinderType<Variable> finder, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void prepare(TransactionLocks lks) throws StorageException {
    for (Variable.Finder varType : modifiedVariables.keySet()) {
      switch (varType) {
        case GenerationStamp:
          if (lks.getGenerationStampLock() != TransactionLockTypes.LockType.WRITE) {
            throw new LockUpgradeException("Trying to upgrade generation stamp lock");
          }
          break;
        case BlockID:
          if (lks.getBlockIdCounterLock() != TransactionLockTypes.LockType.WRITE) {
            throw new LockUpgradeException("Trying to upgrade block id counter lock");
          }
          break;
        default:
      }
    }
    da.prepare(modifiedVariables.values());
  }

  @Override
  public void remove(Variable var) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(Variable var) throws PersistanceException {
    modifiedVariables.put(var.getType(), var);
    variables.put(var.getType(), var);
    log(
            "updated-" + var.getType().toString(),
            CacheHitState.NA,
            new String[]{"value", Long.toString(var.getValue())});
  }
}
