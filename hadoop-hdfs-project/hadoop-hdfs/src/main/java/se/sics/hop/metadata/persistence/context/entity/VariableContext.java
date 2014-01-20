package se.sics.hop.metadata.persistence.context.entity;

import java.util.Collection;
import java.util.EnumMap;
import se.sics.hop.metadata.persistence.lock.TransactionLockTypes;
import se.sics.hop.metadata.persistence.lock.TransactionLocks;
import se.sics.hop.metadata.persistence.CounterType;
import se.sics.hop.metadata.persistence.FinderType;
import se.sics.hop.metadata.persistence.exceptions.PersistanceException;
import se.sics.hop.metadata.persistence.entity.hop.var.HopVariable;
import se.sics.hop.metadata.persistence.dal.VariableDataAccess;
import se.sics.hop.metadata.persistence.context.LockUpgradeException;
import se.sics.hop.metadata.persistence.exceptions.StorageException;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class VariableContext extends EntityContext<HopVariable> {

  private EnumMap<HopVariable.Finder, HopVariable> variables = new EnumMap<HopVariable.Finder, HopVariable>(HopVariable.Finder.class);
  private EnumMap<HopVariable.Finder, HopVariable> modifiedVariables = new EnumMap<HopVariable.Finder, HopVariable>(HopVariable.Finder.class);
  private EnumMap<HopVariable.Finder, HopVariable> newVariables = new EnumMap<HopVariable.Finder, HopVariable>(HopVariable.Finder.class);

  private VariableDataAccess<HopVariable, HopVariable.Finder> da;

  public VariableContext(VariableDataAccess<HopVariable, HopVariable.Finder>  da) {
    this.da = da;
  }

  @Override
  public void add(HopVariable entity) throws PersistanceException {
    newVariables.put(entity.getType(), entity);
    variables.put(entity.getType(), entity);
  }

  @Override
  public void clear() {
    log("CLEARING THE VARIABLES CONTEXT");
    storageCallPrevented = false;
    variables.clear();
    modifiedVariables.clear();
  }

  @Override
  public int count(CounterType<HopVariable> counter, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public HopVariable find(FinderType<HopVariable> finder, Object... params) throws PersistanceException {
    HopVariable.Finder varType = (HopVariable.Finder) finder;
    HopVariable var = null;
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
  public Collection<HopVariable> findList(FinderType<HopVariable> finder, Object... params) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void prepare(TransactionLocks lks) throws StorageException {
    checkLockUpgrade(lks, modifiedVariables);
    checkLockUpgrade(lks, newVariables);
    da.prepare(newVariables.values(), modifiedVariables.values(), null);
  }

  private void checkLockUpgrade(TransactionLocks lks, EnumMap<HopVariable.Finder, HopVariable> varmap) throws LockUpgradeException {
    for (HopVariable.Finder varType : varmap.keySet()) {
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
  }
  
  @Override
  public void remove(HopVariable var) throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void removeAll() throws PersistanceException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void update(HopVariable var) throws PersistanceException {
    modifiedVariables.put(var.getType(), var);
    variables.put(var.getType(), var);
    log(
            "updated-" + var.getType().toString(),
            CacheHitState.NA,
            new String[]{"value", var.toString()});
  }
}
