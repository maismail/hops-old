
package org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity;

import java.util.Collection;
import org.apache.hadoop.hdfs.server.namenode.persistance.Variable;
import se.sics.hop.metadata.persistence.exceptions.StorageException;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public abstract class VariablesDataAccess extends EntityDataAccess{
  
  public static final String TABLE_NAME = "variables";
  public static final String ID = "id";
  public static final String VARIABLE_VALUE = "value";
  
  public abstract Variable getVariable(Variable.Finder varType) throws StorageException;
  public abstract void prepare(Collection<Variable> updatedVariables) throws StorageException;
  
}
