package org.apache.hadoop.hdfs.server.namenode.persistance.storage.clusterj;

import com.mysql.clusterj.Session;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import java.util.Collection;
import org.apache.hadoop.hdfs.server.namenode.persistance.Variable;
import org.apache.hadoop.hdfs.server.namenode.persistance.data_access.entity.VariablesDataAccess;
import org.apache.hadoop.hdfs.server.namenode.persistance.storage.StorageException;

/**
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class VariablesClusterj extends VariablesDataAccess {

  @PersistenceCapable(table = TABLE_NAME)
  public interface VariableDTO {

    @PrimaryKey
    @Column(name = ID)
    int getId();

    void setId(int id);

    @Column(name = VARIABLE_VALUE)
    long getValue();

    void setValue(long value);
  }
  private ClusterjConnector connector = ClusterjConnector.INSTANCE;

  @Override
  public Variable getVariable(Variable.Finder varType) throws StorageException {
    try {
      Session session = connector.obtainSession();
      VariableDTO var = session.find(VariableDTO.class, varType.getId());
      if (var == null) {
        throw new StorageException("There is no variable entry with id " + varType.getId());
      }
      return new Variable(varType, var.getValue());
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }

  @Override
  public void prepare(Collection<Variable> updatedVariables) throws StorageException {
    try {
      Session session = connector.obtainSession();
      for (Variable var : updatedVariables) {
        VariableDTO vd = session.newInstance(VariableDTO.class);
        vd.setValue(var.getValue());
        vd.setId(var.getType().getId());
        session.savePersistent(vd);
      }
    } catch (Exception e) {
      throw new StorageException(e);
    }
  }
}
