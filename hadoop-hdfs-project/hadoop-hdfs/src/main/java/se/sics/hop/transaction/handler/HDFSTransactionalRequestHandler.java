package se.sics.hop.transaction.handler;

import java.io.IOException;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import se.sics.hop.common.HopBlockIdGen;
import se.sics.hop.common.HopINodeIdGen;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.transaction.TransactionInfo;

/**
 * @author Mahmoud Ismail <maism@sics.se>
 */
public abstract class HDFSTransactionalRequestHandler extends TransactionalRequestHandler {

  private final String path;
  
  public HDFSTransactionalRequestHandler(HDFSOperationType opType){
    this(opType, null);
  }
  
  public HDFSTransactionalRequestHandler(HDFSOperationType opType, String path) {
    super(opType);
    this.path = path;
  }

  @Override
  protected Object run(final Object namesystem) throws IOException {

    return super.run(new TransactionInfo() {
      @Override
      public String getContextName(OperationType opType) {
        if (namesystem != null && namesystem instanceof FSNamesystem) {
          return "NN (" + ((FSNamesystem) namesystem).getNamenodeId() + ") " + opType.toString() + "[" + Thread.currentThread().getId() + "]";
        } else {
          return opType.toString();
        }
      }

      @Override
      public void performPostTransactionAction() throws IOException {
        if (namesystem != null && namesystem instanceof FSNamesystem) {
          ((FSNamesystem) namesystem).performPendingSafeModeOperation();
        }
      }
    });
  }

  @Override
  public void preTransactionSetup() throws PersistanceException, IOException {
    super.preTransactionSetup(); 
    
    //call user defined setUp
    setUp();
  }

  
  public void setUp() throws PersistanceException, IOException {
    
  }
}
