package se.sics.hop.transaction.handler;

import java.io.IOException;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import se.sics.hop.transaction.TransactionInfo;

/**
 * @author Mahmoud Ismail <maism@sics.se>
 */
public abstract class HDFSTransactionalRequestHandler extends TransactionalRequestHandler {

    public HDFSTransactionalRequestHandler(HDFSOperationType opType) {
        super(opType);
    }

    @Override
    protected Object run(final Object namesystem) throws IOException {
        
      return super.run(new TransactionInfo() {
        @Override
        public String getContextName(OperationType opType) {
          if (namesystem != null && namesystem instanceof FSNamesystem) {
            return "NN (" + ((FSNamesystem)namesystem).getNamenodeId() + ") " + opType.toString() + "[" + Thread.currentThread().getId() + "]";
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
    
    
}
