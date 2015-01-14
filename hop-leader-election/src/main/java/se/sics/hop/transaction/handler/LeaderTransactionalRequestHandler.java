package se.sics.hop.transaction.handler;

import java.io.IOException;
import se.sics.hop.transaction.TransactionInfo;
import se.sics.hop.transaction.lock.LeaderTransactionalLockAcquirer;
import se.sics.hop.transaction.lock.TransactionLockAcquirer;

/**
 * @author Mahmoud Ismail <maism@sics.se>
 */
public abstract class LeaderTransactionalRequestHandler extends
        TransactionalRequestHandler {

  public LeaderTransactionalRequestHandler(LeaderOperationType opType) {
    super(opType);
  }

  @Override
  protected TransactionLockAcquirer newLockAcquirer() {
    return new LeaderTransactionalLockAcquirer();
  }

  @Override
  protected Object execute(final Object namesystem) throws IOException {

    return super.execute(new TransactionInfo() {
      @Override
      public String getContextName(OperationType opType) {
        return opType.toString();
      }

      @Override
      public void performPostTransactionAction() throws IOException {

      }
    });
  }

  @Override
  public void preTransactionSetup() throws IOException {

  }

  @Override
  protected final boolean shouldAbort(Exception e) {
    return true;
  }
}
