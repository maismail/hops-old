package se.sics.hop.metadata.security.token.block;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.Map;
import org.apache.hadoop.hdfs.security.token.block.BlockKey;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.DataEncryptionKey;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import se.sics.hop.metadata.lock.TransactionLockAcquirer;
import se.sics.hop.metadata.lock.TransactionLockTypes.LockType;
import se.sics.hop.transaction.EntityManager;
import se.sics.hop.exception.PersistanceException;
import se.sics.hop.transaction.handler.RequestHandler;
import se.sics.hop.transaction.handler.TransactionalRequestHandler;
import org.apache.hadoop.util.Time;
import se.sics.hop.metadata.Variables;
import se.sics.hop.metadata.lock.HDFSTransactionLocks;
import se.sics.hop.transaction.handler.HDFSOperationType;

/**
 * Persisted version of the BlockTokenSecretManager to be used by the NameNode
 * We add persistence by overriding only the methods used by the NameNode
 *
 * @author Mahmoud Ismail <maism@sics.se>
 */
public class NameNodeBlockTokenSecretManager extends BlockTokenSecretManager {

  private boolean isLeader;

  /**
   * Constructor for masters.
   */
  public NameNodeBlockTokenSecretManager(long keyUpdateInterval,
          long tokenLifetime, boolean isLeader, String blockPoolId,
          String encryptionAlgorithm) throws IOException {
    super(true, keyUpdateInterval, tokenLifetime, blockPoolId,
            encryptionAlgorithm);
    this.isLeader = isLeader;
    this.setSerialNo(new SecureRandom().nextInt());
    if (isLeader) {
      // TODO[Hooman]: Since Master is keeping the serialNo locally, so whenever
      // A namenode crashes it should remove all keys from the database.
      this.generateKeys();
    } else {
      retrieveBlockKeys();
    }
  }

  @Override
  public void setSerialNo(int serialNo) {
    this.serialNo = serialNo;
  }

  private void generateKeys() throws IOException {
    if (!isMaster) {
      return;
    }
    /*
     * Need to set estimated expiry dates for currentKey and nextKey so that if
     * NN crashes, DN can still expire those keys. NN will stop using the newly
     * generated currentKey after the first keyUpdateInterval, however it may
     * still be used by DN and Balancer to generate new tokens before they get a
     * chance to sync their keys with NN. Since we require keyUpdInterval to be
     * long enough so that all live DN's and Balancer will sync their keys with
     * NN at least once during the period, the estimated expiry date for
     * currentKey is set to now() + 2 * keyUpdateInterval + tokenLifetime.
     * Similarly, the estimated expiry date for nextKey is one keyUpdateInterval
     * more.
     */
    setSerialNo(serialNo + 1);
    currentKey = new BlockKey(serialNo, Time.now() + 2
            * keyUpdateInterval + tokenLifetime, generateSecret());
    currentKey.setKeyType(BlockKey.KeyType.CurrKey);
    setSerialNo(serialNo + 1);
    nextKey = new BlockKey(serialNo, Time.now() + 3
            * keyUpdateInterval + tokenLifetime, generateSecret());
    nextKey.setKeyType(BlockKey.KeyType.NextKey);
    addBlockKeys();
  }

  @Override
  public ExportedBlockKeys exportKeys() throws IOException {
    if (!isMaster) {
      return null;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Exporting access keys");
    }
    BlockKey[] allKeys = getAllKeysAndSync();
    return new ExportedBlockKeys(true, keyUpdateInterval, tokenLifetime,
            currentKey, allKeys);

  }

  @Override
  public boolean updateKeys(final long updateTime) throws IOException {
    if (updateTime > keyUpdateInterval) {
      return updateKeys();
    }
    return false;
  }

  @Override
  public boolean updateKeys() throws IOException {
    if (!isMaster) {
      return false;
    }
    if (isLeader) {
      LOG.info("Updating block keys");
      removeExpiredKeys();
      updateBlockKeys();
    } else {
      retrieveBlockKeys();
    }
    return true;
  }

  @Override
  public DataEncryptionKey generateDataEncryptionKey() throws IOException {
    byte[] nonce = new byte[8];
    nonceGenerator.nextBytes(nonce);
    BlockKey key = getBlockKeyByType(BlockKey.KeyType.CurrKey);

    byte[] encryptionKey = createPassword(nonce, key.getKey());
    return new DataEncryptionKey(key.getKeyId(), blockPoolId, nonce,
            encryptionKey, Time.now() + tokenLifetime,
            encryptionAlgorithm);
  }

  @Override
  protected byte[] createPassword(BlockTokenIdentifier identifier) {
    BlockKey key;
    try {
      key = getBlockKeyByType(BlockKey.KeyType.CurrKey);
    } catch (IOException ex) {
      throw new IllegalStateException("currentKey hasn't been initialized. [" + ex.getMessage() + "]");
    }
    if (key == null) {
      throw new IllegalStateException("currentKey hasn't been initialized.");
    }
    identifier.setExpiryDate(Time.now() + tokenLifetime);
    identifier.setKeyId(key.getKeyId());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Generating block token for " + identifier.toString());
    }
    return createPassword(identifier.getBytes(), key.getKey());
  }

  @Override
  public byte[] retrievePassword(BlockTokenIdentifier identifier) throws InvalidToken {
    if (isExpired(identifier.getExpiryDate())) {
      throw new InvalidToken("Block token with " + identifier.toString()
              + " is expired.");
    }
    BlockKey key = null;
    try {
      key = getBlockKeyById(identifier.getKeyId());
    } catch (IOException ex) {
    }

    if (key == null) {
      throw new InvalidToken("Can't re-compute password for "
              + identifier.toString() + ", since the required block key (keyID="
              + identifier.getKeyId() + ") doesn't exist.");
    }
    return createPassword(identifier.getBytes(), key.getKey());
  }

  public void generateKeysIfNeeded(boolean isLeader) throws IOException {
    this.isLeader = isLeader;
    retrieveBlockKeys();
    if (currentKey == null && nextKey == null) {
      generateKeys();
    }
  }

  public void updateLeaderState(boolean isLeader) {
    this.isLeader = isLeader;
  }

  private void retrieveBlockKeys() throws IOException {
    currentKey = getBlockKeyByType(BlockKey.KeyType.CurrKey);
    nextKey = getBlockKeyByType(BlockKey.KeyType.NextKey);
  }

  private void addBlockKeys() throws IOException {
    new TransactionalRequestHandler(HDFSOperationType.ADD_BLOCK_TOKENS) {
      @Override
      public HDFSTransactionLocks acquireLock() throws PersistanceException, IOException {
        TransactionLockAcquirer tla = new TransactionLockAcquirer();
        tla.getLocks().addBlockKey(LockType.WRITE);
        return tla.acquire();
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        Variables.updateBlockTokenKeys(currentKey, nextKey);
        return null;
      }
    }.handle();
  }

  private BlockKey getBlockKeyById(int keyId) throws IOException {
    return Variables.getAllBlockTokenKeysByIDLW().get(keyId);
  }

  private BlockKey getBlockKeyByType(BlockKey.KeyType keytype) throws IOException {
    return Variables.getAllBlockTokenKeysByTypeLW().get(keytype.ordinal());
  }

  private BlockKey[] getAllKeysAndSync() throws IOException {
    BlockKey[] allKeysArr = null;
    Collection<BlockKey> allKeys = getAllKeys();
    if (allKeys != null) {
      for (BlockKey key : allKeys) {
        if (key.isCurrKey()) {
          currentKey = key;
        } else if (key.isNextKey()) {
          nextKey = key;
        }
      }
      allKeysArr = allKeys.toArray(new BlockKey[allKeys.size()]);
    }
    return allKeysArr;
  }

  private Collection<BlockKey> getAllKeys() throws IOException {
    return Variables.getAllBlockTokenKeysByIDLW().values();
  }

  private void removeExpiredKeys() throws IOException {
    new TransactionalRequestHandler(HDFSOperationType.REMOVE_BLOCK_KEY) {
      @Override
      public HDFSTransactionLocks acquireLock() throws PersistanceException, IOException {
        TransactionLockAcquirer tla = new TransactionLockAcquirer();
        tla.getLocks().addBlockKey(LockType.WRITE);
        return tla.acquire();
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        Map<Integer, BlockKey> keys = Variables.getAllBlockTokenKeysByID();
        for (BlockKey key : keys.values()) {
          if (key != null) {
            if (isExpired(key.getExpiryDate())) {
              EntityManager.remove(key);
            }
          }
        }
        return null;
      }
    }.handle();
  }

  private void updateBlockKeys() throws IOException {
    new TransactionalRequestHandler(HDFSOperationType.UPDATE_BLOCK_KEYS) {
      @Override
      public HDFSTransactionLocks acquireLock() throws PersistanceException, IOException {
        TransactionLockAcquirer tla = new TransactionLockAcquirer();
        tla.getLocks().addBlockKey(LockType.WRITE);
        return tla.acquire();
      }

      @Override
      public Object performTask() throws PersistanceException, IOException {
        Map<Integer, BlockKey> keys = Variables.getAllBlockTokenKeysByType();
        // set final expiry date of retiring currentKey
        // also modifying this key to mark it as 'simple key' instead of 'current key'
        BlockKey currentKeyFromDB = keys.get(BlockKey.KeyType.CurrKey.ordinal());
        currentKeyFromDB.setExpiryDate(Time.now() + keyUpdateInterval + tokenLifetime);
        currentKeyFromDB.setKeyType(BlockKey.KeyType.SimpleKey);

        // after above update, we only have a key marked as 'next key'
        // the 'next key' becomes the 'current key'
        // update the estimated expiry date of new currentKey
        BlockKey nextKeyFromDB = keys.get(BlockKey.KeyType.NextKey.ordinal());
        currentKey = new BlockKey(nextKeyFromDB.getKeyId(), Time.now()
                + 2 * keyUpdateInterval + tokenLifetime, nextKeyFromDB.getKey());
        currentKey.setKeyType(BlockKey.KeyType.CurrKey);

        // generate a new nextKey
        setSerialNo(serialNo + 1);
        nextKey = new BlockKey(serialNo, Time.now() + 3
                * keyUpdateInterval + tokenLifetime, generateSecret());
        nextKey.setKeyType(BlockKey.KeyType.NextKey);

        Variables.updateBlockTokenKeys(currentKey, nextKey, currentKeyFromDB);
        return null;
      }
    }.handle();
  }
}
