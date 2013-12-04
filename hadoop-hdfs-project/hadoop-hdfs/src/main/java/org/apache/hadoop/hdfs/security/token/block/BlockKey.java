/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.security.token.block;

import javax.crypto.SecretKey;

import org.apache.hadoop.classification.InterfaceAudience;
import se.sics.hop.metadata.persistence.FinderType;
import org.apache.hadoop.security.token.delegation.DelegationKey;

/**
 * Key used for generating and verifying block tokens
 */
@InterfaceAudience.Private
public class BlockKey extends DelegationKey {
  //START_HOP_CODE
  public static enum Finder implements FinderType<BlockKey> {

    ById, ByType, All;

    @Override
    public Class getType() {
      return BlockKey.class;
    }
  }
  public static final short CURR_KEY = 0;
  public static final short NEXT_KEY = 1;
  public static final short SIMPLE_KEY = -1;
  private short keyType;
  //END_HOP_CODE
  
  public BlockKey() {
    super();
  }

  public BlockKey(int keyId, long expiryDate, SecretKey key) {
    super(keyId, expiryDate, key);
  }

  public BlockKey(int keyId, long expiryDate, byte[] encodedKey) {
    super(keyId, expiryDate, encodedKey);
  }
  //START_HOP_CODE
  public void setKeyType(short keyType) {
    if (keyType == CURR_KEY || keyType == NEXT_KEY || keyType == SIMPLE_KEY) {
      this.keyType = keyType;
    } else {
      throw new IllegalArgumentException("Wrong key type " + keyType);
    }
  }

  public short getKeyType() {
    return keyType;
  }

  public boolean isCurrKey() {
    return this.keyType == CURR_KEY;
  }

  public boolean isNextKey() {
    return this.keyType == NEXT_KEY;
  }

  public boolean isSimpleKey() {
    return this.keyType == SIMPLE_KEY;
  }
  //END_HOP_CODE
}
