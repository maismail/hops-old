/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.security;

import static org.mockito.Mockito.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class TestLdapGroupsMapping {
  private DirContext mockContext;
  
  private LdapGroupsMapping mappingSpy = spy(new LdapGroupsMapping());
  
  @Before
  public void setupMocks() throws NamingException {
    mockContext = mock(DirContext.class);
    doReturn(mockContext).when(mappingSpy).getDirContext();
    
    NamingEnumeration mockUserNamingEnum = mock(NamingEnumeration.class);
    NamingEnumeration mockGroupNamingEnum = mock(NamingEnumeration.class);
    
    // The search functionality of the mock context is reused, so we will
    // return the user NamingEnumeration first, and then the group
    when(mockContext.search(anyString(), anyString(), any(Object[].class),
        any(SearchControls.class)))
        .thenReturn(mockUserNamingEnum, mockGroupNamingEnum);
    
    SearchResult mockUserResult = mock(SearchResult.class);
    // We only ever call hasMoreElements once for the user NamingEnum, so 
    // we can just have one return value
    when(mockUserNamingEnum.hasMoreElements()).thenReturn(true);
    when(mockUserNamingEnum.nextElement()).thenReturn(mockUserResult);
    when(mockUserResult.getNameInNamespace()).thenReturn("CN=some_user,DC=test,DC=com");
    
    SearchResult mockGroupResult = mock(SearchResult.class);
    // We're going to have to define the loop here. We want two iterations,
    // to get both the groups
    when(mockGroupNamingEnum.hasMoreElements()).thenReturn(true, true, false);
    when(mockGroupNamingEnum.nextElement()).thenReturn(mockGroupResult);
    
    // Define the attribute for the name of the first group
    Attribute group1Attr = new BasicAttribute("cn");
    group1Attr.add("group1");
    Attributes group1Attrs = new BasicAttributes();
    group1Attrs.put(group1Attr);
    
    // Define the attribute for the name of the second group
    Attribute group2Attr = new BasicAttribute("cn");
    group2Attr.add("group2");
    Attributes group2Attrs = new BasicAttributes();
    group2Attrs.put(group2Attr);
    
    // This search result gets reused, so return group1, then group2
    when(mockGroupResult.getAttributes()).thenReturn(group1Attrs, group2Attrs);
    
  }
  
  @Test
  public void testGetGroups() throws IOException, NamingException {
    Configuration conf = new Configuration();
    // Set this, so we don't throw an exception
    conf.set(LdapGroupsMapping.LDAP_URL_KEY, "ldap://test");
    
    mappingSpy.setConf(conf);
    // Username is arbitrary, since the spy is mocked to respond the same,
    // regardless of input
    List<String> groups = mappingSpy.getGroups("some_user");
    
    Assert.assertEquals(Arrays.asList("group1", "group2"), groups);
    
    // We should have searched for a user, and then two groups
    verify(mockContext, times(2)).search(anyString(),
                                         anyString(),
                                         any(Object[].class),
                                         any(SearchControls.class));
  }
  
  @Test
  public void testExtractPassword() throws IOException {
    File testDir = new File(System.getProperty("test.build.data", 
                                               "target/test-dir"));
    testDir.mkdirs();
    File secretFile = new File(testDir, "secret.txt");
    Writer writer = new FileWriter(secretFile);
    writer.write("hadoop");
    writer.close();
    
    LdapGroupsMapping mapping = new LdapGroupsMapping();
    Assert.assertEquals("hadoop",
        mapping.extractPassword(secretFile.getPath()));
  }
}
