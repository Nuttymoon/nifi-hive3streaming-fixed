/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.dbcp.hive;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockVariableRegistry;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class Hive3ConnectionPoolTest {
    private UserGroupInformation userGroupInformation;
    private Hive3ConnectionPool hive3ConnectionPool;
    private BasicDataSource basicDataSource;
    private ComponentLog componentLog;

    @Before
    public void setup() throws Exception {
        userGroupInformation = mock(UserGroupInformation.class);
        basicDataSource = mock(BasicDataSource.class);
        componentLog = mock(ComponentLog.class);

        when(userGroupInformation.doAs(isA(PrivilegedExceptionAction.class))).thenAnswer(invocation -> {
            try {
                return ((PrivilegedExceptionAction) invocation.getArguments()[0]).run();
            } catch (IOException | Error | RuntimeException | InterruptedException e) {
                throw e;
            } catch (Throwable e) {
                throw new UndeclaredThrowableException(e);
            }
        });
        initPool();
    }

    private void initPool() throws Exception {
        hive3ConnectionPool = new Hive3ConnectionPool();

        Field ugiField = Hive3ConnectionPool.class.getDeclaredField("ugi");
        ugiField.setAccessible(true);
        ugiField.set(hive3ConnectionPool, userGroupInformation);

        Field dataSourceField = Hive3ConnectionPool.class.getDeclaredField("dataSource");
        dataSourceField.setAccessible(true);
        dataSourceField.set(hive3ConnectionPool, basicDataSource);

        Field componentLogField = AbstractControllerService.class.getDeclaredField("logger");
        componentLogField.setAccessible(true);
        componentLogField.set(hive3ConnectionPool, componentLog);
    }

    @Test(expected = ProcessException.class)
    public void testGetConnectionSqlException() throws SQLException {
        SQLException sqlException = new SQLException("bad sql");
        when(basicDataSource.getConnection()).thenThrow(sqlException);
        try {
            hive3ConnectionPool.getConnection();
        } catch (ProcessException e) {
            assertEquals(sqlException, e.getCause());
            throw e;
        }
    }

    @Test
    public void testExpressionLanguageSupport() throws Exception {
        final String URL = "jdbc:hive2://localhost:10000/default";
        final String USER = "user";
        final String PASS = "pass";
        final int MAX_CONN = 7;
        final String MAX_WAIT = "10 sec"; // 10000 milliseconds
        final String CONF = "/path/to/hive-site.xml";
        hive3ConnectionPool = new Hive3ConnectionPool();

        Map<PropertyDescriptor, String> props = new HashMap<PropertyDescriptor, String>() {{
            put(Hive3ConnectionPool.DATABASE_URL, "${url}");
            put(Hive3ConnectionPool.DB_USER, "${username}");
            put(Hive3ConnectionPool.DB_PASSWORD, "${password}");
            put(Hive3ConnectionPool.MAX_TOTAL_CONNECTIONS, "${maxconn}");
            put(Hive3ConnectionPool.MAX_WAIT_TIME, "${maxwait}");
            put(Hive3ConnectionPool.HIVE_CONFIGURATION_RESOURCES, "${hiveconf}");
        }};

        MockVariableRegistry registry = new MockVariableRegistry();
        registry.setVariable(new VariableDescriptor("url"), URL);
        registry.setVariable(new VariableDescriptor("username"), USER);
        registry.setVariable(new VariableDescriptor("password"), PASS);
        registry.setVariable(new VariableDescriptor("maxconn"), Integer.toString(MAX_CONN));
        registry.setVariable(new VariableDescriptor("maxwait"), MAX_WAIT);
        registry.setVariable(new VariableDescriptor("hiveconf"), CONF);


        MockConfigurationContext context = new MockConfigurationContext(props, null, registry);
        hive3ConnectionPool.onConfigured(context);

        Field dataSourceField = Hive3ConnectionPool.class.getDeclaredField("dataSource");
        dataSourceField.setAccessible(true);
        basicDataSource = (BasicDataSource) dataSourceField.get(hive3ConnectionPool);
        assertEquals(URL, basicDataSource.getUrl());
        assertEquals(USER, basicDataSource.getUsername());
        assertEquals(PASS, basicDataSource.getPassword());
        assertEquals(MAX_CONN, basicDataSource.getMaxActive());
        assertEquals(10000L, basicDataSource.getMaxWait());
        assertEquals(URL, hive3ConnectionPool.getConnectionURL());
    }
}
