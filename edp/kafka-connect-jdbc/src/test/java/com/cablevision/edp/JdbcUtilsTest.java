/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.cablevision.edp;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cablevision.edp.JdbcUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JdbcUtilsTest {

  EmbeddedDerby db;

  @Before
  public void setup() {
    db = new EmbeddedDerby();
  }

  @After
  public void cleanup() throws Exception {
    db.close();
    db.dropDatabase();
  }

  @Test
  public void testGetTablesEmpty() throws Exception {
    assertEquals(Collections.emptyList(), JdbcUtils.getTables(db.getConnection()));
  }

  @Test
  public void testGetTablesSingle() throws Exception {
    db.createTable("test", "id", "INT");
    assertEquals(Arrays.asList("test"), JdbcUtils.getTables(db.getConnection()));
  }

  @Test
  public void testGetTablesMany() throws Exception {
    db.createTable("test", "id", "INT");
    db.createTable("foo", "id", "INT", "bar", "VARCHAR(20)");
    db.createTable("zab", "id", "INT");
    assertEquals(
        new HashSet<String>(Arrays.asList("test", "foo", "zab")),
        new HashSet<String>(JdbcUtils.getTables(db.getConnection())));
  }

  @Test
  public void testGetAutoincrement() throws Exception {
    // Normal case
    db.createTable("test", "id", "INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY", "bar", "INTEGER");
    assertEquals("id", JdbcUtils.getAutoincrementColumn(db.getConnection(), "test"));

    // No auto increment
    db.createTable("none", "id", "INTEGER", "bar", "INTEGER");
    assertNull(JdbcUtils.getAutoincrementColumn(db.getConnection(), "none"));

    // We can't check multiple columns because Derby ties auto increment to identity and
    // disallows multiple auto increment columns. This is probably ok, multiple auto increment
    // columns should be very unusual anyway

    // Normal case, mixed in and not the first column
    db.createTable("mixed",
                   "foo", "INTEGER",
                   "id", "INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY",
                   "bar", "INTEGER");
    assertEquals("id", JdbcUtils.getAutoincrementColumn(db.getConnection(), "mixed"));
  }

  @Test
  public void testIsColumnNullable() throws Exception {
    db.createTable("test", "id", "INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY", "bar", "INTEGER");
    assertFalse(JdbcUtils.isColumnNullable(db.getConnection(), "test", "id"));
    assertTrue(JdbcUtils.isColumnNullable(db.getConnection(), "test", "bar"));

    // Derby does not seem to allow null
    db.createTable("tstest", "ts", "TIMESTAMP NOT NULL", "tsdefault", "TIMESTAMP",
                   "tsnull", "TIMESTAMP DEFAULT NULL");
    assertFalse(JdbcUtils.isColumnNullable(db.getConnection(), "tstest", "ts"));
    // The default for TIMESTAMP columns can vary between databases, but for Derby it is nullable
    assertTrue(JdbcUtils.isColumnNullable(db.getConnection(), "tstest", "tsdefault"));
    assertTrue(JdbcUtils.isColumnNullable(db.getConnection(), "tstest", "tsnull"));
  }
}
