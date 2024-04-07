/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class JdbcUtilV2 {
        private static final Logger LOG = LoggerFactory.getLogger(JdbcUtilV2.class);

        // property to control strict-mode (aka check if namespace exists when creating
        // a table)
        static final String STRICT_MODE_PROPERTY = JdbcCatalog.PROPERTY_PREFIX + "strict-mode";

        // Catalog Table
        static final String CATALOG_TABLE_NAME = "iceberg_tables";
        static final String CATALOG_NAME = "catalog_name";
        static final String TABLE_NAMESPACE = "table_namespace";
        static final String TABLE_NAME = "table_name";
        static final String METADATA_LOCATION = "metadata_location";
        static final String PREVIOUS_METADATA_LOCATION = "previous_metadata_location";

        static final String DO_COMMIT_SQL = "UPDATE "
                        + CATALOG_TABLE_NAME
                        + " SET "
                        + METADATA_LOCATION
                        + " = ? , "
                        + PREVIOUS_METADATA_LOCATION
                        + " = ? "
                        + " WHERE "
                        + CATALOG_NAME
                        + " = ? AND "
                        + TABLE_NAMESPACE
                        + " = ? AND "
                        + TABLE_NAME
                        + " = ? AND "
                        + METADATA_LOCATION
                        + " = ?";
        static final String CREATE_CATALOG_TABLE = "CREATE TABLE "
                        + CATALOG_TABLE_NAME
                        + "("
                        + CATALOG_NAME
                        + " VARCHAR(255) NOT NULL,"
                        + TABLE_NAMESPACE
                        + " VARCHAR(255) NOT NULL,"
                        + TABLE_NAME
                        + " VARCHAR(255) NOT NULL,"
                        + METADATA_LOCATION
                        + " VARCHAR(1000),"
                        + PREVIOUS_METADATA_LOCATION
                        + " VARCHAR(1000),"
                        + "PRIMARY KEY ("
                        + CATALOG_NAME
                        + ", "
                        + TABLE_NAMESPACE
                        + ", "
                        + TABLE_NAME
                        + ")"
                        + ")";
        static final String GET_TABLE_SQL = "SELECT * FROM "
                        + CATALOG_TABLE_NAME
                        + " WHERE "
                        + CATALOG_NAME
                        + " = ? AND "
                        + TABLE_NAMESPACE
                        + " = ? AND "
                        + TABLE_NAME
                        + " = ? ";
        static final String LIST_TABLES_SQL = "SELECT * FROM "
                        + CATALOG_TABLE_NAME
                        + " WHERE "
                        + CATALOG_NAME
                        + " = ? AND "
                        + TABLE_NAMESPACE
                        + " = ?";
        static final String RENAME_TABLE_SQL = "UPDATE "
                        + CATALOG_TABLE_NAME
                        + " SET "
                        + TABLE_NAMESPACE
                        + " = ? , "
                        + TABLE_NAME
                        + " = ? "
                        + " WHERE "
                        + CATALOG_NAME
                        + " = ? AND "
                        + TABLE_NAMESPACE
                        + " = ? AND "
                        + TABLE_NAME
                        + " = ? ";
        static final String DROP_TABLE_SQL = "DELETE FROM "
                        + CATALOG_TABLE_NAME
                        + " WHERE "
                        + CATALOG_NAME
                        + " = ? AND "
                        + TABLE_NAMESPACE
                        + " = ? AND "
                        + TABLE_NAME
                        + " = ? ";
        static final String GET_NAMESPACE_SQL = "SELECT "
                        + TABLE_NAMESPACE
                        + " FROM "
                        + CATALOG_TABLE_NAME
                        + " WHERE "
                        + CATALOG_NAME
                        + " = ? AND "
                        + " ( "
                        + TABLE_NAMESPACE
                        + " = ? OR "
                        + TABLE_NAMESPACE
                        + " LIKE ? ESCAPE '\\' "
                        + " ) "
                        + " LIMIT 1";
        static final String LIST_NAMESPACES_SQL = "SELECT DISTINCT "
                        + TABLE_NAMESPACE
                        + " FROM "
                        + CATALOG_TABLE_NAME
                        + " WHERE "
                        + CATALOG_NAME
                        + " = ? AND "
                        + TABLE_NAMESPACE
                        + " LIKE ?";
        static final String LIST_ALL_TABLE_NAMESPACES_SQL = "SELECT DISTINCT "
                        + TABLE_NAMESPACE
                        + " FROM "
                        + CATALOG_TABLE_NAME
                        + " WHERE "
                        + CATALOG_NAME
                        + " = ?";
        static final String DO_COMMIT_CREATE_TABLE_SQL = "INSERT INTO "
                        + CATALOG_TABLE_NAME
                        + " ("
                        + CATALOG_NAME
                        + ", "
                        + TABLE_NAMESPACE
                        + ", "
                        + TABLE_NAME
                        + ", "
                        + METADATA_LOCATION
                        + ", "
                        + PREVIOUS_METADATA_LOCATION
                        + ") "
                        + " VALUES (?,?,?,?,null)";

        // Catalog Namespace Properties
        static final String NAMESPACE_PROPERTIES_TABLE_NAME = "iceberg_namespace_properties";
        static final String LOG_TABLE_NAME = "iceberg_log";
        static final String USER_TABLE_NAME = "iceberg_auth";
        static final String NAMESPACE_NAME = "namespace";
        static final String NAMESPACE_PROPERTY_KEY = "property_key";
        static final String NAMESPACE_PROPERTY_VALUE = "property_value";

        static final String CREATE_NAMESPACE_PROPERTIES_TABLE = "CREATE TABLE "
                        + NAMESPACE_PROPERTIES_TABLE_NAME
                        + "("
                        + CATALOG_NAME
                        + " VARCHAR(255) NOT NULL,"
                        + NAMESPACE_NAME
                        + " VARCHAR(255) NOT NULL,"
                        + NAMESPACE_PROPERTY_KEY
                        + " VARCHAR(255),"
                        + NAMESPACE_PROPERTY_VALUE
                        + " VARCHAR(1000),"
                        + "PRIMARY KEY ("
                        + CATALOG_NAME
                        + ", "
                        + NAMESPACE_NAME
                        + ", "
                        + NAMESPACE_PROPERTY_KEY
                        + ")"
                        + ")";

        static final String CREATE_LOG_TABLE = "CREATE TABLE "
                        + LOG_TABLE_NAME
                        + "("
                        + CATALOG_NAME
                        + " VARCHAR(255) NOT NULL,"
                        + TABLE_NAMESPACE
                        + " VARCHAR(255) NOT NULL,"
                        + TABLE_NAME
                        + " VARCHAR(255) NOT NULL,"
                        + "action"
                        + " VARCHAR(255) NOT NULL,"
                        + "created_at"
                        + " timestamp NOT NULL,"
                        + "user_id"
                        + " VARCHAR(255), "
                        + "PRIMARY KEY ("
                        + CATALOG_NAME
                        + ", "
                        + TABLE_NAMESPACE
                        + ", "
                        + TABLE_NAME
                        + ", "
                        + "created_at"
                        + ")"
                        + ")";

        static final String CREATE_USER_TABLE = "CREATE TABLE "
                        + USER_TABLE_NAME
                        + "("
                        + "user_id VARCHAR(255) NOT NULL,"
                        + "digest_password VARCHAR(255) NOT NULL,"
                        + "PRIMARY KEY("
                        + "user_id"
                        + ")"
                        + ")";

        static final String GET_USER = "SELECT "
                        + "digest_password "
                        + "FROM "
                        + USER_TABLE_NAME
                        + " WHERE "
                        + " user_id = ?";

        static final String INSERT_USER = "INSERT INTO "
                        + USER_TABLE_NAME
                        + "("
                        + "user_id"
                        + ", "
                        + "digest_password"
                        + ")"
                        + " VALUES";

        static final String INSERT_USER_VALUES_BASE = "(?, ?)";

        static final String INSERT_LOG = "INSERT INTO "
                        + LOG_TABLE_NAME
                        + "("
                        + CATALOG_NAME
                        + ", "
                        + TABLE_NAMESPACE
                        + ", "
                        + TABLE_NAME
                        + ", "
                        + "action"
                        + ", "
                        + "created_at"
                        + ", "
                        + "user_id"
                        + ") VALUES";

        static final String INSERT_LOG_VALUES_BASE = "(?, ?, ?, ?, ?, ?)";

        static final String GET_NAMESPACE_PROPERTIES_SQL = "SELECT "
                        + NAMESPACE_NAME
                        + " FROM "
                        + NAMESPACE_PROPERTIES_TABLE_NAME
                        + " WHERE "
                        + CATALOG_NAME
                        + " = ? AND "
                        + " ( "
                        + NAMESPACE_NAME
                        + " = ? OR "
                        + NAMESPACE_NAME
                        + " LIKE ? ESCAPE '\\' "
                        + " ) ";
        static final String INSERT_NAMESPACE_PROPERTIES_SQL = "INSERT INTO "
                        + NAMESPACE_PROPERTIES_TABLE_NAME
                        + " ("
                        + CATALOG_NAME
                        + ", "
                        + NAMESPACE_NAME
                        + ", "
                        + NAMESPACE_PROPERTY_KEY
                        + ", "
                        + NAMESPACE_PROPERTY_VALUE
                        + ") VALUES ";
        static final String INSERT_PROPERTIES_VALUES_BASE = "(?,?,?,?)";
        static final String GET_ALL_NAMESPACE_PROPERTIES_SQL = "SELECT * "
                        + " FROM "
                        + NAMESPACE_PROPERTIES_TABLE_NAME
                        + " WHERE "
                        + CATALOG_NAME
                        + " = ? AND "
                        + NAMESPACE_NAME
                        + " = ? ";
        static final String DELETE_NAMESPACE_PROPERTIES_SQL = "DELETE FROM "
                        + NAMESPACE_PROPERTIES_TABLE_NAME
                        + " WHERE "
                        + CATALOG_NAME
                        + " = ? AND "
                        + NAMESPACE_NAME
                        + " = ? AND "
                        + NAMESPACE_PROPERTY_KEY
                        + " IN ";
        static final String DELETE_ALL_NAMESPACE_PROPERTIES_SQL = "DELETE FROM "
                        + NAMESPACE_PROPERTIES_TABLE_NAME
                        + " WHERE "
                        + CATALOG_NAME
                        + " = ? AND "
                        + NAMESPACE_NAME
                        + " = ?";
        static final String LIST_PROPERTY_NAMESPACES_SQL = "SELECT DISTINCT "
                        + NAMESPACE_NAME
                        + " FROM "
                        + NAMESPACE_PROPERTIES_TABLE_NAME
                        + " WHERE "
                        + CATALOG_NAME
                        + " = ? AND "
                        + NAMESPACE_NAME
                        + " LIKE ?";
        static final String LIST_ALL_PROPERTY_NAMESPACES_SQL = "SELECT DISTINCT "
                        + NAMESPACE_NAME
                        + " FROM "
                        + NAMESPACE_PROPERTIES_TABLE_NAME
                        + " WHERE "
                        + CATALOG_NAME
                        + " = ?";

        // Utilities
        private static final Joiner JOINER_DOT = Joiner.on('.');
        private static final Splitter SPLITTER_DOT = Splitter.on('.');

        private JdbcUtilV2() {
        }

        public static Namespace stringToNamespace(String namespace) {
                Preconditions.checkArgument(namespace != null, "Invalid namespace %s", namespace);
                return Namespace.of(Iterables.toArray(SPLITTER_DOT.split(namespace), String.class));
        }

        public static String namespaceToString(Namespace namespace) {
                return JOINER_DOT.join(namespace.levels());
        }

        public static TableIdentifier stringToTableIdentifier(String tableNamespace, String tableName) {
                return TableIdentifier.of(JdbcUtil.stringToNamespace(tableNamespace), tableName);
        }

        public static Properties filterAndRemovePrefix(Map<String, String> properties, String prefix) {
                Properties result = new Properties();
                properties.forEach(
                                (key, value) -> {
                                        if (key.startsWith(prefix)) {
                                                result.put(key.substring(prefix.length()), value);
                                        }
                                });

                return result;
        }

        public static String updatePropertiesStatement(int size) {
                StringBuilder sqlStatement = new StringBuilder(
                                "UPDATE "
                                                + NAMESPACE_PROPERTIES_TABLE_NAME
                                                + " SET "
                                                + NAMESPACE_PROPERTY_VALUE
                                                + " = CASE");
                for (int i = 0; i < size; i += 1) {
                        sqlStatement.append(" WHEN " + NAMESPACE_PROPERTY_KEY + " = ? THEN ?");
                }
                sqlStatement.append(
                                " END WHERE "
                                                + CATALOG_NAME
                                                + " = ? AND "
                                                + NAMESPACE_NAME
                                                + " = ? AND "
                                                + NAMESPACE_PROPERTY_KEY
                                                + " IN ");

                String values = String.join(",", Collections.nCopies(size, String.valueOf('?')));
                sqlStatement.append("(").append(values).append(")");

                return sqlStatement.toString();
        }

        public static String insertPropertiesStatement(int size) {
                StringBuilder sqlStatement = new StringBuilder(JdbcUtil.INSERT_NAMESPACE_PROPERTIES_SQL);

                for (int i = 0; i < size; i++) {
                        if (i != 0) {
                                sqlStatement.append(", ");
                        }
                        sqlStatement.append(JdbcUtil.INSERT_PROPERTIES_VALUES_BASE);
                }

                return sqlStatement.toString();
        }

        public static String insertUserStatement() {
                StringBuilder sqlStatement = new StringBuilder(INSERT_USER);
                sqlStatement.append(INSERT_USER_VALUES_BASE);
                return sqlStatement.toString();
        }

        public static String insertLogStatement() {
                StringBuilder sqlStatement = new StringBuilder(INSERT_LOG);
                sqlStatement.append(INSERT_LOG_VALUES_BASE);
                return sqlStatement.toString();
        }

        public static String deletePropertiesStatement(Set<String> properties) {
                StringBuilder sqlStatement = new StringBuilder(JdbcUtil.DELETE_NAMESPACE_PROPERTIES_SQL);
                String values = String.join(",", Collections.nCopies(properties.size(), String.valueOf('?')));
                sqlStatement.append("(").append(values).append(")");

                return sqlStatement.toString();
        }

        static boolean namespaceExists(
                        String catalogName, JdbcClientPool connections, Namespace namespace) {

                String namespaceEquals = JdbcUtil.namespaceToString(namespace);
                // when namespace has sub-namespace then additionally checking it with LIKE
                // statement.
                // catalog.db can exists as: catalog.db.ns1 or catalog.db.ns1.ns2
                String namespaceStartsWith = namespaceEquals.replace("\\", "\\\\").replace("_", "\\_").replace("%",
                                "\\%")
                                + ".%";
                if (exists(
                                connections,
                                JdbcUtil.GET_NAMESPACE_SQL,
                                catalogName,
                                namespaceEquals,
                                namespaceStartsWith)) {
                        return true;
                }

                if (exists(
                                connections,
                                JdbcUtil.GET_NAMESPACE_PROPERTIES_SQL,
                                catalogName,
                                namespaceEquals,
                                namespaceStartsWith)) {
                        return true;
                }

                return false;
        }

        public static void addLog(JdbcClientPool connections, String catalogName, String tableNamespace, String table,
                        String userId, String action, Date now) {
                String sql = insertLogStatement();
                try {
                        Boolean res = connections.run(
                                        conn -> {
                                                try (PreparedStatement preparedStatement = conn
                                                                .prepareStatement(sql)) {
                                                        preparedStatement.setString(1, catalogName);
                                                        preparedStatement.setString(2, tableNamespace);
                                                        preparedStatement.setString(3, table);
                                                        preparedStatement.setString(4, action);
                                                        preparedStatement.setDate(5, new java.sql.Date(now.getTime()));
                                                        preparedStatement.setString(6, userId);
                                                        preparedStatement.executeUpdate();
                                                        return true;
                                                }
                                        });
                        if (!res) {
                                throw new RuntimeException("could not create user");
                        }
                } catch (SQLException e) {
                        throw new UncheckedSQLException(e, "Failed to execute exists query: %s", sql);
                } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new UncheckedInterruptedException(e, "Interrupted in SQL query");
                }
        }

        public static void addUser(JdbcClientPool connections, String userId, String password) {
                String sql = insertUserStatement();
                String checkSql = GET_USER;
                try {
                        connections.run(
                                        conn -> {
                                                boolean matchPassword = true;
                                                boolean foundRow = false;
                                                try (PreparedStatement preparedStatement = conn
                                                                .prepareStatement(checkSql)) {
                                                        preparedStatement.setString(1, userId);
                                                        try (ResultSet rs = preparedStatement.executeQuery()) {
                                                                if (rs.next()) {
                                                                        foundRow = true;
                                                                        String hash = rs.getString(1);
                                                                        matchPassword = PasswordUtils
                                                                                        .verifyHash(password, hash);
                                                                        if (!matchPassword) {
                                                                                LOG.info("found user, password does not match");
                                                                        } else {
                                                                                LOG.info("found user, password matches");
                                                                        }
                                                                } else {
                                                                        foundRow = false;
                                                                }
                                                        }
                                                }
                                                if (!foundRow) {
                                                        try (PreparedStatement preparedStatement = conn
                                                                        .prepareStatement(sql)) {
                                                                preparedStatement.setString(1, userId);
                                                                preparedStatement.setString(2,
                                                                                PasswordUtils.hash(password));
                                                                preparedStatement.executeUpdate();
                                                                return true;
                                                        }
                                                }
                                                return false;
                                        });
                } catch (SQLException e) {
                        throw new UncheckedSQLException(e, "Failed to execute exists query: %s", sql);
                } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new UncheckedInterruptedException(e, "Interrupted in SQL query");
                }
        }

        public static String getPasswordHash(JdbcClientPool connections, String userId) {
                String sql = GET_USER;
                try {
                        return connections.run(
                                        conn -> {
                                                try (PreparedStatement preparedStatement = conn
                                                                .prepareStatement(sql)) {
                                                        preparedStatement.setString(1, userId);
                                                        try (ResultSet rs = preparedStatement.executeQuery()) {
                                                                if (rs.next()) {
                                                                        String hash = rs.getString(1);
                                                                        return hash;
                                                                } else {
                                                                        throw new RuntimeException(
                                                                                        "could not found user");
                                                                }
                                                        }
                                                }
                                        });
                } catch (SQLException e) {
                        throw new UncheckedSQLException(e, "Failed to execute exists query: %s", sql);
                } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new UncheckedInterruptedException(e, "Interrupted in SQL query");
                }
        }

        public static boolean checkUser(JdbcClientPool connections, String userId, String password) {
                String sql = GET_USER;
                try {
                        return connections.run(
                                        conn -> {
                                                try (PreparedStatement preparedStatement = conn
                                                                .prepareStatement(sql)) {
                                                        preparedStatement.setString(1, userId);
                                                        try (ResultSet rs = preparedStatement.executeQuery()) {
                                                                if (rs.next()) {
                                                                        String hash = rs.getString(1);
                                                                        try {
                                                                                return PasswordUtils.verifyHash(
                                                                                                password, hash);
                                                                        } catch (IllegalArgumentException exc) {
                                                                                LOG.warn("got error while check password: {}",
                                                                                                exc);
                                                                                return false;

                                                                        }
                                                                } else {
                                                                        return false;
                                                                }
                                                        }
                                                }
                                        });
                } catch (SQLException e) {
                        throw new UncheckedSQLException(e, "Failed to execute exists query: %s", sql);
                } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new UncheckedInterruptedException(e, "Interrupted in SQL query");
                }
        }

        @SuppressWarnings("checkstyle:NestedTryDepth")
        private static boolean exists(JdbcClientPool connections, String sql, String... args) {
                try {
                        return connections.run(
                                        conn -> {
                                                try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                                                        for (int pos = 0; pos < args.length; pos += 1) {
                                                                preparedStatement.setString(pos + 1, args[pos]);
                                                        }

                                                        try (ResultSet rs = preparedStatement.executeQuery()) {
                                                                if (rs.next()) {
                                                                        return true;
                                                                }
                                                        }
                                                }

                                                return false;
                                        });
                } catch (SQLException e) {
                        throw new UncheckedSQLException(e, "Failed to execute exists query: %s", sql);
                } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new UncheckedInterruptedException(e, "Interrupted in SQL query");
                }
        }
}