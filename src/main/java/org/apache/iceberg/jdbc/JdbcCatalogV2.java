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

import java.io.Closeable;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.core.CheckDb;
import org.apache.iceberg.core.CheckUser;
import org.apache.iceberg.core.Loggable;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.LocationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
    It is copy-paste from source:
    https://github.com/apache/iceberg/blob/apache-iceberg-1.4.3/core/src/main/java/org/apache/iceberg/jdbc/JdbcCatalog.java
    
    With Small changes
    
*/
public class JdbcCatalogV2 extends BaseMetastoreCatalog
        implements Configurable<Object>, SupportsNamespaces, Closeable, CheckUser, Loggable, CheckDb {

    public static final String PROPERTY_PREFIX = "jdbc.";
    private static final String NAMESPACE_EXISTS_PROPERTY = "exists";
    private static final Logger LOG = LoggerFactory.getLogger(JdbcCatalog.class);
    private static final Joiner SLASH = Joiner.on("/");

    private FileIO io;
    private String catalogName = "jdbc";
    private String warehouseLocation;
    private Object conf;
    private JdbcClientPool connections;
    private Map<String, String> catalogProperties;
    private final Function<Map<String, String>, FileIO> ioBuilder;
    private final Function<Map<String, String>, JdbcClientPool> clientPoolBuilder;
    private final boolean initializeCatalogTables;

    private boolean authRequired = true;

    public JdbcCatalogV2() {
        this(null, null, true);
    }

    public JdbcCatalogV2(
            Function<Map<String, String>, FileIO> ioBuilder,
            Function<Map<String, String>, JdbcClientPool> clientPoolBuilder,
            boolean initializeCatalogTables) {
        this.ioBuilder = ioBuilder;
        this.clientPoolBuilder = clientPoolBuilder;
        this.initializeCatalogTables = initializeCatalogTables;
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
        Preconditions.checkNotNull(properties, "Invalid catalog properties: null");
        String uri = properties.get(CatalogProperties.URI);
        Preconditions.checkNotNull(uri, "JDBC connection URI is required");

        String inputWarehouseLocation = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
        Preconditions.checkArgument(
                inputWarehouseLocation != null && inputWarehouseLocation.length() > 0,
                "Cannot initialize JDBCCatalog because warehousePath must not be null or empty");

        this.warehouseLocation = LocationUtil.stripTrailingSlash(inputWarehouseLocation);
        this.catalogProperties = ImmutableMap.copyOf(properties);

        if (name != null) {
            this.catalogName = name;
        }

        if (null != ioBuilder) {
            this.io = ioBuilder.apply(properties);
        } else {
            String ioImpl = properties.getOrDefault(
                    CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.hadoop.HadoopFileIO");
            this.io = CatalogUtil.loadFileIO(ioImpl, properties, conf);
        }

        LOG.debug("Connecting to JDBC database {}", uri);
        if (null != clientPoolBuilder) {
            this.connections = clientPoolBuilder.apply(properties);
        } else {
            this.connections = new JdbcClientPool(uri, properties);
        }

        Object value = properties.get("AUTH_REQUIRED");
        this.authRequired = value == null || value == "1" || value == "true" || value == "t";

        LOG.info("auth required: {}", this.authRequired);

        try {
            if (initializeCatalogTables) {
                initializeCatalogTables();
            }
        } catch (SQLTimeoutException e) {
            throw new UncheckedSQLException(e, "Cannot initialize JDBC catalog: Query timed out");
        } catch (SQLTransientConnectionException | SQLNonTransientConnectionException e) {
            throw new UncheckedSQLException(e, "Cannot initialize JDBC catalog: Connection failed");
        } catch (SQLException e) {
            throw new UncheckedSQLException(e, "Cannot initialize JDBC catalog");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedInterruptedException(e, "Interrupted in call to initialize");
        }
    }

    private void createTable(String tableName, String ddlCreateTable) throws InterruptedException, SQLException {
        connections.run(
                conn -> {
                    DatabaseMetaData dbMeta = conn.getMetaData();
                    ResultSet tableExists = dbMeta.getTables(
                            null /* catalog name */,
                            null /* schemaPattern */,
                            tableName /* tableNamePattern */,
                            null /* types */);

                    if (tableExists.next()) {
                        return true;
                    }

                    LOG.info(
                            "Creating table {} to store iceberg info",
                            tableName);
                    LOG.info("create table: {}", ddlCreateTable);
                    return conn.prepareStatement(ddlCreateTable).execute();
                });
    }

    private void initializeCatalogTables() throws InterruptedException, SQLException {
        LOG.info("Creating database tables (if missing) to store iceberg catalog");
        createTable(JdbcUtilV2.CATALOG_TABLE_NAME, JdbcUtilV2.CREATE_CATALOG_TABLE);
        createTable(JdbcUtilV2.NAMESPACE_PROPERTIES_TABLE_NAME, JdbcUtilV2.CREATE_NAMESPACE_PROPERTIES_TABLE);
        createTable(JdbcUtilV2.LOG_TABLE_NAME, JdbcUtilV2.CREATE_LOG_TABLE);
        createTable(JdbcUtilV2.USER_TABLE_NAME, JdbcUtilV2.CREATE_USER_TABLE);
    }

    public void addUser(String userId, String password) {
        JdbcUtilV2.addUser(this.connections, userId, password);
    }

    public boolean checkConnection() {
        try {
            return connections.run(
                    conn -> {
                        return !conn.isClosed();
                    });
        } catch (SQLException | InterruptedException e) {
            LOG.error("got error on check connection", e);
            return false;
        }
    }

    public boolean isAuthRequired() {
        return this.authRequired;
    }

    public boolean checkUser(String userId, String password) {
        return JdbcUtilV2.checkUser(connections, userId, password);
    }

    public String getPasswordHash(String userId) {
        LOG.info(String.format("checking password for: %s", userId));
        return JdbcUtilV2.getPasswordHash(connections, userId);
    }

    public void addLog(TableIdentifier tableIdentifier, String userId, String action, Date now) {
        Namespace namespace = tableIdentifier.namespace();
        JdbcUtilV2.addLog(this.connections, this.catalogName, JdbcUtilV2.namespaceToString(namespace),
                tableIdentifier.name(), userId, action, now);
    }

    public void addLog(TableIdentifier tableIdentifier, String userId, String action) {
        addLog(tableIdentifier, userId, action, new Date());
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
        return new JdbcTableOperations(
                connections, io, catalogName, tableIdentifier, catalogProperties);
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier table) {
        return SLASH.join(defaultNamespaceLocation(table.namespace()), table.name());
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
        TableOperations ops = newTableOps(identifier);
        TableMetadata lastMetadata = null;
        if (purge) {
            try {
                lastMetadata = ops.current();
            } catch (NotFoundException e) {
                LOG.warn(
                        "Failed to load table metadata for table: {}, continuing drop without purge",
                        identifier,
                        e);
            }
        }

        int deletedRecords = execute(
                JdbcUtilV2.DROP_TABLE_SQL,
                catalogName,
                JdbcUtilV2.namespaceToString(identifier.namespace()),
                identifier.name());

        if (deletedRecords == 0) {
            LOG.info("Skipping drop, table does not exist: {}", identifier);
            return false;
        }

        if (purge && lastMetadata != null) {
            CatalogUtil.dropTableData(ops.io(), lastMetadata);
        }

        LOG.info("Dropped table: {}", identifier);
        return true;
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
        if (!namespaceExists(namespace)) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }

        return fetch(
                row -> JdbcUtilV2.stringToTableIdentifier(
                        row.getString(JdbcUtilV2.TABLE_NAMESPACE), row.getString(JdbcUtilV2.TABLE_NAME)),
                JdbcUtilV2.LIST_TABLES_SQL,
                catalogName,
                JdbcUtilV2.namespaceToString(namespace));
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
        int updatedRecords = execute(
                err -> {
                    // SQLite doesn't set SQLState or throw SQLIntegrityConstraintViolationException
                    if (err instanceof SQLIntegrityConstraintViolationException
                            || (err.getMessage() != null && err.getMessage().contains("constraint failed"))) {
                        throw new AlreadyExistsException("Table already exists: %s", to);
                    }
                },
                JdbcUtilV2.RENAME_TABLE_SQL,
                JdbcUtilV2.namespaceToString(to.namespace()),
                to.name(),
                catalogName,
                JdbcUtilV2.namespaceToString(from.namespace()),
                from.name());

        if (updatedRecords == 1) {
            LOG.info("Renamed table from {}, to {}", from, to);
        } else if (updatedRecords == 0) {
            throw new NoSuchTableException("Table does not exist: %s", from);
        } else {
            LOG.warn(
                    "Rename operation affected {} rows: the catalog table's primary key assumption has been violated",
                    updatedRecords);
        }
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public void setConf(Object conf) {
        this.conf = conf;
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> metadata) {
        if (namespaceExists(namespace)) {
            throw new AlreadyExistsException("Namespace already exists: %s", namespace);
        }

        Map<String, String> createMetadata;
        if (metadata == null || metadata.isEmpty()) {
            createMetadata = ImmutableMap.of(NAMESPACE_EXISTS_PROPERTY, "true");
        } else {
            createMetadata = ImmutableMap.<String, String>builder()
                    .putAll(metadata)
                    .put(NAMESPACE_EXISTS_PROPERTY, "true")
                    .buildOrThrow();
        }

        insertProperties(namespace, createMetadata);
    }

    @Override
    public List<Namespace> listNamespaces() {
        List<Namespace> namespaces = Lists.newArrayList();
        namespaces.addAll(
                fetch(
                        row -> JdbcUtilV2.stringToNamespace(row.getString(JdbcUtilV2.TABLE_NAMESPACE)),
                        JdbcUtilV2.LIST_ALL_TABLE_NAMESPACES_SQL,
                        catalogName));
        namespaces.addAll(
                fetch(
                        row -> JdbcUtilV2.stringToNamespace(row.getString(JdbcUtilV2.NAMESPACE_NAME)),
                        JdbcUtilV2.LIST_ALL_PROPERTY_NAMESPACES_SQL,
                        catalogName));

        namespaces = namespaces.stream()
                // only get sub namespaces/children
                .filter(n -> n.levels().length >= 1)
                // only get sub namespaces/children
                .map(n -> Namespace.of(Arrays.stream(n.levels()).limit(1).toArray(String[]::new)))
                // remove duplicates
                .distinct()
                .collect(Collectors.toList());

        return namespaces;
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
        if (namespace.isEmpty()) {
            return listNamespaces();
        }

        if (!namespaceExists(namespace)) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }

        List<Namespace> namespaces = Lists.newArrayList();
        namespaces.addAll(
                fetch(
                        row -> JdbcUtilV2.stringToNamespace(row.getString(JdbcUtilV2.TABLE_NAMESPACE)),
                        JdbcUtilV2.LIST_NAMESPACES_SQL,
                        catalogName,
                        JdbcUtilV2.namespaceToString(namespace) + "%"));
        namespaces.addAll(
                fetch(
                        row -> JdbcUtilV2.stringToNamespace(row.getString(JdbcUtilV2.NAMESPACE_NAME)),
                        JdbcUtilV2.LIST_PROPERTY_NAMESPACES_SQL,
                        catalogName,
                        JdbcUtilV2.namespaceToString(namespace) + "%"));

        int subNamespaceLevelLength = namespace.levels().length + 1;
        namespaces = namespaces.stream()
                // exclude itself
                .filter(n -> !n.equals(namespace))
                // only get sub namespaces/children
                .filter(n -> n.levels().length >= subNamespaceLevelLength)
                // only get sub namespaces/children
                .map(
                        n -> Namespace.of(
                                Arrays.stream(n.levels())
                                        .limit(subNamespaceLevelLength)
                                        .toArray(String[]::new)))
                // remove duplicates
                .distinct()
                .collect(Collectors.toList());

        return namespaces;
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace)
            throws NoSuchNamespaceException {
        if (!namespaceExists(namespace)) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }

        Map<String, String> properties = Maps.newHashMap();
        properties.putAll(fetchProperties(namespace));
        if (!properties.containsKey("location")) {
            properties.put("location", defaultNamespaceLocation(namespace));
        }
        properties.remove(NAMESPACE_EXISTS_PROPERTY); // do not return reserved existence property

        return ImmutableMap.copyOf(properties);
    }

    private String defaultNamespaceLocation(Namespace namespace) {
        if (namespace.isEmpty()) {
            return warehouseLocation;
        } else {
            return SLASH.join(warehouseLocation, SLASH.join(namespace.levels()));
        }
    }

    @Override
    public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
        if (!namespaceExists(namespace)) {
            return false;
        }

        List<TableIdentifier> tableIdentifiers = listTables(namespace);
        if (tableIdentifiers != null && !tableIdentifiers.isEmpty()) {
            throw new NamespaceNotEmptyException(
                    "Namespace %s is not empty. %s tables exist.", namespace, tableIdentifiers.size());
        }

        int deletedRows = execute(
                JdbcUtilV2.DELETE_ALL_NAMESPACE_PROPERTIES_SQL,
                catalogName,
                JdbcUtilV2.namespaceToString(namespace));

        return deletedRows > 0;
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> properties)
            throws NoSuchNamespaceException {
        if (!namespaceExists(namespace)) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }

        Preconditions.checkNotNull(properties, "Invalid properties to set: null");

        if (properties.isEmpty()) {
            return false;
        }

        Preconditions.checkArgument(
                !properties.containsKey(NAMESPACE_EXISTS_PROPERTY),
                "Cannot set reserved property: %s",
                NAMESPACE_EXISTS_PROPERTY);

        Map<String, String> startingProperties = fetchProperties(namespace);
        Map<String, String> inserts = Maps.newHashMap();
        Map<String, String> updates = Maps.newHashMap();

        for (String key : properties.keySet()) {
            String value = properties.get(key);
            if (startingProperties.containsKey(key)) {
                updates.put(key, value);
            } else {
                inserts.put(key, value);
            }
        }

        boolean hadInserts = false;
        if (!inserts.isEmpty()) {
            hadInserts = insertProperties(namespace, inserts);
        }

        boolean hadUpdates = false;
        if (!updates.isEmpty()) {
            hadUpdates = updateProperties(namespace, updates);
        }

        return hadInserts || hadUpdates;
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> properties)
            throws NoSuchNamespaceException {
        if (!namespaceExists(namespace)) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }

        Preconditions.checkNotNull(properties, "Invalid properties to remove: null");

        if (properties.isEmpty()) {
            return false;
        }

        return deleteProperties(namespace, properties);
    }

    @Override
    public void close() {
        connections.close();
    }

    @Override
    public boolean namespaceExists(Namespace namespace) {
        return JdbcUtilV2.namespaceExists(catalogName, connections, namespace);
    }

    private int execute(String sql, String... args) {
        return execute(err -> {
        }, sql, args);
    }

    private int execute(Consumer<SQLException> sqlErrorHandler, String sql, String... args) {
        try {
            return connections.run(
                    conn -> {
                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                            for (int pos = 0; pos < args.length; pos += 1) {
                                preparedStatement.setString(pos + 1, args[pos]);
                            }

                            return preparedStatement.executeUpdate();
                        }
                    });
        } catch (SQLException e) {
            sqlErrorHandler.accept(e);
            throw new UncheckedSQLException(e, "Failed to execute: %s", sql);
        } catch (InterruptedException e) {
            throw new UncheckedInterruptedException(e, "Interrupted in SQL command");
        }
    }

    @FunctionalInterface
    interface RowProducer<R> {
        R apply(ResultSet result) throws SQLException;
    }

    @SuppressWarnings("checkstyle:NestedTryDepth")
    private <R> List<R> fetch(RowProducer<R> toRow, String sql, String... args) {
        try {
            return connections.run(
                    conn -> {
                        List<R> result = Lists.newArrayList();

                        try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
                            for (int pos = 0; pos < args.length; pos += 1) {
                                preparedStatement.setString(pos + 1, args[pos]);
                            }

                            try (ResultSet rs = preparedStatement.executeQuery()) {
                                while (rs.next()) {
                                    result.add(toRow.apply(rs));
                                }
                            }
                        }

                        return result;
                    });
        } catch (SQLException e) {
            throw new UncheckedSQLException(e, "Failed to execute query: %s", sql);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedInterruptedException(e, "Interrupted in SQL query");
        }
    }

    private Map<String, String> fetchProperties(Namespace namespace) {
        if (!namespaceExists(namespace)) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }

        String namespaceName = JdbcUtilV2.namespaceToString(namespace);

        List<Map.Entry<String, String>> entries = fetch(
                row -> new AbstractMap.SimpleImmutableEntry<>(
                        row.getString(JdbcUtilV2.NAMESPACE_PROPERTY_KEY),
                        row.getString(JdbcUtilV2.NAMESPACE_PROPERTY_VALUE)),
                JdbcUtilV2.GET_ALL_NAMESPACE_PROPERTIES_SQL,
                catalogName,
                namespaceName);

        return ImmutableMap.<String, String>builder().putAll(entries).buildOrThrow();
    }

    private boolean insertProperties(Namespace namespace, Map<String, String> properties) {
        String namespaceName = JdbcUtilV2.namespaceToString(namespace);
        String[] args = properties.entrySet().stream()
                .flatMap(
                        entry -> Stream.of(catalogName, namespaceName, entry.getKey(), entry.getValue()))
                .toArray(String[]::new);

        int insertedRecords = execute(JdbcUtilV2.insertPropertiesStatement(properties.size()), args);

        if (insertedRecords == properties.size()) {
            return true;
        }

        throw new IllegalStateException(
                String.format("Failed to insert: %d of %d succeeded", insertedRecords, properties.size()));
    }

    private boolean updateProperties(Namespace namespace, Map<String, String> properties) {
        String namespaceName = JdbcUtilV2.namespaceToString(namespace);
        Stream<String> caseArgs = properties.entrySet().stream()
                .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()));
        Stream<String> whereArgs = Stream.concat(Stream.of(catalogName, namespaceName), properties.keySet().stream());

        String[] args = Stream.concat(caseArgs, whereArgs).toArray(String[]::new);

        int updatedRecords = execute(JdbcUtilV2.updatePropertiesStatement(properties.size()), args);

        if (updatedRecords == properties.size()) {
            return true;
        }

        throw new IllegalStateException(
                String.format("Failed to update: %d of %d succeeded", updatedRecords, properties.size()));
    }

    private boolean deleteProperties(Namespace namespace, Set<String> properties) {
        String namespaceName = JdbcUtilV2.namespaceToString(namespace);
        String[] args = Stream.concat(Stream.of(catalogName, namespaceName), properties.stream())
                .toArray(String[]::new);

        return execute(JdbcUtilV2.deletePropertiesStatement(properties), args) > 0;
    }

    @Override
    protected Map<String, String> properties() {
        return catalogProperties == null ? ImmutableMap.of() : catalogProperties;
    }
}