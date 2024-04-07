/*
 * Copyright 2024 Tabular Technologies Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.rest;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.core.CheckUser;
import org.apache.iceberg.util.PropertyUtil;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RESTCatalogServer {
  private static final Logger LOG = LoggerFactory.getLogger(RESTCatalogServer.class);
  private static final Logger LOG_ACCESS = LoggerFactory.getLogger("access");
  private static final String CATALOG_ENV_PREFIX = "CATALOG_";

  private RESTCatalogServer() {
  }

  record CatalogContext(Catalog catalog, Map<String, String> configuration) {
  }

  private static Map<String, String> loadCatalogProperties() {
    return System.getenv().entrySet().stream()
        .filter(e -> e.getKey().startsWith(CATALOG_ENV_PREFIX))
        .collect(
            Collectors.toMap(
                e -> e.getKey()
                    .replaceFirst(CATALOG_ENV_PREFIX, "")
                    .replaceAll("__", "-")
                    .replaceAll("_", ".")
                    .toLowerCase(Locale.ROOT),
                Map.Entry::getValue,
                (m1, m2) -> {
                  throw new IllegalArgumentException("Duplicate key: " + m1);
                },
                HashMap::new));
  }

  private static CatalogContext backendCatalog() throws IOException {
    // Translate environment variable to catalog properties
    Map<String, String> catalogProperties = loadCatalogProperties();
    catalogProperties.put(
        CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.jdbc.JdbcCatalogV2");
    catalogProperties.putIfAbsent(
        CatalogProperties.URI, "jdbc:sqlite:file:/tmp/iceberg_rest_mode=memory");

    // Configure a default location if one is not specified
    String warehouseLocation = catalogProperties.get(CatalogProperties.WAREHOUSE_LOCATION);

    if (warehouseLocation == null) {
      File tmp = java.nio.file.Files.createTempDirectory("iceberg_warehouse").toFile();
      tmp.deleteOnExit();
      warehouseLocation = tmp.toPath().resolve("iceberg_data").toFile().getAbsolutePath();
      catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);

      LOG.info("No warehouse location set.  Defaulting to temp location: {}", warehouseLocation);
    }

    LOG.info("Creating catalog with properties: {}", catalogProperties);
    return new CatalogContext(CatalogUtil.buildIcebergCatalog("rest_backend", catalogProperties, new Configuration()),
        catalogProperties);
  }

  public static void main(String[] args) throws Exception {
    CatalogContext catalogContext = backendCatalog();

    boolean exitAfter = false;

    // adding users from arguments
    for (String argv : args) {
      if (argv == "exit") {
        exitAfter = true;
        continue;
      }
      if (catalogContext.catalog() instanceof CheckUser) {
        String[] split = argv.split(":");
        LOG.info("add user: {}", split[0]);
        ((CheckUser) catalogContext.catalog()).addUser(split[0], split[1]);
      } else {
        throw new RuntimeException("could not add user");
      }
    }

    if (exitAfter) {
      System.exit(0);
    }

    try (RESTServerCatalogAdapter adapter = new RESTServerCatalogAdapter(catalogContext)) {
      RestCatalogServletV2 servlet = new RestCatalogServletV2(adapter);

      ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
      context.setContextPath("/");
      ServletHolder servletHolder = new ServletHolder(servlet);
      servletHolder.setInitParameter("javax.ws.rs.Application", "ServiceListPublic");
      context.addServlet(servletHolder, "/*");
      // context.addLi
      context.setVirtualHosts(null);
      context.setGzipHandler(new GzipHandler());

      Server httpServer = new Server(PropertyUtil.propertyAsInt(System.getenv(), "REST_PORT", 8181));
      httpServer.setHandler(context);
      httpServer.setRequestLog(new RequestLog() {

        @Override
        public void log(Request arg0, Response arg1) {
          long request_ts = arg0.getTimeStamp();
          long now = System.currentTimeMillis();
          String headers = join(arg0.getHeaderNames(), ";");
          RESTCatalogServer.LOG_ACCESS.info("uri: " + arg0.getRequestURI() + ", method: " + arg0.getMethod() + ", ip: "
              + arg0.getRemoteAddr().toString()
              + ", resp status: " + arg1.getStatus() + ", duration(ms): " + Long.toString(now - request_ts)
              + ", input headers: " + headers + ", auth: '" + arg0.getHeader("Authorization") + "'");
        }

      });

      httpServer.start();
      httpServer.join();
    }
  }

  public static String join(Enumeration<String> enumeration, String s) {
    StringBuffer stringbuffer = new StringBuffer();
    while (enumeration.hasMoreElements()) {
      String s1 = (String) enumeration.nextElement();
      stringbuffer.append(s1);
      if (enumeration.hasMoreElements()) {
        stringbuffer.append(s);
      }
    }
    return stringbuffer.toString();
  }

}
