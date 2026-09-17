/**
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
package org.apache.pinot.core.data.manager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.helix.FakePropertyStore;
import org.apache.pinot.core.data.manager.offline.OfflineTableDataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TimestampConfig;
import org.apache.pinot.spi.config.table.TimestampIndexGranularity;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Verifies schema reuse through the table manager's existing cache without hiding schema or index-config changes.
public class BaseTableDataManagerSchemaReuseTest {
  @Test
  public void testReuseAndSchemaRefresh() {
    TableConfig table = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    Schema original = schema();
    BaseTableDataManager manager = manager(table, original);
    manager.updateCachedTableConfigAndSchema(table, original);
    IndexLoadingConfig first = manager.fetchIndexLoadingConfig();
    IndexLoadingConfig second = manager.fetchIndexLoadingConfig();
    assertSame(first.getSchema(), original);
    assertSame(second.getSchema(), original);
    assertNotSame(first.getTableConfig(), second.getTableConfig());

    Schema changed = schema();
    changed.getFieldSpecFor("id").setDefaultNullValue(-2);
    ZKMetadataProvider.setSchema(manager._propertyStore, changed);
    Schema refreshed = manager.fetchIndexLoadingConfig().getSchema();
    assertNotSame(refreshed, original);
    assertEquals(refreshed.getFieldSpecFor("id").getDefaultNullValue(), -2);
    assertEquals(original.getFieldSpecFor("id").getDefaultNullValue(), -1);
    assertSame(manager.getCachedTableConfigAndSchema().getRight(), refreshed);
    assertSame(manager.fetchIndexLoadingConfig().getSchema(), refreshed);

    manager.updateCachedTableConfigAndSchema(table, changed);
    assertSame(manager.fetchIndexLoadingConfig().getSchema(), changed);
  }

  @Test
  public void testTimestampNormalizationBeforeReuse() {
    BaseTableDataManager manager = manager(timestampTable(TimestampIndexGranularity.DAY), schema());
    Schema first = manager.fetchIndexLoadingConfig().getSchema();
    IndexLoadingConfig second = manager.fetchIndexLoadingConfig();
    assertSame(second.getSchema(), first);
    assertTrue(first.hasColumn("$ts$DAY"));
    assertTrue(second.getFieldIndexConfigByColName().get("$ts$DAY").getConfig(StandardIndexes.range()).isEnabled());
    assertEquals(second.getTableConfig().getIndexingConfig().getRangeIndexColumns(), List.of("$ts$DAY"));
    assertEquals(second.getTableConfig().getIngestionConfig().getTransformConfigs().size(), 1);

    ZKMetadataProvider.setTableConfig(manager._propertyStore, timestampTable(TimestampIndexGranularity.HOUR));
    Schema changed = manager.fetchIndexLoadingConfig().getSchema();
    assertNotSame(changed, first);
    assertTrue(changed.hasColumn("$ts$HOUR"));
    assertFalse(changed.hasColumn("$ts$DAY"));
    assertFalse(first.hasColumn("$ts$HOUR"));
  }

  @Test
  public void testNestedChangesAreNotHiddenBySchemaEquality() {
    Schema original = complexSchema(-1);
    BaseTableDataManager manager = manager(timestampTable(TimestampIndexGranularity.DAY), original);
    Schema first = manager.fetchIndexLoadingConfig().getSchema();
    assertSame(manager.fetchIndexLoadingConfig().getSchema(), first);

    Schema changed = complexSchema(-2);
    assertEquals(changed, original);
    ZKMetadataProvider.setSchema(manager._propertyStore, changed);
    Schema refreshed = manager.fetchIndexLoadingConfig().getSchema();
    assertNotSame(refreshed, first);
    ComplexFieldSpec nested = (ComplexFieldSpec) refreshed.getFieldSpecFor("nested");
    assertEquals(((ComplexFieldSpec) nested.getChildFieldSpec("value")).getChildFieldSpec("value")
        .getDefaultNullValue(), -2);
    assertSame(manager.fetchIndexLoadingConfig().getSchema(), refreshed);
  }

  @Test
  public void testConcurrentLoadsReuseCachedSchema()
      throws Exception {
    BaseTableDataManager manager = manager(timestampTable(TimestampIndexGranularity.DAY), schema());
    Schema shared = manager.fetchIndexLoadingConfig().getSchema();
    ExecutorService executor = Executors.newFixedThreadPool(8);
    CountDownLatch start = new CountDownLatch(1);
    try {
      List<Future<Schema>> results = new ArrayList<>();
      for (int i = 0; i < 32; i++) {
        results.add(executor.submit(() -> {
          assertTrue(start.await(10, TimeUnit.SECONDS));
          return manager.fetchIndexLoadingConfig().getSchema();
        }));
      }
      start.countDown();
      for (Future<Schema> result : results) {
        assertSame(result.get(10, TimeUnit.SECONDS), shared);
      }
    } finally {
      start.countDown();
      executor.shutdownNow();
    }
  }

  private static BaseTableDataManager manager(TableConfig table, Schema schema) {
    BaseTableDataManager manager = new OfflineTableDataManager();
    manager._propertyStore = new FakePropertyStore();
    manager._tableNameWithType = "testTable_OFFLINE";
    ZKMetadataProvider.setTableConfig(manager._propertyStore, table);
    ZKMetadataProvider.setSchema(manager._propertyStore, schema);
    return manager;
  }

  private static Schema schema() {
    return new Schema.SchemaBuilder().setSchemaName("testTable")
        .addSingleValueDimension("id", DataType.INT, -1)
        .addDateTime("ts", DataType.TIMESTAMP, "TIMESTAMP", "1:MILLISECONDS").build();
  }

  private static Schema complexSchema(int defaultValue) {
    Schema schema = schema();
    ComplexFieldSpec child = new ComplexFieldSpec("value", DataType.MAP, true,
        Map.of("value", new DimensionFieldSpec("value", DataType.INT, true, defaultValue)));
    schema.addField(new ComplexFieldSpec("nested", DataType.MAP, true, Map.of("value", child)));
    return schema;
  }

  private static TableConfig timestampTable(TimestampIndexGranularity granularity) {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setFieldConfigList(List.of(new FieldConfig.Builder("ts")
            .withTimestampConfig(new TimestampConfig(List.of(granularity))).build())).build();
  }
}
