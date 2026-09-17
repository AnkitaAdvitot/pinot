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
import java.util.function.Consumer;
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
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Verifies table-scoped schema identity across fresh loads, concurrent loads, and schema evolution.
public class BaseTableDataManagerSchemaReuseTest {
  @Test
  public void testFreshTableManagerFetchesShareSchemaAndObserveChanges() {
    BaseTableDataManager manager = new OfflineTableDataManager();
    manager._propertyStore = new FakePropertyStore();
    manager._tableNameWithType = "testTable_OFFLINE";
    TableConfig table = timestampTable(TimestampIndexGranularity.DAY);
    ZKMetadataProvider.setTableConfig(manager._propertyStore, table);
    ZKMetadataProvider.setSchema(manager._propertyStore, schema());

    IndexLoadingConfig first = manager.fetchIndexLoadingConfig();
    IndexLoadingConfig second = manager.fetchIndexLoadingConfig();
    assertNotSame(first.getTableConfig(), second.getTableConfig());
    assertSame(second.getSchema(), first.getSchema());
    assertSame(second.getSegmentSchemaContext(), first.getSegmentSchemaContext());
    assertSame(second.getSchema().getFieldSpecFor("id"), first.getSchema().getFieldSpecFor("id"));
    assertSame(second.getSchema().getFieldSpecFor("$ts$DAY"), first.getSchema().getFieldSpecFor("$ts$DAY"));
    assertSame(manager.getCachedTableConfigAndSchema().getRight(), second.getSchema());

    Schema changed = schema();
    changed.getFieldSpecFor("id").setDefaultNullValue(-2);
    ZKMetadataProvider.setSchema(manager._propertyStore, changed);
    IndexLoadingConfig third = manager.fetchIndexLoadingConfig();
    assertNotSame(third.getSchema(), first.getSchema());
    assertEquals(third.getSchema().getFieldSpecFor("id").getDefaultNullValue(), -2);
    assertEquals(first.getSchema().getFieldSpecFor("id").getDefaultNullValue(), -1);
    assertSame(manager.getCachedTableConfigAndSchema().getRight(), third.getSchema());
    assertSame(manager.fetchIndexLoadingConfig().getSchema(), third.getSchema());
  }

  @Test
  public void testEqualFreshSchemasReuseLatestInstance()
      throws Exception {
    BaseTableDataManager manager = new OfflineTableDataManager();
    Schema first = schema();
    Schema fresh = Schema.fromString(first.toSingleLineJsonString());
    assertNotSame(first, fresh);
    assertSame(manager.createIndexLoadingConfig(table(), first).getSchema(), first);
    assertSame(manager.createIndexLoadingConfig(table(), fresh).getSchema(), first);

    // Separate table managers must not share their schemas through this cache.
    BaseTableDataManager otherManager = new OfflineTableDataManager();
    assertSame(otherManager.createIndexLoadingConfig(table(), fresh).getSchema(), fresh);
  }

  @Test
  public void testExistingCachedSchemaIsTheSourceOfSharedIdentity() {
    BaseTableDataManager manager = new OfflineTableDataManager();
    Schema cached = schema();
    manager.updateCachedTableConfigAndSchema(table(), cached);
    IndexLoadingConfig first = manager.createIndexLoadingConfig(table(), schema());
    assertSame(first.getSchema(), cached);

    Schema replacement = schema();
    manager.updateCachedTableConfigAndSchema(table(), replacement);
    IndexLoadingConfig second = manager.createIndexLoadingConfig(table(), schema());
    assertSame(second.getSchema(), replacement);
    assertNotSame(second.getSegmentSchemaContext(), first.getSegmentSchemaContext());
  }

  @Test
  public void testFailedLoadingConfigDoesNotReplaceCachedSchema() {
    BaseTableDataManager manager = new OfflineTableDataManager();
    IndexLoadingConfig first = manager.createIndexLoadingConfig(table(), schema());
    TableConfig invalid = table();
    invalid.getIndexingConfig().setLoadMode("INVALID");
    Schema changed = schema();
    changed.getFieldSpecFor("id").setDefaultNullValue(-2);
    assertThrows(IllegalArgumentException.class, () -> manager.createIndexLoadingConfig(invalid, changed));
    assertSame(manager.getCachedTableConfigAndSchema().getLeft(), first.getTableConfig());
    assertSame(manager.getCachedTableConfigAndSchema().getRight(), first.getSchema());
    assertSame(manager.createIndexLoadingConfig(table(), schema()).getSegmentSchemaContext(),
        first.getSegmentSchemaContext());
  }

  @DataProvider
  public Object[][] schemaChanges() {
    return new Object[][]{
        {(Consumer<Schema>) schema -> schema.getFieldSpecFor("id").setDefaultNullValue(-2)},
        {(Consumer<Schema>) schema -> schema.getFieldSpecFor("id").setDataType(DataType.LONG)},
        {(Consumer<Schema>) schema -> schema.getFieldSpecFor("id").setNotNull(true)},
        {(Consumer<Schema>) schema -> schema.setEnableColumnBasedNullHandling(true)},
        {(Consumer<Schema>) schema -> schema.getFieldSpecFor("id").setDescription("changed")},
        {(Consumer<Schema>) schema -> schema.setPrimaryKeyColumns(List.of("id"))}
    };
  }

  @Test(dataProvider = "schemaChanges")
  public void testSchemaChangesReplaceLatestInstance(Consumer<Schema> change) {
    BaseTableDataManager manager = new OfflineTableDataManager();
    Schema first = schema();
    manager.createIndexLoadingConfig(table(), first);
    Schema changed = schema();
    change.accept(changed);
    assertSame(manager.createIndexLoadingConfig(table(), changed).getSchema(), changed);
    assertEquals(first.getFieldSpecFor("id").getDefaultNullValue(), -1);
    assertFalse(first.isEnableColumnBasedNullHandling());

    // An old version is not retained in a history map after a different version replaces it.
    Schema reverted = schema();
    assertSame(manager.createIndexLoadingConfig(table(), reverted).getSchema(), reverted);
  }

  @Test
  public void testChangedSampleValueIsNotHiddenByJsonSerialization() {
    BaseTableDataManager manager = new OfflineTableDataManager();
    Schema first = schema();
    manager.createIndexLoadingConfig(table(), first);
    Schema changed = schema();
    ((DateTimeFieldSpec) changed.getFieldSpecFor("ts")).setSampleValue("1000");
    assertEquals(changed.toJsonObject(), first.toJsonObject());
    assertSame(manager.createIndexLoadingConfig(table(), changed).getSchema(), changed);
  }

  @Test
  public void testNestedComplexChangesAreNotHiddenBySchemaEquality() {
    BaseTableDataManager manager = new OfflineTableDataManager();
    Schema first = complexSchema();
    manager.createIndexLoadingConfig(table(), first);
    assertSame(manager.createIndexLoadingConfig(table(), complexSchema()).getSchema(), first);

    Schema changedDefault = complexSchema();
    nestedValue(changedDefault).setDefaultNullValue(-2);
    assertEquals(changedDefault, first);
    assertSame(manager.createIndexLoadingConfig(table(), changedDefault).getSchema(), changedDefault);
    assertEquals(nestedValue(first).getDefaultNullValue(), -1);

    Schema changedType = complexSchema();
    nestedValue(changedType).setDataType(DataType.LONG);
    assertEquals(changedType, changedDefault);
    assertSame(manager.createIndexLoadingConfig(table(), changedType).getSchema(), changedType);

    Schema removedChild = complexSchema();
    ((ComplexFieldSpec) removedChild.getFieldSpecFor("nested")).getChildFieldSpecs().remove("value");
    assertSame(manager.createIndexLoadingConfig(table(), removedChild).getSchema(), removedChild);
  }

  @Test
  public void testConcurrentEqualSchemasShareOneInstance()
      throws Exception {
    BaseTableDataManager manager = new OfflineTableDataManager();
    ExecutorService executor = Executors.newFixedThreadPool(8);
    CountDownLatch start = new CountDownLatch(1);
    try {
      List<Future<Schema>> results = new ArrayList<>();
      for (int i = 0; i < 32; i++) {
        Schema fresh = schema();
        results.add(executor.submit(() -> {
          assertTrue(start.await(10, TimeUnit.SECONDS));
          return manager.createIndexLoadingConfig(table(), fresh).getSchema();
        }));
      }
      start.countDown();
      Schema shared = results.get(0).get(10, TimeUnit.SECONDS);
      for (Future<Schema> result : results) {
        assertSame(result.get(10, TimeUnit.SECONDS), shared);
      }
    } finally {
      start.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void testTimestampNormalizationPrecedesSchemaSharing() {
    BaseTableDataManager manager = new OfflineTableDataManager();
    TableConfig firstTable = timestampTable(TimestampIndexGranularity.DAY);
    IndexLoadingConfig first = manager.createIndexLoadingConfig(firstTable, schema());
    Schema shared = first.getSchema();
    assertTrue(shared.hasColumn("$ts$DAY"));
    FieldSpec derived = shared.getFieldSpecFor("$ts$DAY");

    TableConfig freshTable = timestampTable(TimestampIndexGranularity.DAY);
    IndexLoadingConfig second = manager.createIndexLoadingConfig(freshTable, schema());
    assertSame(second.getSchema(), shared);
    assertSame(second.getSchema().getFieldSpecFor("$ts$DAY"), derived);
    assertTrue(second.getFieldIndexConfigByColName().get("$ts$DAY").getConfig(StandardIndexes.range()).isEnabled());
    assertEquals(freshTable.getIndexingConfig().getRangeIndexColumns(), List.of("$ts$DAY"));
    assertEquals(freshTable.getIngestionConfig().getTransformConfigs().size(), 1);

    IndexLoadingConfig changed =
        manager.createIndexLoadingConfig(timestampTable(TimestampIndexGranularity.HOUR), schema());
    assertNotSame(changed.getSchema(), shared);
    assertTrue(changed.getSchema().hasColumn("$ts$HOUR"));
    assertFalse(changed.getSchema().hasColumn("$ts$DAY"));
    assertFalse(shared.hasColumn("$ts$HOUR"));
    assertSame(shared.getFieldSpecFor("$ts$DAY"), derived);
  }

  private static TableConfig table() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
  }

  private static Schema schema() {
    return new Schema.SchemaBuilder().setSchemaName("testTable")
        .addSingleValueDimension("id", DataType.INT, -1)
        .addDateTime("ts", DataType.TIMESTAMP, "TIMESTAMP", "1:MILLISECONDS").build();
  }

  private static Schema complexSchema() {
    Schema schema = schema();
    ComplexFieldSpec child = new ComplexFieldSpec("value", DataType.MAP, true,
        Map.of("key", new DimensionFieldSpec("key", DataType.STRING, true),
            "value", new DimensionFieldSpec("value", DataType.INT, true, -1)));
    schema.addField(new ComplexFieldSpec("nested", DataType.MAP, true,
        Map.of("key", new DimensionFieldSpec("key", DataType.STRING, true), "value", child)));
    return schema;
  }

  private static FieldSpec nestedValue(Schema schema) {
    return ((ComplexFieldSpec) ((ComplexFieldSpec) schema.getFieldSpecFor("nested")).getChildFieldSpec("value"))
        .getChildFieldSpec("value");
  }

  private static TableConfig timestampTable(TimestampIndexGranularity granularity) {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setFieldConfigList(List.of(new FieldConfig.Builder("ts")
            .withTimestampConfig(new TimestampConfig(List.of(granularity))).build())).build();
  }
}
