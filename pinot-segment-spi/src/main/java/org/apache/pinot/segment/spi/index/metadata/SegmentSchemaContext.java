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
package org.apache.pinot.segment.spi.index.metadata;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.FieldSpec.MaxLengthExceedStrategy;
import org.apache.pinot.spi.data.Schema;


/// Shared, normalized table schema and the scalar definitions its segment metadata would reconstruct. Logical-only
/// attributes stay on the table schema. Stored definitions are reused only when every parsed constructor input matches;
/// historical definitions and complex fields keep the ordinary parser path. Construct before publication, and treat the
/// schema and returned specs as read-only. Resolution performs no mutations and is safe for concurrent segment loads.
public final class SegmentSchemaContext {
  private final Schema _schema;
  private final Map<String, ColumnDefinition> _definitions = new HashMap<>();

  public SegmentSchemaContext(Schema schema) {
    _schema = schema;
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      FieldType fieldType = fieldSpec.getFieldType();
      if (!fieldSpec.isVirtualColumn()
          && (fieldType == FieldType.DIMENSION || fieldType == FieldType.METRIC || fieldType == FieldType.DATE_TIME)) {
        _definitions.put(fieldSpec.getName(), new ColumnDefinition(fieldSpec));
      }
    }
  }

  public Schema getSchema() {
    return _schema;
  }

  @Nullable
  FieldSpec resolve(String name, FieldType fieldType, DataType dataType, boolean singleValue,
      @Nullable String defaultNullValue, @Nullable Integer maxLength,
      @Nullable MaxLengthExceedStrategy maxLengthExceedStrategy, @Nullable String format,
      @Nullable String granularity) {
    ColumnDefinition definition = _definitions.get(name);
    return definition != null && definition._fieldType == fieldType && definition._dataType == dataType
        && definition._singleValue == singleValue && Objects.equals(definition._defaultNullValue, defaultNullValue)
        && Objects.equals(definition._maxLength, maxLength)
        && definition._maxLengthExceedStrategy == maxLengthExceedStrategy
        && Objects.equals(definition._format, format) && Objects.equals(definition._granularity, granularity)
        ? definition._fieldSpec : null;
  }

  /// Metadata constructor inputs and their shared result, computed once for a normalized table column.
  private static final class ColumnDefinition {
    private final FieldType _fieldType;
    private final DataType _dataType;
    private final boolean _singleValue;
    @Nullable
    private final String _defaultNullValue;
    @Nullable
    private final Integer _maxLength;
    @Nullable
    private final MaxLengthExceedStrategy _maxLengthExceedStrategy;
    @Nullable
    private final String _format;
    @Nullable
    private final String _granularity;
    private final FieldSpec _fieldSpec;

    private ColumnDefinition(FieldSpec source) {
      _fieldType = source.getFieldType();
      _dataType = source.getDataType();
      _singleValue = source.isSingleValueField();
      _defaultNullValue = ColumnMetadataImpl.canonicalDefaultNullValue(_fieldType, _dataType,
          source.getDefaultNullValueString());
      // Match BaseSegmentCreator.addFieldSpec(): default length settings are omitted from metadata.properties.
      DataType storedType = _dataType.getStoredType();
      boolean storesLength = storedType == DataType.STRING || storedType == DataType.BYTES;
      _maxLength = storesLength ? source.getNonDefaultMaxLength() : null;
      _maxLengthExceedStrategy = storesLength ? source.getNonDefaultMaxLengthExceedStrategy() : null;
      _format = source instanceof DateTimeFieldSpec ? ((DateTimeFieldSpec) source).getFormat() : null;
      _granularity = source instanceof DateTimeFieldSpec ? ((DateTimeFieldSpec) source).getGranularity() : null;
      FieldSpec projected = ColumnMetadataImpl.createScalarFieldSpec(source.getName().intern(), _fieldType, _dataType,
          _singleValue, _defaultNullValue, _maxLength, _maxLengthExceedStrategy, _format, _granularity);
      _fieldSpec = projected.equals(source) ? source : projected;
    }
  }
}
