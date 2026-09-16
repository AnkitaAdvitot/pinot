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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.metadata.SegmentSchemaContext;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


/// Shares the latest normalized schema and metadata definitions across a table's segment loads. Equal inputs reuse
/// one parsing context;
/// a changed schema replaces the cached instance without modifying previously returned schemas. Callers must treat
/// returned schemas as read-only. Only the latest schema is retained by this cache.
final class TableSchemaCache {
  @Nullable
  private SegmentSchemaContext _context;

  synchronized SegmentSchemaContext canonicalize(Schema schema) {
    if (_context != null && _context.getSchema().equals(schema)
        && equalFieldSpecs(_context.getSchema().getFieldSpecMap(), schema.getFieldSpecMap())) {
      return _context;
    }
    _context = new SegmentSchemaContext(schema);
    return _context;
  }

  // ComplexFieldSpec.equals() does not compare its children. Check them recursively before sharing a schema.
  private static boolean equalFieldSpecs(Map<String, FieldSpec> left, Map<String, FieldSpec> right) {
    if (!left.keySet().equals(right.keySet())) {
      return false;
    }
    for (Map.Entry<String, FieldSpec> entry : left.entrySet()) {
      FieldSpec leftSpec = entry.getValue();
      FieldSpec rightSpec = right.get(entry.getKey());
      if (!leftSpec.equals(rightSpec)) {
        return false;
      }
      if (leftSpec instanceof ComplexFieldSpec && !equalFieldSpecs(
          ((ComplexFieldSpec) leftSpec).getChildFieldSpecs(), ((ComplexFieldSpec) rightSpec).getChildFieldSpecs())) {
        return false;
      }
    }
    return true;
  }
}
