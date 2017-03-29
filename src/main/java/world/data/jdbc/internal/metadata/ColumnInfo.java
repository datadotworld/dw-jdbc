/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package world.data.jdbc.internal.metadata;

import javax.annotation.Nonnull;
import java.sql.ResultSetMetaData;

@lombok.Value
@lombok.Builder(builderClassName = "Builder")
public final class ColumnInfo {
    @Nonnull
    private String catalogName;
    @Nonnull
    private String schemaName;
    @Nonnull
    private String tableName;
    @Nonnull
    private String label;
    @Nonnull
    private String className;
    private String typeName;
    private int displaySize;
    private int type;
    private int precision;
    private int scale;
    private boolean signed;  // ie. numeric allows negative numbers
    private int nullable;

    boolean isCaseSensitive() {
        // Most types in RDF/SPARQL are subject to case sensitivity especially
        // when talking strict RDF equality semantics
        return true;
    }

    boolean isCurrency() {
        // No specific currency type in RDF/SPARQL
        return false;
    }

    boolean isAutoIncrement() {
        // SPARQL engines don't have a notion of auto-increment
        return false;
    }

    boolean isWritable() {
        // All JDBC results are read-only currently
        return false;
    }

    boolean isReadOnly() {
        // All JDBC results are read-only currently
        return true;
    }

    boolean isSearchable() {
        // Assume all columns are searchable since the entire RDF dataset is searchable
        return true;
    }

    public static class Builder {
        private String catalogName = "";
        private String schemaName = "";
        private String tableName = "";
        private int displaySize = Integer.MAX_VALUE;
        private int nullable = ResultSetMetaData.columnNullable;
    }
}
