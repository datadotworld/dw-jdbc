/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package world.data.jdbc.internal.metadata;

import org.junit.Test;

import java.sql.ParameterMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;
import static world.data.jdbc.testing.MoreAssertions.assertSQLException;
import static world.data.jdbc.testing.MoreAssertions.assertSQLFeatureNotSupported;

public class ParameterMetaDataTest {

    private ParameterMetaData sampleMetadata() throws SQLException {
        return new ParameterMetaDataImpl(1);
    }

    @Test
    public void getParameterClassName() throws Exception {
        ParameterMetaData metadata = sampleMetadata();
        assertThat(metadata.getParameterClassName(1)).isEqualTo("java.lang.Object");
    }

    @Test
    public void getParameterClassNameOOB() throws Exception {
        ParameterMetaData metadata = sampleMetadata();
        assertSQLException(() -> metadata.getParameterClassName(2));
    }

    @Test
    public void getParameterCount() throws Exception {
        ParameterMetaData metadata = sampleMetadata();
        assertThat(metadata.getParameterCount()).isEqualTo(1);
    }

    @Test
    public void getParameterMode() throws Exception {
        ParameterMetaData metadata = sampleMetadata();
        assertThat(metadata.getParameterMode(1)).isEqualTo(1);
    }

    @Test
    public void getParameterModeOOB() throws Exception {
        ParameterMetaData metadata = sampleMetadata();
        assertSQLException(() -> metadata.getParameterMode(2));
    }

    @Test
    public void getParameterType() throws Exception {
        ParameterMetaData metadata = sampleMetadata();
        assertThat(metadata.getParameterType(1)).isEqualTo(Types.JAVA_OBJECT);
    }

    @Test
    public void getParameterTypeOOB() throws Exception {
        ParameterMetaData metadata = sampleMetadata();
        assertSQLException(() -> metadata.getParameterType(2));
    }

    @Test
    public void getParameterTypeName() throws Exception {
        ParameterMetaData metadata = sampleMetadata();
        assertThat(metadata.getParameterTypeName(1)).isEqualTo("java.lang.Object");
    }

    @Test
    public void getParameterTypeNameOOB() throws Exception {
        ParameterMetaData metadata = sampleMetadata();
        assertSQLException(() -> metadata.getParameterTypeName(2));
    }

    @Test
    public void getPrecision() throws Exception {
        ParameterMetaData metadata = sampleMetadata();
        assertThat(metadata.getPrecision(1)).isEqualTo(0);
    }

    @Test
    public void getPrecisionOOB() throws Exception {
        ParameterMetaData metadata = sampleMetadata();
        assertSQLException(() -> metadata.getPrecision(3));
    }

    @Test
    public void getScale() throws Exception {
        ParameterMetaData metadata = sampleMetadata();
        assertThat(metadata.getScale(1)).isEqualTo(0);
    }

    @Test
    public void getScaleOOB() throws Exception {
        ParameterMetaData metadata = sampleMetadata();
        assertSQLException(() -> metadata.getScale(-1));
    }

    @Test
    public void isNullable() throws Exception {
        ParameterMetaData metadata = sampleMetadata();
        assertThat(metadata.isNullable(1)).isEqualTo(ResultSetMetaData.columnNullableUnknown);
    }

    @Test
    public void isNullableOOB() throws Exception {
        ParameterMetaData metadata = sampleMetadata();
        assertSQLException(() -> metadata.isNullable(0));
    }

    @Test
    public void isSigned() throws Exception {
        ParameterMetaData metadata = sampleMetadata();
        assertThat(metadata.isSigned(1)).isFalse();
    }

    @Test
    public void isSignedOOB() throws Exception {
        ParameterMetaData metadata = sampleMetadata();
        assertSQLException(() -> metadata.isSigned(4092));
    }

    @Test
    public void testAllNotSupported() throws Exception {
        ParameterMetaData metadata = sampleMetadata();
        assertSQLFeatureNotSupported(() -> metadata.isWrapperFor(Class.class));
        assertSQLFeatureNotSupported(() -> metadata.unwrap(Class.class));
    }
}
