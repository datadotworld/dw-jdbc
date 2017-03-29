/*
* dw-jdbc
* Copyright 2017 data.world, Inc.

* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the
* License.
*
* You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
* implied. See the License for the specific language governing
* permissions and limitations under the License.
*
* This product includes software developed at data.world, Inc.(http://www.data.world/).
*/
package world.data.jdbc.internal.types;

import lombok.AccessLevel;
import world.data.jdbc.model.Iri;

import javax.annotation.Nonnull;
import java.sql.JDBCType;

@lombok.Getter
@lombok.AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TypeMapping {
    @Nonnull
    private final Iri datatype;
    @Nonnull
    private final JDBCType jdbcType;
    @Nonnull
    private final Class<?> javaType;
    private final int precision;  // aka columnSize
    private final Integer scale;  // # digits to the right of the decimal point
    private final Boolean signed;  // is this a signed number?
    private final Boolean fixedPrecisionScale;  // can it be used for money?

    static TypeMapping simple(Iri datatype, JDBCType jdbcType, Class<?> javaType, int columnSize) {
        return new TypeMapping(datatype, jdbcType, javaType, columnSize, null, null, null);
    }

    static TypeMapping numeric(Iri datatype, JDBCType jdbcType, Class<?> javaType, int precision, int scale,
                               boolean signed, boolean fixedPrecisionScale) {
        return new TypeMapping(datatype, jdbcType, javaType, precision, scale, signed, fixedPrecisionScale);
    }

    public int getTypeNumber() {
        return jdbcType.getVendorTypeNumber();
    }
}
