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
package world.data.jdbc.internal.metadata;

import lombok.experimental.UtilityClass;
import world.data.jdbc.internal.types.TypeMap;
import world.data.jdbc.internal.types.TypeMapping;
import world.data.jdbc.model.Iri;

import static world.data.jdbc.internal.util.Optionals.or;

@UtilityClass
public class ColumnFactory {

    public static ColumnInfo.Builder builder(String label, Iri datatype) {
        TypeMapping standard = TypeMap.INSTANCE.getStandard(datatype);
        TypeMapping custom = TypeMap.INSTANCE.getCustom(datatype);
        return ColumnInfo.builder()
                .label(label)
                .typeName(datatype.getIri())
                .type(standard.getTypeNumber())
                .className(standard.getJavaType().getName())
                .scale(or(custom.getScale(), 0))
                .precision(or(custom.getPrecision(), 0))
                .signed(or(custom.getSigned(), false));
    }
}
