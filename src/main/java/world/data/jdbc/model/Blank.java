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
package world.data.jdbc.model;

import world.data.jdbc.internal.types.NTriplesFormat;

import java.util.UUID;

import static java.util.Objects.requireNonNull;

@lombok.Value
@SuppressWarnings("WeakerAccess")
public final class Blank implements Node {
    private final String label;

    /** Creates a new, unique blank node. */
    public static Blank unique() {
        return new Blank(UUID.randomUUID().toString());
    }

    public Blank(String label) {
        this.label = requireNonNull(label, "label");

        // Don't bother with full label validation, but do reject labels that contain invalid ascii characters.
        if (!NTriplesFormat.isWellFormedLabel(label)) {
            throw new IllegalArgumentException("Invalid blank node label: " + label);
        }
    }

    /** Returns the blank node formatted as an NTriples-compatible string. */
    @Override
    public String toString() {
        return NTriplesFormat.formatBlank(label);
    }
}
