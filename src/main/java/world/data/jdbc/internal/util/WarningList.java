/*
 * dw-jdbc
 * Copyright 2018 data.world, Inc.

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
package world.data.jdbc.internal.util;

import lombok.extern.java.Log;

import java.sql.SQLWarning;

@Log
public class WarningList {
    private SQLWarning head, tail;

    public synchronized SQLWarning get() {
        return head;
    }

    public void add(String warning) {
        add(new SQLWarning(warning));
    }

    public synchronized void add(SQLWarning warning) {
        log.warning("SQL Warning was issued: " + warning);
        if (head == null) {
            head = warning;
        } else {
            tail.setNextWarning(warning); // chain with existing warnings
        }
        tail = warning;
    }

    public synchronized void clear() {
        head = tail = null;
    }
}
