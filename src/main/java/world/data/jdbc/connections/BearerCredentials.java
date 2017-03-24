/*
* dw-jdbc
* Copyright 2016 data.world, Inc.

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
package world.data.jdbc.connections;

import org.apache.http.auth.Credentials;

import java.io.Serializable;
import java.security.Principal;

import static java.util.Objects.requireNonNull;

class BearerCredentials implements Credentials, Serializable {
    private static final long serialVersionUID = 8435312191929337240L;

    private final String token;

    BearerCredentials(String token) {
        this.token = requireNonNull(token, "token");
    }

    @Override
    public Principal getUserPrincipal() {
        return null;
    }

    @Override
    public String getPassword() {
        return token;
    }
}
