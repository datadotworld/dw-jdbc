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

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.MalformedChallengeException;
import org.apache.http.impl.auth.RFC2617Scheme;
import org.apache.http.message.BasicHeader;

class BearerScheme extends RFC2617Scheme {
    private static final long serialVersionUID = 8734381047205470195L;

    private boolean complete;

    @Override
    public String getSchemeName() {
        return "Bearer";
    }

    @Override
    public void processChallenge(Header header) throws MalformedChallengeException {
        super.processChallenge(header);
        this.complete = true;
    }

    @Override
    public boolean isComplete() {
        return complete;
    }

    @Override
    public boolean isConnectionBased() {
        return false;
    }

    @Deprecated
    @Override
    public Header authenticate(Credentials credentials, HttpRequest request) {
        return new BasicHeader(HttpHeaders.AUTHORIZATION, "Bearer " + credentials.getPassword());
    }
}
