package world.data.jdbc;

import org.apache.http.impl.client.AbstractHttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.jena.atlas.web.auth.HttpAuthenticator;

import java.net.URI;

class DataWorldHttpAuthenticator implements HttpAuthenticator {
    private final String username;
    private final String password;

    DataWorldHttpAuthenticator(final String username, final String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public void apply(final AbstractHttpClient abstractHttpClient, final HttpContext httpContext, final URI uri) {
        abstractHttpClient.addRequestInterceptor((httpRequest, context) -> httpRequest.setHeader("Authorization", "Bearer " + password));
    }

    @Override
    public void invalidate() {
        //noop
    }

}
