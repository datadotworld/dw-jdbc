package world.data.jdbc;

import fi.iki.elonen.NanoHTTPD;
import org.apache.http.HttpHeaders;
import org.apache.jena.ext.com.google.common.base.Throwables;
import org.junit.rules.ExternalResource;

/**
 * JUnit @{code @Rule} for a simple HTTP server for use in unit tests.
 */
abstract class NanoHTTPDResource extends ExternalResource {
    private final NanoHTTPD apiServer;

    NanoHTTPDResource(int port) {
        this.apiServer = new NanoHTTPD(port) {
            @Override
            public Response serve(IHTTPSession session) {
                try {
                    return NanoHTTPDResource.this.serve(session);
                } catch (Exception e) {
                    e.printStackTrace();
                    return newResponse(Response.Status.INTERNAL_ERROR, "text/plain", Throwables.getStackTraceAsString(e));
                }
            }
        };
    }

    @Override
    protected void before() throws Throwable {
        apiServer.start();
    }

    @Override
    protected void after() {
        apiServer.stop();
    }

    protected abstract NanoHTTPD.Response serve(final NanoHTTPD.IHTTPSession session) throws Exception;

    protected static NanoHTTPD.Response newResponse(NanoHTTPD.Response.Status status, String mimeType, String body) {
        NanoHTTPD.Response response = NanoHTTPD.newFixedLengthResponse(status, mimeType, body);
        response.addHeader(HttpHeaders.CONNECTION, "close");  // avoid errors due to ignoring the POST body
        return response;
    }
}
