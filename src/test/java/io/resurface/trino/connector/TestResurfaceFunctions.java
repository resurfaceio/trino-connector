// © 2016-2023 Graylog, Inc.

package io.resurface.trino.connector;

import org.testng.annotations.Test;

import java.util.Objects;

import static io.resurface.trino.connector.ResurfaceFunctions.*;
import static org.junit.Assert.assertNull;
import static org.testng.Assert.assertEquals;

public class TestResurfaceFunctions {

    @Test
    public void testFlatteningDomainNames() {
        // relative domain names
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("z"))).toStringAscii(), "z");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("localhost"))).toStringAscii(), "localhost");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("y.z"))).toStringAscii(), "y.z");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("foo.io"))).toStringAscii(), "foo.io");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("x.y.z"))).toStringAscii(), "x.y.z");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("docs.foo.io"))).toStringAscii(), "docs.foo.io");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("blah.docs.foo.io"))).toStringAscii(), "docs.foo.io");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("y.blah.docs.foo.io"))).toStringAscii(), "docs.foo.io");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("z.y.blah.docs.foo.com"))).toStringAscii(), "docs.foo.com");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("v.w.x.y.z"))).toStringAscii(), "x.y.z");

        // empty name segments
        assertEquals(Objects.requireNonNull(flattenDomainName(slice(".foo.io"))).toStringAscii(), ".foo.io");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("foo..io"))).toStringAscii(), "foo..io");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("docs.foo..io"))).toStringAscii(), "foo..io");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("docs..foo.io"))).toStringAscii(), ".foo.io");

        // absolute domain names
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("localhost."))).toStringAscii(), "localhost.");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("foo.io."))).toStringAscii(), "foo.io.");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("docs.foo.io."))).toStringAscii(), "foo.io.");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("blah.docs.foo.io."))).toStringAscii(), "foo.io.");

        // other edge cases
        assertEquals(Objects.requireNonNull(flattenDomainName(slice(null))).toStringAscii(), "");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice(""))).toStringAscii(), "");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("."))).toStringAscii(), ".");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice(".."))).toStringAscii(), "..");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("..."))).toStringAscii(), "..");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("...."))).toStringAscii(), "..");
        assertEquals(Objects.requireNonNull(flattenDomainName(slice("....."))).toStringAscii(), (".."));
    }

    @Test
    public void testParsingURLs() {
        // test ports
        assertNull(urlParsePort(slice("http://foo/bar")));
        assertNull(urlParsePort(slice("https://foo/bar")));
        assertEquals(Objects.requireNonNull(urlParsePort(slice("http://foo:8080/bar"))), 8080);

        // test hosts
        assertEquals(Objects.requireNonNull(urlParseHost(slice("http:/"))).toStringAscii(), "");
        assertEquals(Objects.requireNonNull(urlParseHost(slice("http://"))).toStringAscii(), "");
        assertEquals(Objects.requireNonNull(urlParseHost(slice("http:///"))).toStringAscii(), "");
        assertEquals(Objects.requireNonNull(urlParseHost(slice("http://foo/bar"))).toStringAscii(), "foo");
        assertEquals(Objects.requireNonNull(urlParseHost(slice("http://foo.foo.foo/bar"))).toStringAscii(), "foo.foo.foo");
        assertEquals(Objects.requireNonNull(urlParseHost(slice("http://1.2.3.4/bar"))).toStringAscii(), "1.2.3.4");

        // test paths
        assertEquals(Objects.requireNonNull(urlParsePath(slice("http://foo"))).toStringAscii(), "");
        assertEquals(Objects.requireNonNull(urlParsePath(slice("http://foo/"))).toStringAscii(), "/");
        assertEquals(Objects.requireNonNull(urlParsePath(slice("http://foo/bar"))).toStringAscii(), "/bar");
        assertEquals(Objects.requireNonNull(urlParsePath(slice("http://foo/bar/bie"))).toStringAscii(), "/bar/bie");

        // test query strings
        assertEquals(Objects.requireNonNull(urlParseQuery(slice("http://foo/bar"))).toStringAscii(), "");
        assertEquals(Objects.requireNonNull(urlParseQuery(slice("http://foo/bar?"))).toStringAscii(), "");
        assertEquals(Objects.requireNonNull(urlParseQuery(slice("http://foo/bar?1=1"))).toStringAscii(), "1=1");
        assertEquals(Objects.requireNonNull(urlParseQuery(slice("http://foo/bar?1=1&b=2"))).toStringAscii(), "1=1&b=2");

        // these "invalid" URLs would fail with the url_extract_host function
        assertEquals(Objects.requireNonNull(urlParseHost(slice("http://foo/bar bar"))).toStringAscii(), "foo");
        assertEquals(Objects.requireNonNull(urlParseHost(slice("http://foo oo/bar"))).toStringAscii(), "foo oo");
        assertEquals(Objects.requireNonNull(urlParsePath(slice("http://foo oo/bar"))).toStringAscii(), "/bar");
        assertEquals(Objects.requireNonNull(urlParsePath(slice("http://foo/bar bar"))).toStringAscii(), "/bar bar");
        assertEquals(Objects.requireNonNull(urlParsePath(slice("http://foo/bar bar/zoom"))).toStringAscii(), "/bar bar/zoom");
    }

}
