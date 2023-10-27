// © 2016-2023 Graylog, Inc.

package io.resurface.trino.connector;

import io.airlift.slice.Slice;
import io.trino.spi.function.*;
import io.trino.spi.type.StandardTypes;
import jakarta.annotation.Nullable;

import java.net.MalformedURLException;
import java.net.URL;

import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.slice.Slices.utf8Slice;

public final class ResurfaceFunctions {

    @SqlNullable
    @Description("Flatten domain name")
    @ScalarFunction("flatten_domain_name")
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice flattenDomainName(@SqlType("varchar(x)") Slice domain) {
        if (domain == null) return null;
        String s = domain.toStringUtf8();
        int x = s.lastIndexOf('.');
        if (x < 0) return domain;
        x = s.lastIndexOf('.', x - 1);
        if (x < 0) return domain;
        x = s.lastIndexOf('.', x - 1);
        if (x < 0) return domain;
        return slice(s.substring(x + 1));
    }

    @SqlNullable
    @Description("Parse host from url")
    @ScalarFunction("url_parse_host")
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice urlParseHost(@SqlType("varchar(x)") Slice url) {
        URL u = parseUrl(url);
        return (u == null) ? null : slice(u.getHost());
    }

    @SqlNullable
    @Description("Parse path from url")
    @ScalarFunction("url_parse_path")
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice urlParsePath(@SqlType("varchar(x)") Slice url) {
        URL u = parseUrl(url);
        return (u == null) ? null : slice(u.getPath());
    }

    @SqlNullable
    @Description("Parse port from url")
    @ScalarFunction("url_parse_port")
    @LiteralParameters("x")
    @SqlType(StandardTypes.BIGINT)
    public static Long urlParsePort(@SqlType("varchar(x)") Slice url) {
        URL u = parseUrl(url);
        if (u == null) return null;
        int port = u.getPort();
        return (port < 0) ? null : (long) port;
    }

    @SqlNullable
    @Description("Parse protocol from url")
    @ScalarFunction("url_parse_protocol")
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice urlParseProtocol(@SqlType("varchar(x)") Slice url) {
        URL u = parseUrl(url);
        return (u == null) ? null : slice(u.getProtocol());
    }

    @SqlNullable
    @Description("Parse query from url")
    @ScalarFunction("url_parse_query")
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice urlParseQuery(@SqlType("varchar(x)") Slice url) {
        URL u = parseUrl(url);
        return (u == null) ? null : slice(u.getQuery());
    }

    @Nullable
    public static URL parseUrl(Slice url) {
        try {
            return new URL(url.toStringUtf8());
        } catch (MalformedURLException e) {
            return null;
        }
    }

    @Nullable
    public static Slice slice(String s) {
        return utf8Slice(nullToEmpty(s));
    }

}
