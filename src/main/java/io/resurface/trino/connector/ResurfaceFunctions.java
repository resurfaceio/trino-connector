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
        return (u == null) ? null : (long) u.getPort();
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
    private static URL parseUrl(Slice url) {
        try {
            return new URL(url.toStringUtf8());
        } catch (MalformedURLException e) {
            return null;
        }
    }

    private static Slice slice(@Nullable String s) {
        return utf8Slice(nullToEmpty(s));
    }

}
