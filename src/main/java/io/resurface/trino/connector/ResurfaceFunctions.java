// © 2016-2022 Resurface Labs Inc.

package io.resurface.trino.connector;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.VARCHAR;

public final class ResurfaceFunctions {

    @Description("Returns description for the specified interval time in millis")
    @ScalarFunction("category_for_interval")
    @SqlType(VARCHAR)
    public static Slice category_for_interval(@SqlType(BIGINT) long value) {
        if (value == 0) return UNKNOWN;
        else if (value < 2000) return SATISFIED;
        else if (value < 8000) return TOLERATING;
        else if (value < 30000) return FRUSTRATED;
        else return TIMEOUT;
    }

    @Description("Returns description for the specified size in bytes")
    @ScalarFunction("category_for_size")
    @SqlType(VARCHAR)
    public static Slice category_for_size(@SqlType(BIGINT) long value) {
        if (value == 0) return UNKNOWN;
        else if (value < 1000) return TINY;
        else if (value < 10000) return SMALL;
        else if (value < 100000) return TYPICAL;
        else if (value < 500000) return LARGE;
        else return EXCESSIVE;
    }

    @Description("Returns clique name for the specified interval time in millis")
    @ScalarFunction("clique_for_interval")
    @SqlType(VARCHAR)
    public static Slice clique_for_interval(@SqlType(BIGINT) long value) {
        if (value == 0) return UNKNOWN;
        else if (value < 250) return CLIQUE_250;
        else if (value < 500) return CLIQUE_500;
        else if (value < 750) return CLIQUE_750;
        else if (value < 1000) return CLIQUE_1000;
        else if (value < 2000) return CLIQUE_2000;
        else if (value < 3000) return CLIQUE_3000;
        else if (value < 4000) return CLIQUE_4000;
        else if (value < 5000) return CLIQUE_5000;
        else if (value < 6000) return CLIQUE_6000;
        else if (value < 7000) return CLIQUE_7000;
        else if (value < 8000) return CLIQUE_8000;
        else if (value < 9000) return CLIQUE_9000;
        else if (value < 10000) return CLIQUE_10000;
        else if (value < 15000) return CLIQUE_15000;
        else if (value < 20000) return CLIQUE_20000;
        else if (value < 30000) return CLIQUE_30000;
        else return CLIQUE_TIMEOUT;
    }

    private static final Slice CLIQUE_250 = utf8Slice("1..250 ms");
    private static final Slice CLIQUE_500 = utf8Slice("250..500 ms");
    private static final Slice CLIQUE_750 = utf8Slice("500..750 ms");
    private static final Slice CLIQUE_1000 = utf8Slice("750..1000 ms");
    private static final Slice CLIQUE_2000 = utf8Slice("1..2 sec");
    private static final Slice CLIQUE_3000 = utf8Slice("2..3 sec");
    private static final Slice CLIQUE_4000 = utf8Slice("3..4 sec");
    private static final Slice CLIQUE_5000 = utf8Slice("4..5 sec");
    private static final Slice CLIQUE_6000 = utf8Slice("5..6 sec");
    private static final Slice CLIQUE_7000 = utf8Slice("6..7 sec");
    private static final Slice CLIQUE_8000 = utf8Slice("7..8 sec");
    private static final Slice CLIQUE_9000 = utf8Slice("8..9 sec");
    private static final Slice CLIQUE_10000 = utf8Slice("9..10 sec");
    private static final Slice CLIQUE_15000 = utf8Slice("10..15 sec");
    private static final Slice CLIQUE_20000 = utf8Slice("15..20 sec");
    private static final Slice CLIQUE_30000 = utf8Slice("20..30 sec");
    private static final Slice CLIQUE_TIMEOUT = utf8Slice(">30 sec");

    private static final Slice EXCESSIVE = utf8Slice("Excessive");
    private static final Slice FRUSTRATED = utf8Slice("Frustrated");
    private static final Slice LARGE = utf8Slice("Large");
    private static final Slice SATISFIED = utf8Slice("Satisfied");
    private static final Slice SMALL = utf8Slice("Small");
    private static final Slice TIMEOUT = utf8Slice("Timeout");
    private static final Slice TINY = utf8Slice("Tiny");
    private static final Slice TOLERATING = utf8Slice("Tolerating");
    private static final Slice TYPICAL = utf8Slice("Typical");
    private static final Slice UNKNOWN = utf8Slice("Unknown");

}
