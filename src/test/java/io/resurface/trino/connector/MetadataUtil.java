// © 2016-2023 Graylog, Inc.

package io.resurface.trino.connector;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;

import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

final class MetadataUtil {

    private MetadataUtil() {
    }

    public static final JsonCodec<ResurfaceColumnHandle> COLUMN_CODEC;
    public static final JsonCodec<ResurfaceTableHandle> TABLE_CODEC;

    static {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TestingTypeDeserializer()));
        JsonCodecFactory codecFactory = new JsonCodecFactory(objectMapperProvider);
        COLUMN_CODEC = codecFactory.jsonCodec(ResurfaceColumnHandle.class);
        TABLE_CODEC = codecFactory.jsonCodec(ResurfaceTableHandle.class);
    }

    public static final class TestingTypeDeserializer extends FromStringDeserializer<Type> {

        private final Map<TypeId, Type> types = new ImmutableMap.Builder<TypeId, Type>()
                .put(BOOLEAN.getTypeId(), BOOLEAN)
                .put(BIGINT.getTypeId(), BIGINT)
                .put(DOUBLE.getTypeId(), DOUBLE)
                .put(createTimestampWithTimeZoneType(3).getTypeId(), createTimestampWithTimeZoneType(3))
                .put(DATE.getTypeId(), DATE)
                .put(VARCHAR.getTypeId(), createUnboundedVarcharType())
                .build();

        public TestingTypeDeserializer() {
            super(Type.class);
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context) {
            Type type = types.get(TypeId.of(value));
            if (type == null) {
                throw new IllegalArgumentException("Unknown type " + value);
            }
            return type;
        }
    }

}
