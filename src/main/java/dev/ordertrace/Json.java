package dev.ordertrace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

final class Json {
    static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    static String write(Object value) {
        try { return MAPPER.writeValueAsString(value); }
        catch (Exception e) { throw new IllegalArgumentException("Cannot encode JSON", e); }
    }
    static <T> T read(String value, Class<T> type) {
        try { return MAPPER.readValue(value, type); }
        catch (Exception e) { throw new IllegalArgumentException("Invalid " + type.getSimpleName() + " JSON", e); }
    }
    static <T> Serde<T> serde(Class<T> type) {
        return Serdes.serdeFrom((topic, value) -> value == null ? null : write(value).getBytes(java.nio.charset.StandardCharsets.UTF_8),
                (topic, bytes) -> bytes == null ? null : read(new String(bytes, java.nio.charset.StandardCharsets.UTF_8), type));
    }
}
