package no.ssb.rawdata.converter.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import lombok.experimental.UtilityClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@UtilityClass
public class Json {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Convert JSON to Object
     */
    public static <T> T toObject(Class<T> type, String json) {
        try {
            return OBJECT_MAPPER.readValue(json, type);
        }
        catch (IOException e) {
            throw new JsonException("Error mapping JSON to " + type.getSimpleName() + " object", e);
        }
    }

    /**
     * Convert JSON to Object
     *
     * Use with generics, like new TypeReference<HashMap<MyPair, String>>() {}
     */
    public static <T> T toObject(TypeReference<T> type, String json) {
        try {
            return OBJECT_MAPPER.readValue(json, type);
        }
        catch (IOException e) {
            throw new JsonException("Error mapping JSON to " + type.getType() + " object", e);
        }
    }

    /**
     * Convert JSON to String->Object map
     */
    public static Map<String, Object> toGenericMap(String json) {
        return toObject(new TypeReference<Map<String, Object>>() {}, json);
    }

    /**
     * Convert Object to JSON
     */
    public static String from(Object object) {
        try {
            return OBJECT_MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new JsonException("Error mapping " +  object.getClass().getSimpleName() + " object to JSON", e);
        }
    }

    /**
     * Convert Object to pretty (indented) JSON
     */
    public static String prettyFrom(Object object) {
        try {
            return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new JsonException("Error mapping " +  object.getClass().getSimpleName() + " object to JSON", e);
        }
    }

    /**
     * Pretty print (indent) JSON string
     */
    public static String prettyFrom(String string) {
        return prettyFrom(toObject(Object.class, string));
    }

    /**
     * Scramble values of specified properties from a JSON structure.
     */
    public static byte[] withScrambledProps(byte[] json, Iterable<String> propsToScramble) {
        try {
            JsonNode jsonNode = OBJECT_MAPPER.readTree(json);
            for (String propName : propsToScramble) {
                jsonNode.findParents(propName)
                  .forEach(n -> ((ObjectNode) n).replace(propName, new TextNode("***")));
            }
            return OBJECT_MAPPER.writeValueAsBytes(jsonNode);
        } catch (Exception e) {
            throw new JsonException("Error scrambling JSON properties " + propsToScramble, e);
        }
    }

    /**
     * Scramble values of specified properties from a JSON structure.
     */
    public static String withScrambledProps(String json, Iterable<String> propsToScramble) {
        return new String(withScrambledProps(json.getBytes(StandardCharsets.UTF_8), propsToScramble));
    }

    /**
     * Convert all property keys of the specified JSON to camelCase
     */
    public static String withCamelCasedKeys(String json) {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new SimpleModule()
          .addKeySerializer(String.class, new JsonSerializer<>() {
              @Override
              public void serialize(String value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                  String key = WordUtil.toCamelCase(value);
                  gen.writeFieldName(key);
              }
          })
        );

        try {
            Map<String, Object> jsonMap = objectMapper.readValue(json, new TypeReference<>() {
            });
            return objectMapper.writeValueAsString(jsonMap);
        } catch (Exception e) {
            throw new JsonException("Error transforming JSON", e);
        }
    }

    public static class JsonException extends RuntimeException {
        public JsonException(String message, Throwable cause) {
            super(message, cause);
        }
    }

}
