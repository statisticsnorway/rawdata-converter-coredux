package no.ssb.rawdata.converter.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

// TODO: Move this to a separate rawdata-converter-xml lib?

public class Xml {
    private static final XmlMapper MAPPER = new XmlMapper();

    static {
        MAPPER.registerModule(new SimpleModule().addDeserializer(Object.class, new FixedUntypedObjectDeserializer()));
    }

    private Xml() {}

    /**
     * Convert XML to Object
     */
    public static <T> T toObject(Class<T> type, String xml) {
        try {
            return MAPPER.readValue(xml, type);
        } catch (IOException e) {
            throw new XmlMappingException("Error mapping XML to " + type.getSimpleName() + " object", e);
        }
    }

    /**
     * Convert XML to Object
     * <p>
     * Use with generics, like new TypeReference<HashMap<MyPair, String>>() {}
     */
    public static <T> T toObject(TypeReference<T> type, String xml) {
        try {
            return MAPPER.readValue(xml, type);
        } catch (IOException e) {
            throw new XmlMappingException("Error mapping XML to " + type.getType() + " object", e);
        }
    }

    /**
     * Convert XML to String->Object map
     */
    public static Map<String, Object> toGenericMap(String xml) {

        /*
        The following implementation should be revisited when we upgrade to jackson 2.12.
        Jackson suffers from two issues:
        1) no proper array detection in xml documents (ref https://github.com/FasterXML/jackson-dataformat-xml/issues/403
          and https://github.com/FasterXML/jackson-dataformat-xml/issues/205)

        2) Nodes might be initialized with empty key elements, ref:
        https://stackoverflow.com/questions/62009220/jackson-jsonnode-with-empty-element-key
         */

        Map<String, Object> map = toObject(new TypeReference<>() {}, xml);
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode node = objectMapper.valueToTree(map);
        replaceEmptyKeys(node);
        return objectMapper.convertValue(node, new TypeReference<Map<String, Object>>() {});

        /*
        The following should work when we upgrade to jackson 2.12:
        try {
            JsonNode node = MAPPER.readTree(xml);
            replaceEmptyKeys(node);
            return MAPPER.convertValue(node, new TypeReference<Map<String, Object>>() {});

        }
        catch (IOException e) {
            throw new XmlMappingException("Error mapping XML to Map<String,Object>", e);
        }
        */
    }

    /**
     * Convert Object to XML
     */
    public static String from(Object object) {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new XmlMappingException("Error mapping " + object.getClass().getSimpleName() + " object to XML", e);
        }
    }

    /**
     * Convert Object to pretty (indented) XML
     */
    public static String prettyFrom(Object object) {
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new XmlMappingException("Error mapping " + object.getClass().getSimpleName() + " object to XML", e);
        }
    }

    public static class XmlMappingException extends RuntimeException {
        public XmlMappingException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static void replaceEmptyKeys(JsonNode jsonNode) {

        if (jsonNode.isArray()) {
            ArrayNode array = (ArrayNode) jsonNode;
            Iterable<JsonNode> elements = () -> array.elements();

            // recursive post-processing
            for (JsonNode element : elements) {
                replaceEmptyKeys(element);
            }
        }
        if (jsonNode.isObject()) {
            ObjectNode object = (ObjectNode) jsonNode;
            Iterable<String> fieldNames = () -> object.fieldNames();

            // recursive post-processing
            for (String fieldName : fieldNames) {
                replaceEmptyKeys(object.get(fieldName));
            }
            // check if an attribute with empty string key exists, and rename it to 'value',
            // unless there already exists another non-null attribute named 'value' which
            // would be overwritten.
            JsonNode emptyKeyValue = object.get("");
            JsonNode existing = object.get("value");
            if (emptyKeyValue != null) {
                if (existing == null || existing.isNull()) {
                    object.set("value", emptyKeyValue);
                    object.remove("");
                } else {
                    System.err.println("Skipping empty key value as a key named 'value' already exists.");
                }
            }
        }
    }

    /**
     * Jackson Deserializer that will handle xml -> String-Object Maps
     * where we have "implicit arrays", such as:
     * <dogs>
     * <dog>
     * <name>Spike</name>
     * <age>12</age>
     * </dog>
     * <dog>
     * <name>Brutus</name>
     * <age>9</age>
     * </dog>
     * <dogs>
     * <p>
     * Jackson will only include the last dog element in the resulting Map if this deserializer is not registered.
     * <p>
     * ref https://github.com/FasterXML/jackson-dataformat-xml/issues/205
     *
     * This can be removed when updating to jackson 2.12
     */
    @SuppressWarnings({"deprecation", "serial"})
    static class FixedUntypedObjectDeserializer extends UntypedObjectDeserializer {

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        protected Object mapObject(JsonParser p, DeserializationContext ctxt) throws IOException {
            String firstKey;

            JsonToken t = p.getCurrentToken();

            if (t == JsonToken.START_OBJECT) {
                firstKey = p.nextFieldName();
            } else if (t == JsonToken.FIELD_NAME) {
                firstKey = p.getCurrentName();
            } else {
                if (t != JsonToken.END_OBJECT) {
                    throw ctxt.mappingException(handledType(), p.getCurrentToken());
                }
                firstKey = null;
            }

            // empty map might work; but caller may want to modify... so better just give small modifiable
            LinkedHashMap<String, Object> resultMap = new LinkedHashMap<String, Object>(2);;
            if (firstKey == null) {
                return resultMap;
            }

            p.nextToken();
            resultMap.put(firstKey, deserialize(p, ctxt));
/*
            Object o = deserialize(p, ctxt);
            // Prevent empty keys, ref: https://github.com/FasterXML/jackson-dataformat-xml/issues/14
            if (o instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) o;
                if (map.containsKey("")) {
                    map.put(emptyKeySubstitutionString, map.remove(""));
                }
                o = map;
            }
            resultMap.put(firstKey, o);
*/
            // 03-Aug-2016, jpvarandas: handle next objects and create an array
            Set<String> listKeys = new LinkedHashSet<>();

            String nextKey;
            while ((nextKey = p.nextFieldName()) != null) {
                p.nextToken();
                if (resultMap.containsKey(nextKey)) {
                    Object listObject = resultMap.get(nextKey);

                    if (!(listObject instanceof List)) {
                        listObject = new ArrayList<>();
                        ((List) listObject).add(resultMap.get(nextKey));
                        resultMap.put(nextKey, listObject);
                    }

                    ((List) listObject).add(deserialize(p, ctxt));
                    listKeys.add(nextKey);

                } else {
                    resultMap.put(nextKey, deserialize(p, ctxt));

                }
            }

            return resultMap;

        }
    }
}
