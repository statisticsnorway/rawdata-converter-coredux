package no.ssb.rawdata.converter.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.experimental.UtilityClass;
import net.thisptr.jackson.jq.BuiltinFunctionLoader;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Versions;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@UtilityClass
public class Jq {

    private static final Scope ROOT_JQ_SCOPE;
    private static final ObjectMapper OBJECT_MAPPER;

    static {
        ROOT_JQ_SCOPE = Scope.newEmptyScope();
        BuiltinFunctionLoader.getInstance().loadFunctions(Versions.JQ_1_6, ROOT_JQ_SCOPE);

        OBJECT_MAPPER = new ObjectMapper();
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static List<JsonNode> query(String jqExpression, String json) {
        Scope childScope = Scope.newChildScope(ROOT_JQ_SCOPE);
        List<JsonNode> out = new ArrayList<>();
        try {
            JsonNode in = OBJECT_MAPPER.readTree(json);
            JsonQuery jq = JsonQuery.compile(jqExpression, Versions.JQ_1_6);

            jq.apply(childScope, in, out::add);
        }
        catch (Exception e) {
            throw new JqException("jq query error for jqExpression=" + jqExpression, e);
        }
        return out;
    }

    public static <T> Optional<T> queryOne(String jqPath, String json, Class<T> clazz) {
        List<JsonNode> nodes = query(jqPath, json);
        if (nodes.size() == 0) {
            return Optional.empty();
        }
        else if (nodes.size() > 1) {
            throw new JqException("Expected JQ expression to match a single value, but multiple matches was found: " + nodes);
        }
        else {
            return Optional.ofNullable(OBJECT_MAPPER.convertValue(nodes.get(0), clazz));
        }
    }

    public static <T> Optional<T> queryOne(String jqPath, String json, TypeReference<T> type) {
        List<JsonNode> nodes = query(jqPath, json);
        if (nodes.size() == 0) {
            return Optional.empty();
        }
        else if (nodes.size() > 1) {
            throw new JqException("Expected JQ expression to match a single value, but multiple matches was found: " + nodes);
        }
        else {
            return Optional.ofNullable(OBJECT_MAPPER.convertValue(nodes.get(0), type));
        }
    }


    public static class JqException extends RuntimeException {
        public JqException(String message) {
            super(message);
        }

        public JqException(String message, Throwable cause) {
            super(message, cause);
        }
    }

}
