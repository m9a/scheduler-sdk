package com.scheduler.sdk;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The bridge that passes context from the worker to the job process.
 * Carries the worker agent URL (so the job can POST status updates back),
 * the job ID, and user-supplied parameters.
 *
 * <pre>
 * Worker                                             Job process (container/child)
 * ──────                                             ──────────────────────────────
 * WorkerAgent.executeJob()                           _Harness.main(args)
 *   │                                                  │
 *   ├─ payload as EXECUTION_PAYLOAD env var ────────►  ExecutionPayload.decode(args)
 *   │  or as args[0]                                   ├─ .workerAgentUrl() → JobReporter
 *   │                                                  ├─ .jobId()
 *   │                                                  └─ .param("region", String.class)
 * </pre>
 *
 * <p>Wire format is base64(JSON): {"workerAgentUrl":"...", "jobId":"...", "params":{...}}
 */
public record ExecutionPayload(String workerAgentUrl, String jobId, Map<String, String> params) {

    public static final String ENV_EXECUTION_PAYLOAD = "EXECUTION_PAYLOAD";

    public ExecutionPayload {
        Objects.requireNonNull(workerAgentUrl, "workerAgentUrl");
        Objects.requireNonNull(jobId, "jobId");
        params = Map.copyOf(params);
    }

    /**
     * Decodes the payload from CLI args or the EXECUTION_PAYLOAD environment variable.
     * Checks args[0] first; if absent, falls back to env var.
     */
    public static ExecutionPayload decode(String[] args) {
        String raw = args.length > 0 ? args[0] : System.getenv(ENV_EXECUTION_PAYLOAD);
        if (raw == null || raw.isEmpty()) {
            throw new IllegalStateException(
                    "No payload: pass as args[0] or set " + ENV_EXECUTION_PAYLOAD + " env var");
        }
        return decodeBase64Json(raw);
    }

    /** Decodes a base64(JSON) payload string. */
    public static ExecutionPayload decodeBase64Json(String base64) {
        String json = new String(Base64.getDecoder().decode(base64));
        return fromJson(json);
    }

    /** Returns a typed parameter value with coercion for String, int, long, double, boolean. */
    @SuppressWarnings("unchecked")
    public <T> T param(String name, Class<T> type) {
        String value = params.get(name);
        if (value == null) {
            return null;
        }
        Object result;
        if (type == String.class) {
            result = value;
        } else if (type == int.class || type == Integer.class) {
            result = Integer.parseInt(value);
        } else if (type == long.class || type == Long.class) {
            result = Long.parseLong(value);
        } else if (type == double.class || type == Double.class) {
            result = Double.parseDouble(value);
        } else if (type == boolean.class || type == Boolean.class) {
            result = Boolean.parseBoolean(value);
        } else {
            throw new IllegalArgumentException("Unsupported param type: " + type.getName());
        }
        return (T) result;
    }

    /**
     * Minimal JSON parser — avoids external dependency on Jackson in the SDK.
     * Expects: {"workerAgentUrl":"...","jobId":"...","params":{"key":"val",...}}
     */
    @SuppressWarnings("unchecked")
    private static ExecutionPayload fromJson(String json) {
        String workerAgentUrl = extractString(json, "workerAgentUrl");
        String jobId = extractString(json, "jobId");

        Map<String, String> params = new HashMap<>();
        int paramsStart = json.indexOf("\"params\"");
        if (paramsStart != -1) {
            int braceStart = json.indexOf('{', paramsStart);
            if (braceStart != -1) {
                int braceEnd = findMatchingBrace(json, braceStart);
                String paramsJson = json.substring(braceStart + 1, braceEnd);
                parseSimpleMap(paramsJson, params);
            }
        }

        return new ExecutionPayload(workerAgentUrl, jobId, params);
    }

    private static String extractString(String json, String field) {
        String prefix = "\"" + field + "\":\"";
        int start = json.indexOf(prefix);
        if (start == -1) {
            // Try with space after colon
            prefix = "\"" + field + "\" : \"";
            start = json.indexOf(prefix);
            if (start == -1) {
                throw new IllegalArgumentException("Field '" + field + "' not found in: " + json);
            }
        }
        start += prefix.length();
        int end = findUnescapedQuote(json, start);
        return unescapeJson(json.substring(start, end));
    }

    private static int findUnescapedQuote(String s, int from) {
        for (int i = from; i < s.length(); i++) {
            if (s.charAt(i) == '"' && (i == 0 || s.charAt(i - 1) != '\\')) {
                return i;
            }
        }
        return s.length();
    }

    private static int findMatchingBrace(String s, int openPos) {
        int depth = 0;
        for (int i = openPos; i < s.length(); i++) {
            if (s.charAt(i) == '{') depth++;
            else if (s.charAt(i) == '}') {
                depth--;
                if (depth == 0) return i;
            }
        }
        return s.length() - 1;
    }

    private static void parseSimpleMap(String content, Map<String, String> map) {
        int i = 0;
        while (i < content.length()) {
            int keyStart = content.indexOf('"', i);
            if (keyStart == -1) break;
            int keyEnd = findUnescapedQuote(content, keyStart + 1);
            String key = unescapeJson(content.substring(keyStart + 1, keyEnd));

            int valStart = content.indexOf('"', keyEnd + 1);
            if (valStart == -1) break;
            int valEnd = findUnescapedQuote(content, valStart + 1);
            String val = unescapeJson(content.substring(valStart + 1, valEnd));

            map.put(key, val);
            i = valEnd + 1;
        }
    }

    private static String unescapeJson(String value) {
        return value.replace("\\t", "\t")
                .replace("\\r", "\r")
                .replace("\\n", "\n")
                .replace("\\\"", "\"")
                .replace("\\\\", "\\");
    }
}
