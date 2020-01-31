package com.reedelk.mysql.utils;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryReplacer {

    private static final Pattern REGEXP = Pattern.compile("(:\\w+)");
    private final String query;
    private final Matcher matcher;

    public QueryReplacer(String query) {
        this.query = query;
        matcher = REGEXP.matcher(query);
    }

    public String replace(Map<String,Object> replacements) {
        if (replacements.isEmpty()) {
            return query;
        }

        StringBuilder builder = new StringBuilder();
        int i  = 0;
        while (matcher.find()) {
            Object replacement = replacements.get(matcher.group(1).substring(1));
            builder.append(query, i, matcher.start());
            if (replacement == null) {
                builder.append(matcher.group(0));
            } else {
                if (replacement instanceof String) {
                    builder.append("'").append(replacement).append("'");
                } else {
                    builder.append(replacement);
                }
            }
            i = matcher.end();
        }
        return builder.toString();
    }
}
