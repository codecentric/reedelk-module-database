package com.reedelk.database.utils;

import com.reedelk.runtime.api.commons.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class QueryReplacerTest {

    @Test
    void shouldCorrectlyReplaceVariables() {
        // Given
        QueryReplacer replacer = new QueryReplacer("SELECT * FROM Orders WHERE name = :name AND surname = :surname;");

        // When
        Map<String,Object> replacements = ImmutableMap.of("name","Mark", "surname", "Anton");
        String replaced = replacer.replace(replacements);

        // Then
        assertThat(replaced).isEqualTo("SELECT * FROM Orders WHERE name = 'Mark' AND surname = 'Anton'");
    }

    // Test when only surname for instance is given and not name as well.
    @Test
    void shouldReplaceWhenMissingParameter() {
        // Given
        QueryReplacer replacer = new QueryReplacer("SELECT * FROM Orders WHERE name = :name AND surname = :surname;");

        // When
        Map<String,Object> replacements = ImmutableMap.of( "surname", "Anton");
        String replaced = replacer.replace(replacements);

        // Then
        assertThat(replaced).isEqualTo("SELECT * FROM Orders WHERE name = :name AND surname = 'Anton'");
    }

    @Test
    void shouldReplaceIntegerObjectVariable() {
        // Given
        QueryReplacer replacer = new QueryReplacer("SELECT * FROM Orders WHERE id = :orderId;");

        // When
        Map<String,Object> replacements = ImmutableMap.of( "orderId", 1238498234);
        String replaced = replacer.replace(replacements);

        // Then
        assertThat(replaced).isEqualTo("SELECT * FROM Orders WHERE id = 1238498234");
    }
}