package de.codecentric.reedelk.database.internal.commons;

import de.codecentric.reedelk.runtime.api.commons.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class   QueryStatementTemplateTest {

    @Test
    void shouldCorrectlyReplaceVariables() {
        // Given
        QueryStatementTemplate replacer = new QueryStatementTemplate("SELECT * FROM Orders WHERE name = :name AND surname = :surname");

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
        QueryStatementTemplate replacer = new QueryStatementTemplate("SELECT * FROM Orders WHERE name = :name AND surname = :surname;");

        // When
        Map<String,Object> replacements = ImmutableMap.of( "surname", "Anton");
        String replaced = replacer.replace(replacements);

        // Then
        assertThat(replaced).isEqualTo("SELECT * FROM Orders WHERE name = :name AND surname = 'Anton';");
    }

    @Test
    void shouldReplaceIntegerObjectVariable() {
        // Given
        QueryStatementTemplate replacer = new QueryStatementTemplate("SELECT * FROM Orders WHERE id = :orderId;");

        // When
        Map<String,Object> replacements = ImmutableMap.of( "orderId", 1238498234);
        String replaced = replacer.replace(replacements);

        // Then
        assertThat(replaced).isEqualTo("SELECT * FROM Orders WHERE id = 1238498234;");
    }

    @Test
    void shouldCorrectlyReplaceParameterizedInsertValues() {
        // Given
        QueryStatementTemplate replacer = new QueryStatementTemplate("INSERT INTO ORDERS VALUES (:id,:name)");

        // When
        Map<String,Object> replacements = ImmutableMap.of("id", "aabbcc", "name", "my test name");
        String replaced = replacer.replace(replacements);

        // Then
        assertThat(replaced).isEqualTo("INSERT INTO ORDERS VALUES ('aabbcc','my test name')");
    }
}
