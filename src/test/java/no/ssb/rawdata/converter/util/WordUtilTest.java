package no.ssb.rawdata.converter.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WordUtilTest {

    @Test
    void convertToCamelCase() {
        assertThat(WordUtil.toCamelCase("  Some       text-with__miscCasing   ")).isEqualTo("someTextWithMiscCasing");
        assertThat(WordUtil.toCamelCase(null)).isEqualTo(null);
        assertThat(WordUtil.toCamelCase("")).isEqualTo("");
        assertThat(WordUtil.toCamelCase("    \t        ")).isEqualTo("");
    }

}