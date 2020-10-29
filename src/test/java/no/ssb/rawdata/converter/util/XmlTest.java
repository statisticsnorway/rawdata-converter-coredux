package no.ssb.rawdata.converter.util;

import lombok.Data;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class XmlTest {

    @Test
    void somePojo_shouldConvertToAndFromXml() {
        SomePojo obj = new SomePojo();
        obj.setSomeInt(42);
        obj.setSomeString("blah");
        obj.setSomeDouble(42.13D);
        obj.setSomeBool(true);
        obj.setSomeArray(new String[]{"foo", "bar"});

        String xml = Xml.from(obj);
        SomePojo obj2 = Xml.toObject(SomePojo.class, xml);
        assertThat(obj).isEqualTo(obj2);
    }

    /**
     * Should handle https://github.com/FasterXML/jackson-dataformat-xml/issues/205
     */
    @Test
    void xmlWithImplicitLists_convertToGenericMap_shouldProduceList() {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
          "<person>\n" +
          "   <name>John</name>\n" +
          "   <dogs>\n" +
          "      <count>3</count>\n" +
          "      <dog>\n" +
          "         <name>Spike</name>\n" +
          "         <age>12</age>\n" +
          "      </dog>\n" +
          "      <dog>\n" +
          "         <name>Brutus</name>\n" +
          "         <age>9</age>\n" +
          "      </dog>\n" +
          "      <dog>\n" +
          "         <name>Bob</name>\n" +
          "         <age>14</age>\n" +
          "      </dog>\n" +
          "   </dogs>\n" +
          "</person>";

        Map<String, Object> xmlMap = Xml.toGenericMap(xml);
        Map<String, Object> dogs = (Map<String, Object>) xmlMap.get("dogs");
        List<Map<String, Object>> dogList = (List<Map<String, Object>>) dogs.get("dog");
        assertThat(dogList.size()).isEqualTo(3);
    }

    @Test
    void xmlWithAttributes_convertToGenericMap_shouldNotProduceEmptyKeys() {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<root xmlns:SomeNS=\"http://blah.com/something/\">\n" +
          "    <unit>\n" +
          "      <unitId SomeNS:blah=\"12345\">13</unitId>\n" +
          "    </unit>\n" +
          "</root>\n";
        Map<String, Object> xmlMap = Xml.toGenericMap(xml);
        Map<String, Object> unitMap = (Map<String, Object>) xmlMap.get("unit");
        Map<String, Object> unitIdMap = (Map<String, Object>) unitMap.get("unitId");
        assertThat(unitIdMap).doesNotContainKey("");
        assertThat(unitIdMap).containsKey("value");
        assertThat(unitIdMap.get("value")).isEqualTo("13");
    }

    @Data
    static class SomePojo {
        private Integer someInt;
        private String someString;
        private Double someDouble;
        private boolean someBool;
        private String[] someArray;
    }

}