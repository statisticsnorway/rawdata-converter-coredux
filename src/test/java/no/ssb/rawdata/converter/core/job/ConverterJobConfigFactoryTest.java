package no.ssb.rawdata.converter.core.job;

import no.ssb.rawdata.converter.core.job.ConverterJobConfigFactory.InvalidConverterJobConfigException;
import no.ssb.rawdata.converter.core.job.ConverterJobConfigFactory.MissingRequiredPropertiesException;
import no.ssb.rawdata.converter.core.rawdatasource.RawdataSourceConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConverterJobConfigFactoryTest {


    @Test
    void findInheritanceChain_shouldDetermineCorrectInheritanceOrder() {
        ConverterJobConfig base = new ConverterJobConfig("base")
          .setPrototype(true);
        ConverterJobConfig secondBase = new ConverterJobConfig("secondBase")
          .setPrototype(true)
          .setParent(base.getName());
        ConverterJobConfig config1 = new ConverterJobConfig("config1")
          .setParent(secondBase.getName());
        ConverterJobConfig config2 = new ConverterJobConfig("config2")
          .setParent(config1.getName());
        ConverterJobConfig config3 = new ConverterJobConfig("config3");

        ConverterJobConfigFactory factory = new ConverterJobConfigFactory(List.of(base, config2, secondBase, config1), List.of());
        assertThat(converterJobConfigNamesOf(factory.inheritanceChainOf(config2))).containsExactly("config2", "config1", "secondBase", "base");
        assertThat(converterJobConfigNamesOf(factory.inheritanceChainOf(base))).containsExactly("base");
        assertThat(converterJobConfigNamesOf(factory.inheritanceChainOf(config3))).containsExactly("config3");
    }

    private List<String> converterJobConfigNamesOf(List<ConverterJobConfig> converterJobConfigs) {
        return converterJobConfigs.stream()
          .map(ConverterJobConfig::getName)
          .collect(Collectors.toList());
    }

    @Test
    void inheritanceChain_shouldAssembleEffectiveJobConfigs() {
        ConverterJobConfig base = new ConverterJobConfig("base")
          .setPrototype(true)
          .setActiveByDefault(true);
        base.getConverterSettings()
          .setRawdataSamples(1);
        base.getConverterSettings()
          .setMaxRecordsBeforeFlush(1000L)
          .setMaxSecondsBeforeFlush(60L);

        ConverterJobConfig config1 = new ConverterJobConfig("config1")
          .setParent(base.getName())
          .setActiveByDefault(true);
        config1.getDebug()
          .setDryrun(true);
        config1.getConverterSettings()
          .setMaxRecordsBeforeFlush(1000L)
          .setMaxSecondsBeforeFlush(60L);

        ConverterJobConfig config2 = new ConverterJobConfig("config2")
          .setPrototype(true)
          .setParent(config1.getName())
          .setActiveByDefault(false);
        config2.getDebug()
          .setFailedMessagesStoragePath("file:///tmp");
        config2.getConverterSettings()
          .setMaxRecordsBeforeFlush(9999L);
        config1.getRawdataSource()
          .setName("someRawdataSource");

        List<ConverterJobConfig> converterJobConfigs = List.of(base, config1, config2);
        List<RawdataSourceConfig> rawdataSourceConfigs = List.of(new RawdataSourceConfig("someRawdataSource"));
        ConverterJobConfigFactory factory = new ConverterJobConfigFactory(converterJobConfigs, rawdataSourceConfigs);

        ConverterJobConfig effectiveJobConfig =  factory.effectiveConverterJobConfigOf(config2);

        assertThat(effectiveJobConfig.getName()).isEqualTo("config2");
        assertThat(effectiveJobConfig.getPrototype()).isNull();
        assertThat(effectiveJobConfig.getParent()).isEqualTo("config1");
        assertThat(effectiveJobConfig.getActiveByDefault()).isFalse();
        assertThat(effectiveJobConfig.getDebug().getFailedMessagesStoragePath()).isEqualTo("file:///tmp");
        assertThat(effectiveJobConfig.getDebug().getDryrun()).isTrue();
        assertThat(effectiveJobConfig.getDebug().getLogRawdataContentAllowed()).isFalse();
        assertThat(effectiveJobConfig.getConverterSettings().getRawdataSamples()).isEqualTo(1);
        assertThat(effectiveJobConfig.getConverterSettings().getMaxSecondsBeforeFlush()).isEqualTo(60L);
        assertThat(effectiveJobConfig.getConverterSettings().getMaxRecordsBeforeFlush()).isEqualTo(9999L);
    }


    @Test
    void merge_shouldProduceMergedJobConfigsWithoutSideEffects() {
        ConverterJobConfig config1 = new ConverterJobConfig("config1")
          .setActiveByDefault(true);
        config1.getDebug()
          .setDryrun(true);
        config1.getConverterSettings()
          .setMaxRecordsBeforeFlush(1000L)
          .setMaxSecondsBeforeFlush(60L);
        int hash1 = config1.hashCode();

        ConverterJobConfig config2 = new ConverterJobConfig("config2")
          .setActiveByDefault(false);
        config2.getDebug()
          .setFailedMessagesStoragePath("file:///tmp");
        config2.getConverterSettings()
          .setMaxRecordsBeforeFlush(9999L);
        int hash2 = config2.hashCode();

        ConverterJobConfig merged = ConverterJobConfigFactory.merge(config1, config2);
        assertThat(merged).hasFieldOrPropertyWithValue("activeByDefault", false);
        assertThat(merged.getConverterSettings()).hasFieldOrPropertyWithValue("maxRecordsBeforeFlush", 9999L);
        assertThat(merged.getConverterSettings()).hasFieldOrPropertyWithValue("maxSecondsBeforeFlush", 60L);

        assertThat(config1.hashCode())
          .withFailMessage("Base ConverterJobConfig is not expected to change during merge operation")
          .isEqualTo(hash1);

        assertThat(config2.hashCode())
          .withFailMessage("Override ConverterJobConfig is not expected to change during merge operation")
          .isEqualTo(hash2);
    }

    @Test
    void configWithRequiredPropertiesMissing_assembleEffectiveConfig_shouldThrowValidationException() {
        ConverterJobConfigFactory factory = new ConverterJobConfigFactory(List.of(), List.of(new RawdataSourceConfig("someRawdataSource")));
        ConverterJobConfig jobConfig = new ConverterJobConfig("someJobConfig")
          .setActiveByDefault(true);

        MissingRequiredPropertiesException e = assertThrows(MissingRequiredPropertiesException.class, () -> {
            factory.effectiveConverterJobConfigOf(jobConfig);
        });
        assertThat(e.getMessage()).startsWith("Missing properties in someJobConfig converter job config:");
    }

    @Test
    void configWithMismatchedRawdataSourceDefinition_assembleEffectiveConfig_shouldThrowValidationException() {
        ConverterJobConfigFactory factory = new ConverterJobConfigFactory(List.of(), List.of(new RawdataSourceConfig("someRawdataSource")));
        ConverterJobConfig jobConfig = new ConverterJobConfig("someJobConfig")
          .setActiveByDefault(true);
        jobConfig.getRawdataSource().setName("blah");

        InvalidConverterJobConfigException e = assertThrows(InvalidConverterJobConfigException.class, () -> {
            factory.effectiveConverterJobConfigOf(jobConfig);
        });
        assertThat(e.getMessage()).isEqualTo("Missing rawdata-source definition for blah");

        jobConfig.getRawdataSource().setName("someRawdataSource");
        factory.effectiveConverterJobConfigOf(jobConfig); // should not throw exception
    }

}