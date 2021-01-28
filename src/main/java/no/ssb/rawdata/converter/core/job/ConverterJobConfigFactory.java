package no.ssb.rawdata.converter.core.job;

import io.micronaut.context.annotation.Factory;
import io.micronaut.core.beans.BeanIntrospection;
import io.micronaut.core.beans.BeanIntrospector;
import io.micronaut.core.beans.BeanProperty;
import io.micronaut.core.beans.BeanWrapper;
import no.ssb.rawdata.converter.core.exception.RawdataConverterException;
import no.ssb.rawdata.converter.core.rawdatasource.RawdataSourceConfig;
import no.ssb.rawdata.converter.util.CloneUtil;

import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * <p>Factory methods to assemble "effective" {@link ConverterJobConfig}s. An effective converter job config is
 * the merged config based on the default config, a base/general config and a specific config.</p>
 */
@Factory
public class ConverterJobConfigFactory {

    /**
     * The grandma of all converter job configs. Defines reasonable defaults. All effective configs are based on this.
     */
    private static final ConverterJobConfig DEFAULT_CONFIG = new ConverterJobConfig("default");

    static {
        DEFAULT_CONFIG
          .setActiveByDefault(true)
        ;
        DEFAULT_CONFIG.getDebug()
          .setDryrun(false)
          .setDevelopmentMode(false)
        ;
        DEFAULT_CONFIG.getConverterSettings()
          .setMaxRecordsBeforeFlush(1000000L)
          .setMaxSecondsBeforeFlush(300L)
          .setRawdataSamples(1)
        ;
        DEFAULT_CONFIG.getRawdataSource()
          .setInitialPosition("LAST")
        ;
        DEFAULT_CONFIG.getRawdataSource()
          ;

    }

    /**
     * All config fragments as defined in application.yml (rawdata.converter.jobs)
     */
    private final List<ConverterJobConfig> jobConfigFragments;

    /**
     * All rawdata-source configs as defined in application.yml (rawdata-sources)
     */
    private final List<RawdataSourceConfig> rawdataSources;

    public ConverterJobConfigFactory(List<ConverterJobConfig> jobConfigFragments, List<RawdataSourceConfig> rawdataSources) {
        this.jobConfigFragments = jobConfigFragments;
        this.rawdataSources = rawdataSources;
    }

    /**
     * Factory method that assemble predefined "effective" converter job configs.
     * "Predefined" means jobs declared in application.yml
     *
     * The opposite of "predefined" is a config that is assembled on-demand using
     * {@link #effectiveConverterJobConfigOf(ConverterJobConfig)}
     *
     * @return A {@link PredefinedConverterJobConfigs} - map that holds all predefined effective ConvertJobConfig objects
     */
    @Singleton
    public PredefinedConverterJobConfigs predefinedJobs() {
        PredefinedConverterJobConfigs effectiveJobConfigs = new PredefinedConverterJobConfigs();

        effectiveJobConfigs.putAll(jobConfigFragments.stream()
          .filter(jobConfig -> !jobConfig.isPrototype())
          .collect(Collectors.toMap(
            jobConfig -> jobConfig.getJobName(),
            jobConfig -> effectiveConverterJobConfigOf(jobConfig)
          )));

        return effectiveJobConfigs;
    }

    /**
     * Compute the "effective" converter job config. An effective converter job config is the merged
     * config based on the inheritance chain starting from the default config, following all configs until the
     * specified config.
     *
     * An effective ConverterJobConfig will *never* be a "prototype".
     *
     * @param jobConfigFragment ConverterJobConfig fragment to compute effective config for
     * @return an effective converter job config
     */
    public ConverterJobConfig effectiveConverterJobConfigOf(ConverterJobConfig jobConfigFragment) {
        List<ConverterJobConfig> inheritanceChain = inheritanceChainOf(jobConfigFragment);
        ConverterJobConfig current = DEFAULT_CONFIG;
        for (int i = inheritanceChain.size(); i-- > 0; ) {
            current = merge(current, inheritanceChain.get(i));
        }

        validateEffectiveJobConfigIntegrity(current);
        return current;
    }

    /**
     * Integrity check a ConverterJobConfig. Throws an Exception if not okay.
     *
     * @param jobConfig ConverterJobConfig to check
     * @throws MissingRequiredPropertiesException if any required properties are missing
     * @throws InvalidConverterJobConfigException if config is invalid
     */
    public void validateEffectiveJobConfigIntegrity(ConverterJobConfig jobConfig) {
        // Validate that required properties exist
        List<String> missingProps = checkMissingProperties(jobConfig);
        if (!missingProps.isEmpty()) {
            throw new MissingRequiredPropertiesException("Missing properties in " + jobConfig.getJobName() + " converter job config: " + missingProps);
        }

        // TODO: Complete this

        // Validate that the referenced rawdata source is configured
        rawdataSources.stream()
          .filter(job -> job.getName().equals(jobConfig.getRawdataSource().getName()))
          .findAny()
          .orElseThrow(() -> new InvalidConverterJobConfigException("Missing rawdata-source definition for " + jobConfig.getRawdataSource().getName()));
    }

    /**
     * Retrieve a ConverterJobConfig fragment by name.
     *
     * @param name name of the ConverterJobConfig to retrieve ({@link ConverterJobConfig#getJobName()})
     * @return a ConverterJobConfig
     * @throws NoConverterJobConfigFoundException if the ConverterJobConfig is unknown
     */
    ConverterJobConfig getJobConfigFragmentByName(String name) {
        return jobConfigFragments.stream()
          .filter(jobConfig -> jobConfig.getJobName().equals(name))
          .findFirst()
          .orElseThrow(() -> new NoConverterJobConfigFoundException("Unknown ConverterJobConfig '" + name + "'"));
    }

    /**
     * Determine inheritance chain of a ConverterJobConfig.
     *
     * @param jobConfig the child jobConfig to determine inheritance chain for
     * @return An ordered List of ConverterJobConfigs starting from child to parents
     * @throws NoConverterJobConfigFoundException if an unknown ConverterJobConfig is referenced as parentConfig
     */
    List<ConverterJobConfig> inheritanceChainOf(ConverterJobConfig jobConfig) {
        List<ConverterJobConfig> inheritanceChain = new ArrayList();
        inheritanceChain.add(jobConfig);

        if (jobConfig.getParent() != null) {
            ConverterJobConfig parentJobConfig = getJobConfigFragmentByName(jobConfig.getParent());
            inheritanceChain.addAll(inheritanceChainOf(parentJobConfig));
        }

        return inheritanceChain;
    }

    /**
     * Checks for missing required properties for an effective converter job config.
     *
     * @param jobConfig the config to check
     * @return a List of missing properties, or empty List if no properties were missing. Never null.
     */
    private List<String> checkMissingProperties(ConverterJobConfig jobConfig) {
        List<String> missingProps = new ArrayList<>();
        if (jobConfig.getRawdataSource().getName() == null) {
            missingProps.add("rawdata-source.name");
        }

        return missingProps;
    }

    /**
     * <p>Merge two ConverterJobConfigs. Note that the argument order matters: The first argument denotes the
     * ConverterJobConfig to use as a starting point. The second argument specifies values that will override the
     * first ConverterJobConfig.</p>
     *
     * <p>This will create a completely new ConverterJobConfig. The objects passed to this method will not be mutated.</p>
     *
     * <p>Utilizes Micronaut bean introspection. Thus: all subclasses of ConverterJobConfig must also be annotated
     * with @Introspected</p>
     *
     * @param base      the ConverterJobConfig to use as a base
     * @param overrides all non-null values of this ConverterJobConfig will override those of the base
     * @return a new ConverterJobConfig with merged values
     */
    static ConverterJobConfig merge(ConverterJobConfig base, ConverterJobConfig overrides) {
        // Create a clone of the base config and use this as starting point
        ConverterJobConfig merged = CloneUtil.deepClone(base);

        BeanWrapper dstBeanWrapper = BeanWrapper.getWrapper(merged);
        BeanWrapper srcBeanWrapper = BeanWrapper.getWrapper(base);

        BeanIntrospection<ConverterJobConfig> introspection = BeanIntrospector.SHARED.getIntrospection(ConverterJobConfig.class);
        introspection.getBeanProperties().forEach(rootProp -> {

            // Merge ConfigElement properties (inner config classes)
            if (ConverterJobConfig.ConfigElement.class.isAssignableFrom(rootProp.getType())) {
                Object srcBean = rootProp.get(overrides);
                Object dstBean = rootProp.get(merged);
                BeanWrapper src = BeanWrapper.getWrapper(srcBean);
                BeanWrapper dst = BeanWrapper.getWrapper(dstBean);

                src.getBeanProperties().forEach(bp -> {
                    BeanProperty subProp = (BeanProperty) bp;
                    Object value = subProp.hasAnnotation(ConverterJobConfig.NotInherited.class) ? null : subProp.get(srcBean);
                    if (value != null) {
                        dst.setProperty(subProp.getName(), value);
                    }
                });
            }

            // Merge map properties, preserving any existing map contents from the base
            else if (Map.class.isAssignableFrom(rootProp.getType())) {
                Object value = rootProp.get(overrides);
                if (value != null) {
                    LinkedHashMap<String, Object> newMap = new LinkedHashMap();
                    srcBeanWrapper.getProperty(rootProp.getName(), Map.class).ifPresent( existingMap -> {
                        newMap.putAll((Map<String, Object>) existingMap);
                    });
                    newMap.putAll((Map<String,Object>) value);
                    dstBeanWrapper.setProperty(rootProp.getName(), newMap);
                }
            }

            // Merge list properties, preserving any existing list contents from the base
            // New list contents will be add before list values from the base, allowing for rule overriding
            // since rules are evaluated using a "first match strategy"
            else if (List.class.isAssignableFrom(rootProp.getType())) {
                Object value = rootProp.get(overrides);
                if (value != null) {
                    ArrayList newList = new ArrayList();
                    newList.addAll((List) value);
                    srcBeanWrapper.getProperty(rootProp.getName(), List.class).ifPresent( existingList -> {
                        newList.addAll((List) existingList);
                    });
                    dstBeanWrapper.setProperty(rootProp.getName(), newList);
                }
            }

            // Merge root properties (non-@Introspected)
            else {
                Object value = rootProp.hasAnnotation(ConverterJobConfig.NotInherited.class) ? null : rootProp.get(overrides);
                if (value != null) {
                    dstBeanWrapper.setProperty(rootProp.getName(), value);
                }
            }

        });

        return merged;
    }

    /**
     * Locate a ConverterJobConfig fragment by name
     *
     * @param name
     * @return
     */
    private Optional<ConverterJobConfig> findConverterJobConfigFragment(String name) {
        return jobConfigFragments.stream()
          .filter(job -> job.getJobName().equalsIgnoreCase(name))
          .findFirst();
    }

    public static class InvalidConverterJobConfigException extends RawdataConverterException {
        public InvalidConverterJobConfigException(String message) {
            super(message);
        }
    }

    public static class MissingRequiredPropertiesException extends RawdataConverterException {
        public MissingRequiredPropertiesException(String msg) {
            super(msg);
        }
    }

    public static class NoConverterJobConfigFoundException extends RawdataConverterException {
        public NoConverterJobConfigFoundException(String msg) {
            super(msg);
        }
    }

}
