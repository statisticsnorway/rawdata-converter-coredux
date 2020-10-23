package no.ssb.rawdata.converter.util;

import lombok.experimental.UtilityClass;

import java.util.Optional;
import java.util.Properties;

@UtilityClass
public class MavenArtifactUtil {

    public static Optional<String> findArtifactVersion(String groupId, String artifactId) {
        try {
            Properties props = new Properties();
            String propFile = "META-INF/maven/" + groupId + "/" + artifactId + "/pom.properties";
            props.load(MavenArtifactUtil.class.getClassLoader().getResourceAsStream(propFile));
            return Optional.ofNullable(props.getProperty("version"));
        }
        catch (Exception e) {
            /* Swallowing this exception is okay */
            return Optional.empty();
        }
    }

}
