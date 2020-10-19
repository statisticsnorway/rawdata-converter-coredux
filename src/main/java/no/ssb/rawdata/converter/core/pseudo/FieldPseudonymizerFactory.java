package no.ssb.rawdata.converter.core.pseudo;

import io.micronaut.context.annotation.Property;
import no.ssb.dlp.pseudo.core.FieldPseudonymizer;
import no.ssb.dlp.pseudo.core.PseudoSecret;
import no.ssb.rawdata.converter.core.job.ConverterJobConfig;

import javax.inject.Singleton;
import java.util.List;

@Singleton
public class FieldPseudonymizerFactory {

    private final List<PseudoSecret> pseudoSecrets;

    public FieldPseudonymizerFactory(@Property(name="pseudo-secrets") List<PseudoSecret> pseudoSecrets) {
        this.pseudoSecrets = pseudoSecrets;
    }

    public FieldPseudonymizer newFieldPseudonymizer(ConverterJobConfig jobConfig) {
        return new FieldPseudonymizer.Builder()
          .rules(jobConfig.getPseudoRules())
          .secrets(pseudoSecrets)
          .build();
    }

}
