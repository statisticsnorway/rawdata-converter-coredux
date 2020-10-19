package no.ssb.rawdata.converter.micronaut.typeconverter;


import io.micronaut.context.annotation.Factory;
import no.ssb.dlp.pseudo.core.typeconverter.PseudoFuncRuleTypeConverter;
import no.ssb.dlp.pseudo.core.typeconverter.PseudoSecretTypeConverter;

import javax.inject.Singleton;

@Factory
public class TypeConverterFactory {

    @Singleton
    PseudoFuncRuleTypeConverter pseudoFuncRuleTypeConverter() {
        return new PseudoFuncRuleTypeConverter();
    }

    @Singleton
    PseudoSecretTypeConverter pseudoSecretTypeConverter() {
        return new PseudoSecretTypeConverter();
    }

}
