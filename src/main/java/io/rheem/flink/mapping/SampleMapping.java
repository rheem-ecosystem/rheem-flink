package io.rheem.flink.mapping;

import io.rheem.basic.operators.SampleOperator;
import io.rheem.core.mapping.Mapping;
import io.rheem.core.mapping.OperatorPattern;
import io.rheem.core.mapping.PlanTransformation;
import io.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.core.mapping.SubplanPattern;
import io.rheem.core.types.DataSetType;
import io.rheem.flink.operators.FlinkSampleOperator;
import io.rheem.flink.platform.FlinkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link SampleOperator} to {@link FlinkSampleOperator}.
 */
@SuppressWarnings("unchecked")
public class SampleMapping implements Mapping{
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                FlinkPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern<>(
                "sample", new SampleOperator<>(0, DataSetType.none(), null, 0L), false
        ).withAdditionalTest(op ->
                op.getSampleMethod() == SampleOperator.Methods.RANDOM
             || op.getSampleMethod() == SampleOperator.Methods.BERNOULLI
             || op.getSampleMethod() == SampleOperator.Methods.ANY
             || op.getSampleMethod() == SampleOperator.Methods.RESERVOIR
        ); //TODO: check if the zero here affects execution
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<SampleOperator>(
                (matchedOperator, epoch) -> {
                    return new FlinkSampleOperator<>(matchedOperator);
                }
        );
    }
}
