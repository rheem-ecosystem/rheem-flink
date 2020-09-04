package io.rheem.flink.mapping;

import io.rheem.basic.operators.DoWhileOperator;
import io.rheem.core.function.PredicateDescriptor;
import io.rheem.core.mapping.Mapping;
import io.rheem.core.mapping.OperatorPattern;
import io.rheem.core.mapping.PlanTransformation;
import io.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.core.mapping.SubplanPattern;
import io.rheem.core.types.DataSetType;
import io.rheem.flink.operators.FlinkDoWhileOperator;
import io.rheem.flink.platform.FlinkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link DoWhileOperator} to {@link FlinkDoWhileOperator}.
 */
@SuppressWarnings("unchecked")
public class DoWhileMapping implements Mapping {
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(
                new PlanTransformation(
                        this.createSubplanPattern(),
                        this.createReplacementSubplanFactory(),
                        FlinkPlatform.getInstance()
                )
        );
    }

    @SuppressWarnings("unchecked")
    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "loop", new DoWhileOperator<>(DataSetType.none(), DataSetType.none(), (PredicateDescriptor) null, 1), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<DoWhileOperator<?, ?>>(
                (matchedOperator, epoch) -> new FlinkDoWhileOperator<>(matchedOperator).at(epoch)
        );
    }
}
