package io.rheem.flink.mapping;

import io.rheem.basic.operators.GroupByOperator;
import io.rheem.core.function.FunctionDescriptor;
import io.rheem.core.mapping.Mapping;
import io.rheem.core.mapping.OperatorPattern;
import io.rheem.core.mapping.PlanTransformation;
import io.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.core.mapping.SubplanPattern;
import io.rheem.flink.operators.FlinkGroupByOperator;
import io.rheem.flink.platform.FlinkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link GroupByOperator} to {@link FlinkGroupByOperator}.
 */
@SuppressWarnings("unchecked")
public class GroupByMapping implements Mapping{
    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                FlinkPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "groupby", new GroupByOperator<>((FunctionDescriptor.SerializableFunction)null, Void.class, Void.class), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }


    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<GroupByOperator>(
                (matchedOperator, epoch) -> new FlinkGroupByOperator<>(matchedOperator).at(epoch)
        );
    }
}
