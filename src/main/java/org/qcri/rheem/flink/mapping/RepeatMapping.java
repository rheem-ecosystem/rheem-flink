package org.qcri.rheem.flink.mapping;

import org.qcri.rheem.basic.operators.RepeatOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.flink.operators.FlinkRepeatExpandedOperator;
import org.qcri.rheem.flink.operators.FlinkRepeatOperator;
import org.qcri.rheem.flink.platform.FlinkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link RepeatOperator} to {@link FlinkRepeatOperator}.
 */
@SuppressWarnings("unchecked")
public class RepeatMapping implements Mapping{
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
                "repeat", new RepeatOperator<>(1, DataSetType.none()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }


    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<RepeatOperator>(
                (matchedOperator, epoch) -> new FlinkRepeatExpandedOperator<>(matchedOperator).at(epoch)
        );
    }
}
