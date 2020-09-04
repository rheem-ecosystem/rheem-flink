package io.rheem.flink.mapping;

import io.rheem.basic.operators.GlobalMaterializedGroupOperator;
import io.rheem.core.mapping.Mapping;
import io.rheem.core.mapping.OperatorPattern;
import io.rheem.core.mapping.PlanTransformation;
import io.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.core.mapping.SubplanPattern;
import io.rheem.core.types.DataSetType;
import io.rheem.flink.operators.FlinkGlobalMaterializedGroupOperator;
import io.rheem.flink.platform.FlinkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link GlobalMaterializedGroupOperator} to {@link FlinkGlobalMaterializedGroupOperator}.
 */
@SuppressWarnings("unchecked")
public class GlobalMaterializedGroupMapping implements Mapping {
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
                "group", new GlobalMaterializedGroupOperator<>(DataSetType.none(), DataSetType.groupedNone()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<GlobalMaterializedGroupOperator>(
                (matchedOperator, epoch) -> new FlinkGlobalMaterializedGroupOperator<>(matchedOperator).at(epoch)
        );
    }
}
