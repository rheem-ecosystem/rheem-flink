package io.rheem.flink.mapping;

import io.rheem.basic.operators.CollectionSource;
import io.rheem.core.mapping.Mapping;
import io.rheem.core.mapping.OperatorPattern;
import io.rheem.core.mapping.PlanTransformation;
import io.rheem.core.mapping.ReplacementSubplanFactory;
import io.rheem.core.mapping.SubplanPattern;
import io.rheem.core.types.DataSetType;
import io.rheem.flink.operators.FlinkCollectionSource;
import io.rheem.flink.platform.FlinkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link CollectionSource} to {@link FlinkCollectionSource}.
 */
@SuppressWarnings("unchecked")
public class CollectionSourceMapping implements Mapping {
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
                "source", new CollectionSource(Collections.emptyList(), DataSetType.none()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<CollectionSource>(
                (matchedOperator, epoch) -> new FlinkCollectionSource<>(matchedOperator).at(epoch)
        );
    }
}
