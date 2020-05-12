package org.qcri.rheem.flink.mapping;

import org.qcri.rheem.basic.operators.MaterializedGroupByOperator;
import org.qcri.rheem.basic.operators.PageRankOperator;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.flink.operators.FlinkMaterializedGroupByOperator;
import org.qcri.rheem.flink.operators.FlinkPageRankOperator;
import org.qcri.rheem.flink.platform.FlinkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link PageRankOperator} to {@link FlinkPageRankOperator}.
 */
public class PageRankMapping implements Mapping {
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
                "operator", new PageRankOperator(0), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<PageRankOperator>(
                (matchedOperator, epoch) -> new FlinkPageRankOperator(matchedOperator).at(epoch)
        );
    }
}
