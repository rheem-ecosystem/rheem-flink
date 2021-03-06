package io.rheem.flink.operators;

import org.apache.commons.lang3.Validate;
import org.apache.flink.api.java.DataSet;
import io.rheem.core.api.Configuration;
import io.rheem.core.optimizer.OptimizationContext;
import io.rheem.core.optimizer.cardinality.CardinalityEstimator;
import io.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import io.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import io.rheem.core.platform.ChannelDescriptor;
import io.rheem.core.platform.ChannelInstance;
import io.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.core.types.DataSetType;
import io.rheem.core.util.Tuple;
import io.rheem.flink.channels.DataSetChannel;
import io.rheem.flink.execution.FlinkExecutor;
import io.rheem.java.channels.CollectionChannel;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;


/**
 * Converts {@link DataSetChannel} into a {@link CollectionChannel}
 */
public class FlinkCollectionSink<Type> extends UnaryToUnaryOperator<Type, Type>
        implements FlinkExecutionOperator  {
    public FlinkCollectionSink(DataSetType<Type> type) {
        super(type, type, false);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FlinkExecutor flinkExecutor, OptimizationContext.OperatorContext operatorContext) throws Exception {

        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final DataSetChannel.Instance input  = (DataSetChannel.Instance) inputs[0];
        final CollectionChannel.Instance output = (CollectionChannel.Instance) outputs[0];

        final DataSet<Type> dataSetInput = input.provideDataSet();

        output.accept(dataSetInput.filter(a -> true).setParallelism(1).collect());

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);

    }

    @Override
    public boolean containsAction() {
        return true;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, 0, outputIndex);
        return Optional.of(new DefaultCardinalityEstimator(1d, 1, this.isSupportingBroadcastInputs(),
                inputCards -> inputCards[0]));
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.flink.collect.load";
    }
}
