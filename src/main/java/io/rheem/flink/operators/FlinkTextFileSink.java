package io.rheem.flink.operators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.TextOutputFormat;
import io.rheem.basic.operators.TextFileSink;
import io.rheem.core.api.Configuration;
import io.rheem.core.function.TransformationDescriptor;
import io.rheem.core.optimizer.OptimizationContext;
import io.rheem.core.optimizer.costs.LoadProfileEstimator;
import io.rheem.core.optimizer.costs.LoadProfileEstimators;
import io.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.core.platform.ChannelDescriptor;
import io.rheem.core.platform.ChannelInstance;
import io.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.core.util.Tuple;
import io.rheem.flink.channels.DataSetChannel;
import io.rheem.flink.execution.FlinkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Implementation of the {@link TextFileSink} operator for the Flink platform.
 */
public class FlinkTextFileSink<Type> extends TextFileSink<Type> implements FlinkExecutionOperator{



    public FlinkTextFileSink(String textFileUrl, TransformationDescriptor<Type, String> formattingDescriptor) {
        super(textFileUrl, formattingDescriptor);
    }

    public FlinkTextFileSink(TextFileSink<Type> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            FlinkExecutor flinkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == 1;
        assert outputs.length == 0;

        DataSet<Type> inputDataset = ((DataSetChannel.Instance) inputs[0]).provideDataSet();

        final TextOutputFormat.TextFormatter<Type> fileOutputFormat = flinkExecutor.getCompiler().compileOutput(this.formattingDescriptor);

        inputDataset.writeAsFormattedText(this.textFileUrl, fileOutputFormat);

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        throw new UnsupportedOperationException("This operator has no outputs.");
    }

    @Override
    public boolean containsAction() {
        return true;
    }



    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                FlinkExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.formattingDescriptor, configuration);
        return optEstimator;
    }

}
