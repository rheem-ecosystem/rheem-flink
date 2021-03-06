package io.rheem.flink.compiler;

import io.rheem.core.function.FunctionDescriptor;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import io.rheem.core.function.ConsumerDescriptor;

import java.io.IOException;
import java.io.Serializable;

/**
 * Wrapper for {@Link OutputFormat}
 */
public class OutputFormatConsumer<T> implements OutputFormat<T>, Serializable {

    private ConsumerDescriptor.SerializableConsumer<T> tConsumer;

    public OutputFormatConsumer(FunctionDescriptor.SerializableConsumer<T> consumer) {
        this.tConsumer = consumer;
    }


    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int i, int i1) throws IOException {
    }

    @Override
    public void writeRecord(T o) throws IOException {
        this.tConsumer.accept(o);
    }

    @Override
    public void close() throws IOException {

    }
}
