package io.rheem.flink;


import io.rheem.flink.platform.FlinkPlatform;
import io.rheem.flink.plugin.FlinkBasicPlugin;
import io.rheem.flink.plugin.FlinkConversionPlugin;
import io.rheem.flink.plugin.FlinkGraphPlugin;

/**
 * Register for relevant components of this module.
 */
public class Flink {

    private final static FlinkBasicPlugin PLUGIN = new FlinkBasicPlugin();

    private final static FlinkGraphPlugin GRAPH_PLUGIN = new FlinkGraphPlugin();

    private final static FlinkConversionPlugin CONVERSION_PLUGIN = new FlinkConversionPlugin();

    /**
     * Retrieve the {@link FlinkBasicPlugin}.
     *
     * @return the {@link FlinkBasicPlugin}
     */
    public static FlinkBasicPlugin basicPlugin() {
        return PLUGIN;
    }

    /**
     * Retrieve the {@link FlinkGraphPlugin}.
     *
     * @return the {@link FlinkGraphPlugin}
     */
    public static FlinkGraphPlugin graphPlugin() {
        return GRAPH_PLUGIN;
    }

    /**
     * Retrieve the {@link FlinkConversionPlugin}.
     *
     * @return the {@link FlinkConversionPlugin}
     */
    public static FlinkConversionPlugin conversionPlugin() {
        return CONVERSION_PLUGIN;
    }

    /**
     * Retrieve the {@link FlinkPlatform}.
     *
     * @return the {@link FlinkPlatform}
     */
    public static FlinkPlatform platform() {
        return FlinkPlatform.getInstance();
    }
}
