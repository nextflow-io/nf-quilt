package nextflow.quilt3

import groovy.transform.CompileStatic
import nextflow.plugin.BasePlugin
import org.pf4j.PluginWrapper
/**
 * Implement the plugin entry point for Quilt support
 *
 * @author Ernest Prabhakar <ernest@quiltdata.io>
 */
@CompileStatic
class QuiltPlugin extends BasePlugin {

    QuiltPlugin(PluginWrapper wrapper) {
        super(wrapper)
    }

}
