package nextflow.quilt3

import groovy.transform.CompileStatic
import nextflow.plugin.BasePlugin
import nextflow.file.FileHelper
import org.pf4j.PluginWrapper
import nextflow.quilt3.util.QuiltFileSystemProvider
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

    @Override
    void start() {
        super.start()
        // register Quilt file system
        FileHelper.getOrInstallProvider(QuiltFileSystemProvider)
    }

}
