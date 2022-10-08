/*
 * Copyright 2022, Quilt Data Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// https://medium.com/geekculture/how-to-execute-python-modules-from-java-2384041a3d6d
// package nextflow.quilt3.jep

package nextflow.quilt3.jep
import nextflow.quilt3.nio.QuiltPath

import jep.Interpreter;
import groovy.transform.CompileStatic
import groovy.transform.Memoized
import groovy.util.logging.Slf4j
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

@Slf4j
@CompileStatic
class QuiltPackage {
    static JavaEmbedPython jep = JavaEmbedPython.WithModules(['quilt3'])

    private static final Map<String,QuiltPackage> packages = [:]
    private static final String installPrefix = "QuiltPackage"
    public static final Path installParent = Files.createTempDirectory(installPrefix)

    private final String bucket
    private final String pkg_name
    private final Path folder
    private boolean installed

    static public QuiltPackage ForPath(QuiltPath path) {
        def pkgKey = path.getPackage().toString()
        def pkg = packages.get(pkgKey)
        if( !pkg ) {
            pkg = new QuiltPackage(path.bucket(), path.pkg_name())
            packages[pkgKey] = pkg
        }
        return pkg
    }

    QuiltPackage(String bucket, String pkg_name) {
        this.bucket = bucket
        this.pkg_name = pkg_name
        this.folder = Paths.get(installParent.toString(), this.toString())
        Files.createDirectories(this.folder)
        this.installed = false
    }

    Path install() {
        String dest = "dest='${installPath()}'"
        String registry = "'s3://${bucket}'"
        String pkg = "'${pkg_name}'"
        List<String> args = [pkg,registry,dest]
        String cmd = JavaEmbedPython.MakeCall('quilt3.Package','install',args)
        jep.setValue(toString(), cmd)
        installPath()
    }

    boolean isInstalled() {
        installed
    }

    Path installPath() {
        folder
    }

    boolean push() {
        log.info "Mock `push` $this"
        true
    }

    @Override
    String toString() {
        "${bucket}_${pkg_name}".replaceAll(/[-\/]/,'_')
    }

}
