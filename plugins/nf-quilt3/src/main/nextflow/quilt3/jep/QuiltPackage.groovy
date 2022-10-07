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
import java.nio.file.Paths

@Slf4j
@CompileStatic
class QuiltPackage {
    static JavaEmbedPython jep = JavaEmbedPython.WithModules(['quilt3'])

    static private Map<String,QuiltPackage> packages = [:]
    static public String installFolder = ".quilt"

    private String bucket
    private String pkg_name
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
        this.installed = false
    }

    boolean isInstalled() {
        installed
    }

    String installPath() {
        Paths.get(installFolder, toString()).toString()
    }

    @Override
    String toString() {
        "${bucket}_${pkg_name}".replace("/","_")
    }

}
