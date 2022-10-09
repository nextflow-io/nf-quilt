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

import java.text.SimpleDateFormat
import java.util.Date

@Slf4j
@CompileStatic
class QuiltPackage {
    private static final Map<String,QuiltPackage> packages = [:]
    private static final String installPrefix = "QuiltPackage"
    public static final Path installParent = Files.createTempDirectory(installPrefix)

    private final String bucket
    private final String pkg_name
    private final Path folder
    private boolean installed
    private JavaEmbedPython jep

    static public QuiltPackage ForPath(QuiltPath path) {
        def pkgKey = path.getPackage().toString()
        def pkg = packages.get(pkgKey)
        if( !pkg ) {
            pkg = new QuiltPackage(path.bucket(), path.pkg_name())
            packages[pkgKey] = pkg
        }
        return pkg
    }

    static public String today() {
        Date dateObj =  new Date()
        new SimpleDateFormat('yyyy-MM-dd').format(dateObj)
    }

    QuiltPackage(String bucket, String pkg_name) {
        this.bucket = bucket
        this.pkg_name = pkg_name
        this.folder = Paths.get(installParent.toString(), this.toString())
        Files.createDirectories(this.folder)
        this.installed = false
    }

    Object jep_begin() {
        this.jep = new JavaEmbedPython(['quilt3'])
        jep.setValue(toString(), 'quilt3.Package()')
    }
    
    void jep_end() {
        jep.close()
        this.jep = null
    }

    String arg_name() {
        "'${pkg_name}'"
    }

    String arg_registry() {
        "'s3://${bucket}'"
    }

    String key_dest() {
        "dest='${installPath()}'"
    }

    String key_force() {
        "force=True"
    }

    String key_msg(prefix="") {
        "message='${prefix}@${today()}'"
    }

    String key_name() {
        "name='${pkg_name}'"
    }

    String key_path() {
        "path='${installPath()}'"
    }

    String key_registry() {
        "registry='s3://${bucket}'"
    }

    Object call(String op, List<String> args = []) {
        if ( !jep ) {
            throw new IllegalArgumentException("JavaEmbedPython not initialized")
        }
        String cmd = JavaEmbedPython.MakeCall(toString(),op,args)
        log.debug "`call` ${this}: ${cmd}"
        jep.eval(cmd)
    }

    Path install() {
        jep_begin()
        call('install',[arg_name(),arg_registry(),key_dest()])
        jep_end()
        installPath()
    }

    boolean isInstalled() {
        installed
    }

    Path installPath() {
        folder
    }

    boolean push() {
        log.info "`push` $this"
        try {
            jep_begin()
            call('browse',[key_name(),key_registry()])
            call('set_dir',["'/'",key_path()])
            call('push',[key_name(),key_registry(),key_force()])
            jep_end()
        }
        catch (Exception e) {
            log.error "Failed `push` ${this}: ${e}"
            return false
        }
        return true
    }

    @Override
    String toString() {
        "${bucket}_${pkg_name}".replaceAll(/[-\/]/,'_')
    }

}
