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

import jep.JepConfig
import jep.MainInterpreter
import jep.SubInterpreter
import jep.Interpreter

import java.nio.file.Files
import java.nio.file.Path
import java.util.ArrayList

import groovy.transform.CompileStatic
import groovy.transform.Memoized
import groovy.util.logging.Slf4j

@Slf4j
@CompileStatic
class JavaEmbedPython {
    protected static final String MACOS_LIB_PATH = "DYLD_FALLBACK_LIBRARY_PATH"
    protected static final String LINUX_LIB_PATH = "LD_LIBRARY_PATH"

    protected static final String MACOS_LIB = "/jep/libjep.jnilib"
    protected static final String LINUX_LIB = "/jep/libjep.so"
    protected static JavaEmbedPython jep = new JavaEmbedPython()

    private final JepConfig config
    private final Interpreter interp


    static public String MakeCall(String obj, String method, List<String> args = []) {
        def cmd = obj ? "${obj}.${method}" : method
        def arglist = args.join(',')
        "$cmd($arglist)"
    }

    // define the JEP library path
    static public String findPythonPath() {
        String path = System.getenv(MACOS_LIB_PATH)
        if ( !path ) {
            path = System.getenv(LINUX_LIB_PATH)
        }
        if (!path) {
            throw new FileNotFoundException("No Python Path at: `$MACOS_LIB_PATH` or `$LINUX_LIB_PATH`" )
        }
        path
    }

    static public String findJepPath() {
        final String root = findPythonPath()
        String path = root + MACOS_LIB;
        if (!Files.exists(Path.of(path))){
           path = root + LINUX_LIB;
        }
        if (!Files.exists(Path.of(path))){
            throw new FileNotFoundException("No JavaEmbedPython library at: `$path`" )
        }
        path
    }

    static public JavaEmbedPython Context() {
        jep
    }

    static public JavaEmbedPython WithModules(List<String> modules) {
        modules.each { jep.import_module(it) }
        jep
    }

    private JavaEmbedPython() {
        String jepPath = findJepPath() // set path for jep executing python3.9
        MainInterpreter.setJepLibraryPath(jepPath) //initialize the MainInterpreter
        this.config = new JepConfig()
        this.interp = config.createSubInterpreter() //create the interpreter for python executing
    }

    void addSourceDir(String sourceDir) {
        String sourcePath = System.getProperty("user.dir")+sourceDir // set path for python docs with python script to run
        this.config.addIncludePaths(sourcePath)
    }

    void eval(String python_script) {
        log.debug('eval', python_script)
        interp.eval(python_script);
    }

    void import_module(String module) {
        interp.eval("import $module");
    }

    Object getValue(String variable) {
        interp.getValue(variable);
    }

    Object setValue(String variable, String expression) {
        interp.eval("${variable} = ${expression}");
    }

}
