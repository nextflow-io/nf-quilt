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

import jep.JepConfig;
import jep.MainInterpreter;
import jep.SubInterpreter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;

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

    private JepConfig config
    private SubInterpreter subInterp

    //define the JEP library path
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

    JavaEmbedPython(String sourceDir = null) {
        String jepPath = findJepPath() // set path for jep executing python3.9
        MainInterpreter.setJepLibraryPath(jepPath) //initialize the MainInterpreter
        this.config = new JepConfig()
        if ( sourceDir ) {
            String sourcePath = System.getProperty("user.dir")+sourceDir // set path for python docs with python script to run
            this.config.addIncludePaths(sourcePath)
        }
        this.subInterp = config.createSubInterpreter() //create the interpreter for python executing
    }


    void eval(String python_script) {
        subInterp.eval(python_script);
    }

    Object getValue(String variable) {
        subInterp.getValue(variable);
    }

    Object setValue(String variable, String expression) {
        subInterp.eval("${variable} = ${expression}");
    }

}
