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

import jep.Interpreter;

import groovy.transform.CompileStatic
import groovy.transform.Memoized
import groovy.util.logging.Slf4j

@Slf4j
@CompileStatic
class QuiltPackage {
    private Interpreter jep

    QuiltPackage(Interpreter jep) {
        this.jep = jep
    }

    void eval(String python_script) {
        jep.eval(python_script);
    }

    Object getValue(String variable) {
        jep.getValue(variable);
    }

    Object setValue(String variable, String expression) {
        jep.eval("${variable} = ${expression}");
    }
}
