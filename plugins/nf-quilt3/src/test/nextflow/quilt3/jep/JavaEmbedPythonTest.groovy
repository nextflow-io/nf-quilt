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

package nextflow.quilt3.jep
import nextflow.quilt3.QuiltSpecification

import java.nio.file.Path
import java.nio.file.Paths
import nextflow.Global
import nextflow.Session

/*
//import  .py doc with to run
subInterp.eval("import python_functions as p");

// run each function from the .py doc I
subInterp.eval("res_spacy = p.run_spacy_nlp('Apple is looking at buying U.K. startup for one billion')");
System.out.println(subInterp.getValue("res_spacy"));

//II
subInterp.eval("res_c = p.get_c_path('.idea','*.xml')");
System.out.println(subInterp.getValue("res_c"));
*/

/**
 *
 * @author Ernest Prabhakar <ernest@quiltdata.io>
 */
class JavaEmbedPythonTest extends QuiltSpecification {
    static JavaEmbedPython jep = new JavaEmbedPython()

    def 'should be able to compile and run wrapper class'() {
        expect:
        jep
    }

    def 'should findPythonPath'() {
        when:
        def path = nextflow.quilt3.jep.JavaEmbedPython.findPythonPath()
        then:
        path.contains("python")
    }

    def 'should findJepPath'() {
        when:
        def path = nextflow.quilt3.jep.JavaEmbedPython.findJepPath()
        then:
        path.contains("jep")
    }

    def 'should import quilt'() {
        expect:
        jep.eval("import quilt3")
    }

    def 'should evaluate expressions'() {
        when:
        jep.setValue("x", "2 + 2")
        and:
        def x = jep.getValue("x")
        then:
        x == 4
    }

    def 'should return quilt config as Map'() {
        when:
        jep.eval("import quilt3")
        and:
        jep.setValue("cf", "quilt3.config(navigator_url='https://example.com')")
        and:
        def config = jep.getValue("cf")
        then:
        config['default_local_registry'].contains("packages")
        config['navigator_url'] == 'https://example.com'
    }

}
