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
import nextflow.quilt3.nio.QuiltPathFactory
import nextflow.quilt3.nio.QuiltPath

import nextflow.Global
import nextflow.Session
import spock.lang.Unroll
import spock.lang.Shared
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

/**
 *
 * @author Ernest Prabhakar <ernest@quiltdata.io>
 */

class QuiltPackageTest extends QuiltSpecification {
    QuiltPathFactory factory
    QuiltPath qpath
    QuiltPackage pkg

    static JavaEmbedPython jep = JavaEmbedPython.WithModules(['quilt3'])
    static String pkg_url = 'quilt://quilt-example/examples/hurdat/'
    static String url = pkg_url + '/scripts/build.py?hash=058e62ccfa'

    def setup() {
        factory = new QuiltPathFactory()
        qpath = factory.parseUri(url)
        pkg = qpath.pkg()
    }

    @Unroll
    def 'should use a singleton Interpreter' () {
        expect:
        jep
    }

    def 'should create unique Package for associated Paths' () {
        given:
        def pkgPath = qpath.getPackage()
        def pkg2 = pkgPath.pkg()

        expect:
        pkg != null
        pkg.toString() == "quilt_example_examples_hurdat"
        pkgPath.toString() == pkg_url
        pkg == pkg2
    }

    def 'should distinguish Packages with same name in different Buckets ' () {
        given:
        def url2 = url.replace('-example','-example2')
        def qpath2 = factory.parseUri(url2)
        def pkg2 = qpath2.pkg()

        expect:
        url2.toString().contains('-example2')
        pkg != pkg2
        pkg.toString() != pkg2.toString()

        !Files.exists(qpath2.installPath())
    }

    def 'should create an install folder ' () {
        given:
        Path installPath = pkg.installPath()
        String tmpDirsLocation = System.getProperty("java.io.tmpdir")
        expect:
        installPath.toString().startsWith(tmpDirsLocation)
        Files.exists(installPath)
    }

    def 'should install and attribute files ' () {
        expect:
        pkg.install()
        Files.exists(qpath.installPath())
    }

    def 'Package should return Attributes IFF the file exists' () {
    }

}
