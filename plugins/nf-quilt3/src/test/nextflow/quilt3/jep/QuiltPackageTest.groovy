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

/**
 *
 * @author Ernest Prabhakar <ernest@quiltdata.io>
 */

class QuiltPackageTest extends QuiltSpecification {
    QuiltPathFactory factory
    QuiltPath qpath
    QuiltPackage pkg

    def setup() {
        factory = new QuiltPathFactory()
        qpath = factory.parseUri(path)
        pkg = QuiltPackage.ForPath(qpath)
    }

    static JavaEmbedPython jep = JavaEmbedPython.WithModules(['quilt3'])
    static String path = 'quilt://bucket/pkg/name/file/path?hash=hexcode'

    @Unroll
    def 'should create a singleton Interpreter' () {
        expect:
        jep
    }

    def 'should create unique Package for associated Paths' () {
        given:
        def pkgPath = qpath.getPackage()
        def pkg2 = QuiltPackage.ForPath(pkgPath)

        expect:
        pkg != null
        pkg.toString() == "bucket_pkg_name"
        pkgPath.toString() == "quilt://bucket/pkg/name/"
        pkg == pkg2
    }

    def 'should distinguish Packages with same name in different Buckets ' () {
        given:
        def path2 = path.replace('bucket','bucket2')
        def qpath2 = factory.parseUri(path2)
        def pkg2 = QuiltPackage.ForPath(qpath2)

        expect:
        path2.toString().contains('bucket2')
        pkg != pkg2
        pkg.toString() != pkg2.toString()
    }

    def 'Package should install into a staging directory' () {
        given:
        String tmpdir = Files.createTempDirectory("tmpDirPrefix").toFile().getAbsolutePath()
        String tmpDirsLocation = System.getProperty("java.io.tmpdir")
        expect:
        tmpdir.startsWith(tmpDirsLocation)
    }

    def 'Package should return Attributes IFF the file exists' () {
    }

}
