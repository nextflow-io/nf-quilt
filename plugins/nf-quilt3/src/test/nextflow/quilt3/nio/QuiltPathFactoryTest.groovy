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

package nextflow.quilt3.nio
import nextflow.quilt3.QuiltSpecification

import nextflow.Global
import nextflow.Session
import spock.lang.Unroll

/**
 *
 * @author Ernest Prabhakar <ernest@quiltdata.io>
 */

class QuiltPathFactoryTest extends QuiltSpecification {

    @Unroll
    def 'should decompose Quilt URLs' () {
        given:
        def factory = new QuiltPathFactory()
        and:
        def url = 'quilt://bucket/pkg/name/optional/file/key?hash=hexcode&summarize=pattern1&summarize=pattern2&metadata=filename.json'
        and:
        def qpath = factory.parseUri(url)
        expect:
        qpath != null
        qpath.bucket() == 'bucket'
        qpath.pkg_name() == 'pkg/name'
        qpath.file_key() == 'optional/file/key'
        qpath.option('hash') == 'hexcode'
        qpath.option('metadata') == 'filename.json'
        qpath.option('summarize') == 'pattern2' // should be a list
    }

    def 'should create quilt path #PATH' () {
        given:
        Global.session = Mock(Session) {
            getConfig() >> [quilt:[project:'foo', region:'x']]
        }
        and:
        def factory = new QuiltPathFactory()

        expect:
        //factory.parseUri(PATH).toUriString() == PATH
        factory.parseUri(PATH).toString() == STR

        where:
        _ | PATH                                       | STR
        _ | 'quilt://reg/user/pkg/'                    | 'quilt://reg/user/pkg/'
        _ | 'quilt://reg/user/pkg'                     | 'quilt://reg/user/pkg/'
        _ | 'quilt://reg/pkg/name/opt/file/key'       | 'quilt://reg/pkg/name/opt/file/key'
        _ | 'quilt://reg/user/pkg?hash=hex'            | 'quilt://reg/user/pkg/'
    }

}
