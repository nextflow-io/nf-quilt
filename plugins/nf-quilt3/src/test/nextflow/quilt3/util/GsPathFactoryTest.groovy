/*
 * Copyright 2020-2022, Seqera Labs
 * Copyright 2013-2019, Centre for Genomic Regulation (CRG)
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

package nextflow.quilt3.util

import nextflow.Global
import nextflow.Session
import spock.lang.Specification
import spock.lang.Unroll

/**
 *
 * @author Ernest Prabhakar <ernest@quiltdata.io>
 */
class QuiltPathFactoryTest extends Specification {

    @Unroll
    def 'should create gs path #PATH' () {
        given:
        Global.session = Mock(Session) {
            getConfig() >> [google:[project:'foo', region:'x']]
        }
        and:
        def factory = new QuiltPathFactory()

        expect:
        factory.parseUri(PATH).toUriString() == PATH
        factory.parseUri(PATH).toString() == STR

        where:
        _ | PATH                | STR
        _ | 'quilt://foo'          | ''
        _ | 'quilt://foo/bar'      | '/bar'
        _ | 'quilt://foo/bar/'     | '/bar/'   // <-- bug or feature ?
        _ | 'quilt://foo/b a r'    | '/b a r'
        _ | 'quilt://f o o/bar'    | '/bar'
        _ | 'quilt://f_o_o/bar'    | '/bar'
    }

    def 'should use requester pays' () {
        given:
        Global.session = Mock(Session) {
            getConfig() >> [google:[project:'foo', region:'x', enableRequesterPaysBuckets:true]]
        }

        when:
        def storageConfig = QuiltPathFactory.getCloudStorageConfig()

        then:
        storageConfig.userProject() == 'foo'
    }

    def 'should not use requester pays' () {
        given:
        def sess = new Session()
        sess.config = [google:[project:'foo', region:'x', lifeSciences: [:]]]
        Global.session = sess

        when:
        def storageConfig = QuiltPathFactory.getCloudStorageConfig()

        then:
        storageConfig.userProject() == null
    }
}
