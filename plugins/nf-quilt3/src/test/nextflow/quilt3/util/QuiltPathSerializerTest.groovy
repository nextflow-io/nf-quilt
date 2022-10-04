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

import java.nio.file.Path
import java.nio.file.Paths

import nextflow.Global
import nextflow.Session
import spock.lang.Specification

/**
 *
 * @author Ernest Prabhakar <ernest@quiltdata.io>
 */
class QuiltPathSerializerTest extends QuiltSpecification {

    def 'should serialize a Quilt path'() {
        given:
        Global.session = Mock(Session) {
            getConfig() >> [quilt:[project:'foo', region:'x']]
        }

        when:
        def uri = URI.create("quilt://bucket/pkg/name/sample.csv")
        def path = Paths.get(uri)
        then:
        path instanceof QuiltPath
        path.toUri() == uri
        path.toUriString() == "quilt://bucket/pkg/name/sample.csv"
    }
}