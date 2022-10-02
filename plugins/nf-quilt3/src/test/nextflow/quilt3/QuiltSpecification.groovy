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

package nextflow.quilt3

import java.nio.file.Paths

import spock.lang.Specification

import java.nio.file.FileSystem
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.spi.FileSystemProvider

/**
 *
 * @author Ernest Prabhakar <ernest@quiltdata.io>
 */
abstract class QuiltSpecification extends Specification {

    protected Path mockQuiltPath(String path, boolean isDir=false) {
        assert path.startsWith('quilt://')

        def tokens = path.tokenize('/')
        def bucket = tokens[1]
        def file = '/' + tokens[2..-1].join('/')

        def attr = Mock(BasicFileAttributes)
        attr.isDirectory() >> isDir
        attr.isRegularFile() >> !isDir
        attr.isSymbolicLink() >> false

        def provider = Mock(FileSystemProvider)
        provider.getScheme() >> 'quilt'
        provider.readAttributes(_, _, _) >> attr

        def fs = Mock(FileSystem)
        fs.provider() >> provider
        fs.toString() >> ('quilt://' + bucket)
        def uri = GroovyMock(URI)
        uri.toString() >> path


        def result = GroovyMock(Path)
        result.bucket() >> bucket
        result.toUriString() >> path
        result.toString() >> file
        result.getFileSystem() >> fs
        result.toUri() >> uri
        result.resolve(_) >> { mockQuiltPath("$path/${it[0]}") }
        result.toAbsolutePath() >> result
        result.asBoolean() >> true
        result.getParent() >> { def p=path.lastIndexOf('/'); p!=-1 ? mockQuiltPath("${path.substring(0,p)}", true) : null }
        result.getFileName() >> { Paths.get(tokens[-1]) }
        result.getName() >> tokens[1]
        return result
    }


    static CloudStoragePath gsPath(String path) {
        assert path.startsWith('quilt://')
        (CloudStoragePath) Paths.get( new URI(null,null,path,null,null))
    }

}
