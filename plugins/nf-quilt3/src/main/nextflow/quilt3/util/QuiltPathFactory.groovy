/*
 * Copyright 2019, Google Inc
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

import groovy.transform.CompileStatic
import nextflow.Global
import nextflow.Session
import nextflow.quilt3.QuiltOpts
import nextflow.file.FileSystemPathFactory
/**
 * Implements FileSystemPathFactory interface for Google storage
 *
 * @author Ernest Prabhakar <ernest@quiltdata.io>
 */
@CompileStatic
class QuiltPathFactory extends FileSystemPathFactory {

    @Override
    protected Path parseUri(String uri) {
      if( !uri.startsWith('quilt://') )
          return null
      final body = uri.substring(8)
      final reg_split = body.indexOf('/')
      if( reg_split==-1 )
          return null

      final registry = body.substring(0,reg_split)
      final pkg_path = body.substring(reg_split)
      final pkg_split = pkg_path.indexOf('/', pkg_path.indexOf("/") + 1)
      if( pkg_split==-1 )
          return null

      final pkg_name = pkg_path.substring(0,pkg_split)
      final path = pkg_path.substring(pkg_split)
      return QuiltFileSystem.forBucket(registry).getPath(pkg_name, path)
    }

    @Override
    protected String toUriString(Path p) {
      if( p instanceof QuiltPath ) {
          return "quilt://${p.registry()}/${p.pkg_name}/${p.path}".toString()
      }
      return null
    }
}
