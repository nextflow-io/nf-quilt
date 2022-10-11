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
import nextflow.quilt3.nio.QuiltPath

import groovy.transform.CompileStatic
import groovy.transform.Memoized
import groovy.util.logging.Slf4j
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import java.text.SimpleDateFormat
import java.util.Date
import java.lang.ProcessBuilder

@Slf4j
@CompileStatic
class QuiltParser {
    public static final String SCHEME = 'quilt+s3'
    public static final String SEP = '/'
    public static final String PREFIX = SCHEME+"://"

    private final String bucket
    private final String pkg_name
    private final String[] sub_paths
    private final String hash
    private final String tag
    private final String catalog
    private final Map<String,Object> options

    static public QuiltParser ForString(String uri_string) {
        URI url = new URI(uri_string)
        new QuiltParser(url)
    }

    QuiltParser(URI uri) {
        this.bucket = uri.authority
        this.pkg_name = uri.fragment
    }

    String bucket() {
        bucket ? bucket.toLowerCase() : null
    }

    String toString() {
        "${bucket}#${pkg_name}".replaceAll(/[-\/]/,'_')
    }

    String toUriString() {
        PREFIX + toString()
    }

}
