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

import nextflow.quilt3.jep.QuiltPackage
import nextflow.quilt3.jep.QuiltParser
import java.nio.file.Files
import java.nio.file.FileSystem
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.ProviderMismatchException
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Global
import nextflow.Session
import nextflow.quilt3.QuiltOpts
/**
 * Implements Path interface for Quilt storage
 *
 * @author Ernest Prabhakar <ernest@quiltdata.io>
 */

@Slf4j
@CompileStatic
public final class QuiltPath implements Path {
    public static final String SEP = '/'

    private final QuiltFileSystem filesystem
    private final String pkg_name
    private final String[] sub_paths
    private final String hash
    private final String tag
    private final String catalog
    private final Map<String,Object> options

    public QuiltPath(QuiltFileSystem filesystem, String pkg_name="", String sub_path = null, Map<String,Object> options = [:]) {
        if ( pkg_name.contains(':')) {
            throw new IllegalArgumentException("Invalid `pkg_name`: $pkg_name")
        }
        this.filesystem = filesystem
        this.pkg_name = pkg_name
        this.sub_paths = sub_path ? sub_path.split(SEP) : new String[0]
        if ( options ) {
            this.options = options
            this.hash = options.get('hash')
            this.tag = options.get('tag')
            this.catalog = options.get('catalog')
        }
        log.info "Creating QuiltPath[$this]: pkg=$pkg_name"
    }

    public String bucket() {
        return filesystem.bucket
    }

    public String pkg_name() {
        return pkg_name
    }

    public QuiltPackage pkg() {
        return isAbsolute() ? QuiltPackage.ForPath(this) : null
    }

    public String file_key() {
        return sub_paths.join(SEP)
    }

    Path installPath() {
        Path pkgPath = pkg().installPath()
        Paths.get(pkgPath.toUriString(), file_key())
    }

    public boolean deinstall() {
        Path path = installPath()
        return Files.deleteIfExists(path)
    }

    public Object option(key) {
        return options ? options[key] : null
    }

    @Override
    FileSystem getFileSystem() {
        return filesystem
    }

    @Override
    boolean isAbsolute() {
        pkg_name != ""
    }

    boolean isPackage() {
        getNameCount() == 0
    }

    QuiltPath getPackage() {
        isPackage() ? this : new QuiltPath(filesystem, pkg_name)
    }

    @Override
    Path getRoot() {
        isAbsolute() ? getPackage() : null
    }

    @Override
    Path getFileName() {
        isPackage() ? null : this
    }

    @Override
    Path getParent() {
        getPackage()
    }

    @Override
    int getNameCount() {
        sub_paths.size()
    }

    @Override
    Path getName(int index) {
        new QuiltPath(filesystem, pkg_name, sub_paths[0,index], options)
    }

    @Override
    Path subpath(int beginIndex, int endIndex) {
        final sub = sub_paths[beginIndex,endIndex].join(SEP)
        new QuiltPath(filesystem, pkg_name, sub, options)
    }

    @Override
    boolean startsWith(Path other) {
        startsWith(other.toString())
    }

    @Override
    boolean startsWith(String other) {
        startsWith(other)
    }

    @Override
    boolean endsWith(Path other) {
        endsWith(other.toString())
    }

    @Override
    boolean endsWith(String other) {
        endsWith(other)
    }

    @Override
    Path normalize() {
        this
    }

    @Override
    QuiltPath resolve(Path other) {
        if( other.class != QuiltPath )
            throw new ProviderMismatchException()

        final that = (QuiltPath)other
        if( other.isAbsolute() )
            return that

        new QuiltPath(filesystem, pkg_name, other.toString(), options)
    }

    @Override
    QuiltPath resolve(String other) {
        new QuiltPath(filesystem, pkg_name, other, options)
    }

    @Override
    Path resolveSibling(Path other) {
      new QuiltPath(filesystem, pkg_name, other.toString(), options)
    }

    @Override
    Path resolveSibling(String other) {
      new QuiltPath(filesystem, pkg_name, other, options)
    }

    @Override
    Path relativize(Path other) {
        return this
        new QuiltPath(filesystem, "", file_key(), options)
    }

    @Override
    String toString() {
        toUriString().replace(QuiltParser.PREFIX,"")
    }

    String toUriString() {
        QuiltPathFactory factory = new QuiltPathFactory()
        factory.toUriString(this)
    }

    @Override
    URI toUri() {
        return new URI(toUriString())
    }

    @Override
    Path toAbsolutePath() {
        if(isAbsolute()) return this
        throw new UnsupportedOperationException("Operation 'toAbsolutePath' is not supported by QuiltPath")
    }

    @Override
    Path toRealPath(LinkOption... options) throws IOException {
        return toAbsolutePath()
    }

    @Override
    File toFile() {
        throw new UnsupportedOperationException("Operation 'toFile' is not supported by QuiltPath")
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>[] events, WatchEvent.Modifier... modifiers) throws IOException {
        throw new UnsupportedOperationException("Operation 'register' is not supported by QuiltPath")
    }

    @Override
    WatchKey register(WatchService watcher, WatchEvent.Kind<?>... events) throws IOException {
        throw new UnsupportedOperationException("Operation 'register' is not supported by QuiltPath")
    }

    @Override
    Iterator<Path> iterator() {
      throw new UnsupportedOperationException("Operation 'iterator' is not supported by QuiltPath")
    }

    @Override
    int compareTo(Path other) {
        return toString() <=> other.toString()
    }
}
