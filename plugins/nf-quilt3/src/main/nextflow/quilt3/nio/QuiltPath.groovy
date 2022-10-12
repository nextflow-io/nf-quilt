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
import nextflow.quilt3.jep.QuiltParser
/**
 * Implements Path interface for Quilt storage
 *
 * @author Ernest Prabhakar <ernest@quiltdata.io>
 */

@Slf4j
@CompileStatic
public final class QuiltPath implements Path {
    private final QuiltFileSystem filesystem
    private final QuiltParser parsed
    private final String[] paths

    public QuiltPath(QuiltFileSystem filesystem, QuiltParser parsed) {
        this.filesystem = filesystem
        this.parsed = parsed
        this.paths = parsed.paths()
        log.info "Creating QuiltPath[$parsed]@$filesystem"
    }

    public String bucket() {
        filesystem ? filesystem.bucket : null
    }

    public String pkg_name() {
        parsed.pkg_name()
    }

    public String sub_paths() {
        parsed.path()
    }

    public QuiltPackage pkg() {
        isAbsolute() ? QuiltPackage.ForPath(this) : null
    }

    public String file_key() {
        parsed.toString()
    }

    Path installPath() {
        Path pkgPath = pkg().installPath()
        Paths.get(pkgPath.toUriString(), sub_paths())
    }

    public boolean deinstall() {
        Path path = installPath()
        return Files.deleteIfExists(path)
    }

    @Override
    FileSystem getFileSystem() {
        return filesystem
    }

    @Override
    boolean isAbsolute() {
        filesystem && pkg_name() != ""
    }

    boolean isJustPackage() {
        !parsed.hasPath()
    }

    QuiltPath getJustPackage() {
        if ( isJustPackage() ) return this
        QuiltParser pkg_parsed = QuiltParser.ForString(parsed.toPackageString())
        new QuiltPath(filesystem, pkg_parsed)
    }

    @Override
    Path getRoot() {
        isAbsolute() ? getJustPackage() : null
    }

    @Override
    Path getFileName() {
        throw new UnsupportedOperationException("Operation 'getFileName' is not supported by QuiltPath")
        isJustPackage() ? null : this
    }

    @Override
    Path getParent() {
        throw new UnsupportedOperationException("Operation 'getParent' is not supported by QuiltPath")
    }

    @Override
    int getNameCount() {
        paths.size()
    }

    @Override
    Path getName(int index) {
        throw new UnsupportedOperationException("Operation 'getName' is not supported by QuiltPath")
        //new QuiltPath(filesystem, pkg_name, sub_paths[0,index], options)
    }

    @Override
    Path subpath(int beginIndex, int endIndex) {
        throw new UnsupportedOperationException("Operation 'subpath' is not supported by QuiltPath")
        //final sub = sub_paths[beginIndex,endIndex].join(SEP)
        //new QuiltPath(filesystem, pkg_name, sub, options)
    }

    @Override
    boolean startsWith(Path other) {
        startsWith(other.toString())
    }

    @Override
    boolean startsWith(String other) {
        toString().startsWith(other)
    }

    @Override
    boolean endsWith(Path other) {
        endsWith(other.toString())
    }

    @Override
    boolean endsWith(String other) {
        toString().endsWith(other)
    }

    @Override
    int compareTo(Path other) {
        return toString() <=> other.toString()
    }

    @Override
    Path normalize() {
        throw new UnsupportedOperationException("Operation 'normalize' is not supported by QuiltPath")
        this
    }

    @Override
    QuiltPath resolve(Path other) {
        throw new UnsupportedOperationException("Operation 'resolve' is not supported by QuiltPath")
        if( other.class != QuiltPath )
            throw new ProviderMismatchException()

        final that = (QuiltPath)other
        if( other.isAbsolute() )
            return that

        //new QuiltPath(filesystem, pkg_name, other.toString(), options)
    }

    @Override
    QuiltPath resolve(String other) {
        throw new UnsupportedOperationException("Operation 'resolve' is not supported by QuiltPath")
        //new QuiltPath(filesystem, pkg_name, other, options)
    }

    @Override
    Path resolveSibling(Path other) {
        throw new UnsupportedOperationException("Operation 'resolve' is not supported by QuiltPath")
        //new QuiltPath(filesystem, pkg_name, other.toString(), options)
    }

    @Override
    Path resolveSibling(String other) {
        throw new UnsupportedOperationException("Operation 'resolve' is not supported by QuiltPath")
        //new QuiltPath(filesystem, pkg_name, other, options)
    }

    @Override
    Path relativize(Path other) {
        throw new UnsupportedOperationException("Operation 'resolve' is not supported by QuiltPath")
        //new QuiltPath(filesystem, "", file_key(), options)
    }

    @Override
    String toString() {
        parsed.toString()
    }

    String toUriString() {
        parsed.toUriString()
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
        throw new UnsupportedOperationException("Operation 'toRealPath' is not supported by QuiltPath")
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

}
