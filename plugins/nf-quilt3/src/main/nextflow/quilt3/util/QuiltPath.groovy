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

package nextflow.quilt3.util

import java.nio.file.FileSystem
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.ProviderMismatchException
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService

import groovy.transform.CompileStatic
import nextflow.Global
import nextflow.Session
import nextflow.quilt3.QuiltOpts
/**
 * Implements Path interface for Quilt storage
 *
 * @author Ernest Prabhakar <ernest@quiltdata.io>
 */
@CompileStatic

// cf. https://cloud.google.com/java/docs/reference/google-cloud-nio/latest/com.google.cloud.storage.contrib.nio.CloudStoragePath
public final class QuiltPath implements Path {
    final public static String SEP = '/'

    private QuiltFileSystem filesystem
    private String pkg_name
    private String[] names
    private Map<String,Object> options

    public QuiltPath(QuiltFileSystem filesystem, String pkg_name, String filepath, Map<String,Object> options) {
        this.filesystem = filesystem
        this.pkg_name = pkg_name
        this.names = filepath.split(SEP)
        this.options = options
    }

    public String registry() {
        return filesystem.registry
    }

    public String pkg_name() {
        return pkg_name
    }

    public String filepath() {
        return names.join(SEP)
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
        pkg_name != null
    }

    boolean isPackage() {
        getNameCount() == 0
    }

    Path getPackage() {
        isPackage() ? this : new QuiltPath(filesystem, pkg_name, "", null)
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
        names.size()
    }

    @Override
    Path getName(int index) {
        new QuiltPath(filesystem, pkg_name, names[0,index], options)
    }

    @Override
    Path subpath(int beginIndex, int endIndex) {
        final sub = names[beginIndex,endIndex].join(SEP)
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
        new QuiltPath(filesystem, null, filepath(), options)
    }

    @Override
    String toString() {
        toUriString()
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
