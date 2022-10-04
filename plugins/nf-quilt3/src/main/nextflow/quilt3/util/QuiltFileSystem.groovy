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
import java.nio.file.FileStore
import java.nio.file.Path
import java.nio.file.PathMatcher
import java.nio.file.WatchService
import java.nio.file.attribute.UserPrincipalLookupService
import java.nio.file.spi.FileSystemProvider;

import groovy.transform.CompileStatic
import nextflow.Global
import nextflow.Session
import nextflow.quilt3.QuiltOpts
/**
 * Implements FileSystem interface for Quilt registries
 * Each bucket is a FileSystem
 * Every package is a Root Directory
 * Every logical key is a Path
 *
 * @author Ernest Prabhakar <ernest@quiltdata.io>
 */
@CompileStatic

// cf. https://cloud.google.com/java/docs/reference/google-cloud-nio/latest/com.google.cloud.storage.contrib.nio.CloudStorageFileSystem
// https://github.com/nextflow-io/nextflow-s3fs/tree/master/src/main/java/com/upplication/s3fs
public final class QuiltFileSystem extends FileSystem {

    public final String registry;
    protected final QuiltFileSystemProvider provider

    public QuiltFileSystem(String registry, QuiltFileSystemProvider provider) {
      this.registry = registry;
      this.provider = provider;
    }

    public QuiltPath getPath(String pkg_name, String path) {
      return new QuiltPath(this, pkg_name, path)
    }

    void copy(QuiltPath source, QuiltPath target) {
      throw new UnsupportedOperationException("Operation 'copy' is not supported by QuiltFileSystem")
    }

    void delete(QuiltPath path) {
      throw new UnsupportedOperationException("Operation 'delete' is not supported by QuiltFileSystem")
    }

    @Override
    FileSystemProvider provider() {
        return provider
    }

    @Override
    void close() throws IOException {
        // nothing to do
    }

    @Override
    boolean isOpen() {
        return true
    }

    @Override
    boolean isReadOnly() {
        return false
    }

    @Override
    String getSeparator() {
        return QuiltPath.SEP
    }

    Iterable<? extends Path> getRootDirectories() {
        throw new UnsupportedOperationException("Operation 'getRootDirectories' is not supported by QuiltFileSystem")
    }

    @Override
    Iterable<FileStore> getFileStores() {
        throw new UnsupportedOperationException("Operation 'getFileStores' is not supported by QuiltFileSystem")
    }

    @Override
    Set<String> supportedFileAttributeViews() {
        return Collections.unmodifiableSet( ['basic'] as Set )
    }

    @Override
    QuiltPath getPath(String pkg_name, String... more) {
        final String path = more.join(QuiltPath.SEP)
        return new QuiltPath(this, pkg_name, path)
    }

    protected String toUriString(Path path) {
        return path instanceof QuiltPath ? ((QuiltPath)path).toUriString() : null
    }

    protected String getBashLib(Path path) {
        return path instanceof QuiltPath ? QuiltBashLib.script() : null
    }

    protected String getUploadCmd(String source, Path target) {
        return target instanceof QuiltPath ?  QuiltFileCopyStrategy.uploadCmd(source, target) : null
    }

    @Override
    PathMatcher getPathMatcher(String syntaxAndPattern) {
        throw new UnsupportedOperationException("Operation 'getPathMatcher' is not supported by QuiltFileSystem")
    }

    @Override
    UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException("Operation 'getUserPrincipalLookupService' is not supported by QuiltFileSystem")
    }

    @Override
    WatchService newWatchService() throws IOException {
        throw new UnsupportedOperationException("Operation 'newWatchService' is not supported by QuiltFileSystem")
    }

}
