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

import static java.nio.file.StandardCopyOption.*
import static java.nio.file.StandardOpenOption.*

import java.nio.channels.SeekableByteChannel
import java.nio.file.AccessDeniedException
import java.nio.file.AccessMode
import java.nio.file.CopyOption
import java.nio.file.DirectoryStream
import java.nio.file.FileAlreadyExistsException
import java.nio.file.FileStore
import java.nio.file.FileSystem
import java.nio.file.FileSystemAlreadyExistsException
import java.nio.file.FileSystemNotFoundException
import java.nio.file.LinkOption
import java.nio.file.NoSuchFileException
import java.nio.file.NotDirectoryException;
import java.nio.file.OpenOption
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileAttribute
import java.nio.file.attribute.FileAttributeView
import java.nio.file.attribute.FileTime
import java.nio.file.spi.FileSystemProvider

import groovy.transform.CompileStatic
import groovy.transform.Memoized
import groovy.util.logging.Slf4j
/**
 * Implements NIO File system provider for Quilt Blob Storage
 *
 * @author Ernest Prabhakar <ernest@quiltdata.io>
 */


@Slf4j
@CompileStatic
class QuiltFileSystemProvider extends FileSystemProvider {

    public static final String SCHEME = 'quilt'

    private Map<String,String> env = new HashMap<>(System.getenv())
    private Map<String,QuiltFileSystem> fileSystems = [:]

    /**
     * @inheritDoc
     */
    @Override
    String getScheme() {
        return SCHEME
    }

    static private QuiltPath asQuiltPath(Path path ) {
        if( path !instanceof QuiltPath )
            throw new IllegalArgumentException("Not a valid Quilt blob storage path object: `$path` [${path?.class?.name?:'-'}]" )
        return (QuiltPath)path
    }

    static private QuiltFileSystem getQuiltFilesystem(Path path ) {
        final qPath = asQuiltPath(path)
        final fs = qPath.getFileSystem()
        if( fs !instanceof QuiltFileSystem )
            throw new IllegalArgumentException("Not a valid Quilt file system: `$fs` [${path?.class?.name?:'-'}]" )
        return (QuiltFileSystem)fs
    }

    static private Map<String,Object> parseQuery(String query) {
        final queryParams = query?.split('&') // safe operator for urls without query params
        queryParams.collectEntries { param -> param.split('=').collect { URLDecoder.decode(it) }}
    }
    // def map = url.query.split('&').inject([:])
    // {map, kv-> def (key, value) = kv.split('=').toList(); map[key] = value != null ? URLDecoder.decode(value) : null; map }

    protected String getRegistryName(URI uri) {
        assert uri
        if( !uri.scheme )
            throw new IllegalArgumentException("Missing URI scheme")

        if( uri.scheme.toLowerCase() != SCHEME )
            throw new IllegalArgumentException("Mismatch provider URI scheme: `$scheme`")

        if( !uri.authority ) {
            if( uri.host )
                return uri.host.toLowerCase()
            else
                throw new IllegalArgumentException("Missing Quilt registry name")
        }

        return uri.authority.toLowerCase()
    }


    /**
     * Constructs a new {@code FileSystem} object identified by a URI. This
     * method is invoked by the {@link java.nio.file.FileSystems#newFileSystem(URI,Map)}
     * method to open a new file system identified by a URI.
     *
     * <p> The {@code uri} parameter is an absolute, hierarchical URI, with a
     * scheme equal (without regard to case) to the scheme supported by this
     * provider. The exact form of the URI is highly provider dependent. The
     * {@code env} parameter is a map of provider specific properties to configure
     * the file system.
     *
     * <p> This method throws {@link java.nio.file.FileSystemAlreadyExistsException} if the
     * file system already exists because it was previously created by an
     * invocation of this method. Once a file system is {@link
     * java.nio.file.FileSystem#close closed} it is provider-dependent if the
     * provider allows a new file system to be created with the same URI as a
     * file system it previously created.
     *
     * @param   uri
     *          URI reference
     * @param   config
     *          A map of provider specific properties to configure the file system;
     *          may be empty
     *
     * @return  A new file system
     *
     * @throws  IllegalArgumentException
     *          If the pre-conditions for the {@code uri} parameter aren't met,
     *          or the {@code env} parameter does not contain properties required
     *          by the provider, or a property value is invalid
     * @throws  IOException
     *          An I/O error occurs creating the file system
     * @throws  SecurityException
     *          If a security manager is installed and it denies an unspecified
     *          permission required by the file system provider implementation
     * @throws  java.nio.file.FileSystemAlreadyExistsException
     *          If the file system has already been created
     */
    @Override
    QuiltFileSystem newFileSystem(URI uri, Map<String, ?> config) throws IOException {
        final bucket = getRegistryName(uri)
        newFileSystem(bucket,config)
    }

    QuiltFileSystem newFileSystem(String registry, Map<String, ?> env) throws IOException {
        final fs = new QuiltFileSystem(registry, this)
        fileSystems[registry] = fs
        fs
    }

    /**
     * Returns an existing {@code FileSystem} created by this provider.
     *
     * <p> This method returns a reference to a {@code FileSystem} that was
     * created by invoking the {@link #newFileSystem(URI,Map) newFileSystem(URI,Map)}
     * method. File systems created the {@link #newFileSystem(Path,Map)
     * newFileSystem(Path,Map)} method are not returned by this method.
     * The file system is identified by its {@code URI}. Its exact form
     * is highly provider dependent. In the case of the default provider the URI's
     * path component is {@code "/"} and the authority, query and fragment components
     * are undefined (Undefined components are represented by {@code null}).
     *
     * <p> Once a file system created by this provider is {@link
     * java.nio.file.FileSystem#close closed} it is provider-dependent if this
     * method returns a reference to the closed file system or throws {@link
     * java.nio.file.FileSystemNotFoundException}. If the provider allows a new file system to
     * be created with the same URI as a file system it previously created then
     * this method throws the exception if invoked after the file system is
     * closed (and before a new instance is created by the {@link #newFileSystem
     * newFileSystem} method).
     *
     * @param   uri
     *          URI reference
     *
     * @return  The file system
     *
     * @throws  IllegalArgumentException
     *          If the pre-conditions for the {@code uri} parameter aren't met
     * @throws  java.nio.file.FileSystemNotFoundException
     *          If the file system does not exist
     * @throws  SecurityException
     *          If a security manager is installed and it denies an unspecified
     *          permission.
     */
    @Override
    FileSystem getFileSystem(URI uri) {
        final bucket = getRegistryName(uri)
        getFileSystem0(bucket,false)
    }

    protected QuiltFileSystem getFileSystem0(String bucket, boolean canCreate) {

        def fs = fileSystems.get(bucket)
        if( !fs ) {
            if( canCreate )
                fs = newFileSystem(bucket, env)
            else
                throw new FileSystemNotFoundException("Missing Quilt storage blob file system for bucket: `$bucket`")
        }

        return fs
    }

    /**
     * Return a {@code Path} object by converting the given {@link URI}. The
     * resulting {@code Path} is associated with a {@link FileSystem} that
     * already exists or is constructed automatically.
     *
     * <p> The exact form of the URI is file system provider dependent. In the
     * case of the default provider, the URI scheme is {@code "file"} and the
     * given URI has a non-empty path component, and undefined query, and
     * fragment components. The resulting {@code Path} is associated with the
     * default {@link java.nio.file.FileSystems#getDefault default} {@code FileSystem}.
     *
     * @param   uri
     *          The URI to convert
     *
     * @return  The resulting {@code Path}
     *
     * @throws  IllegalArgumentException
     *          If the URI scheme does not identify this provider or other
     *          preconditions on the uri parameter do not hold
     * @throws  java.nio.file.FileSystemNotFoundException
     *          The file system, identified by the URI, does not exist and
     *          cannot be created automatically
     * @throws  SecurityException
     *          If a security manager is installed and it denies an unspecified
     *          permission.
     */
    @Override
    QuiltPath getPath(URI uri) {
        final registry = getRegistryName(uri)
        final fs = getFileSystem0(registry,true)
        getPath(fs, uri.path, uri.query)
    }

    /**
     * Get a {@link QuiltPath} from an object path string
     *
     * @param path A path in the form {@code containerName/blobName}
     * @return A {@link QuiltPath} object
     */
    QuiltPath getPath(QuiltFileSystem fs, String abspath, String query) {
        final path = abspath.substring(1)
        final pkg_split = path.indexOf('/', path.indexOf("/") + 1)
        final String pkg_name = (pkg_split==-1) ? path : path.substring(0,pkg_split)
        final String filepath = (pkg_split==-1) ? null : path.substring(pkg_split+1)
        final opts = query ? parseQuery(query) : null
        return new QuiltPath(fs, pkg_name, filepath, opts)
    }

    static private FileSystemProvider provider( Path path ) {
        path.getFileSystem().provider()
    }

    private void checkRoot(Path path) {
        if( path.toString() == '/' )
            throw new UnsupportedOperationException("Operation 'checkRoot' not supported on root path")
    }

    @Override
    SeekableByteChannel newByteChannel(Path obj, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        log.info "Faking call to `newByteChannel`: ${obj}"
        checkRoot(obj)

        final modeWrite = options.contains(WRITE) || options.contains(APPEND)
        final modeRead = options.contains(READ) || !modeWrite

        if( modeRead && modeWrite ) {
            throw new IllegalArgumentException("Quilt Blob Storage file cannot be opened in R/W mode at the same time")
        }
        if( options.contains(APPEND) ) {
            throw new IllegalArgumentException("Quilt Blob Storage file system does not support `APPEND` mode")
        }
        if( options.contains(SYNC) ) {
            throw new IllegalArgumentException("Quilt Blob Storage file system does not support `SYNC` mode")
        }
        if( options.contains(DSYNC) ) {
            throw new IllegalArgumentException("Quilt Blob Storage file system does not support `DSYNC` mode")
        }

        final path = asQuiltPath(obj)
        final fs = getQuiltFilesystem(obj)
        if( modeRead ) {
            return fs.newReadableByteChannel(path)
        }

        // -- mode write
        if( options.contains(CREATE_NEW) ) {
            if( fs.exists(path) )
                throw new FileAlreadyExistsException(path.toUriString())
        }
        else if( !options.contains(CREATE)  ) {
            if( !fs.exists(path) )
                throw new NoSuchFileException(path.toUriString())
        }
        if( options.contains(APPEND) ) {
            throw new IllegalArgumentException("File APPEND mode is not supported by Azure Blob Storage")
        }
        return fs.newWritableByteChannel(path)
    }

    private List<Path> listFiles(Path dir, DirectoryStream.Filter<? super Path> filter ) {
        log.info "Faking call to `listFiles`: ${dir}"
        [dir]
    }

    @Override
    DirectoryStream<Path> newDirectoryStream(Path obj, DirectoryStream.Filter<? super Path> filter) throws IOException {
        log.info "Creating `newDirectoryStream`: ${obj}"
        final qPath = asQuiltPath(obj)
        if (!qPath.isPackage())
            throw new NotDirectoryException(qPath.toString());

        final list = listFiles(obj, filter)
        return new DirectoryStream<Path>() {
            @Override
            Iterator<Path> iterator() {
                list.iterator()
            }

            @Override void close() throws IOException { }
        }
    }

    @Override
    void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        log.info "Ignoring call to `createDirectory`: ${dir}"
    }

    @Override
    void delete(Path obj) throws IOException {
        checkRoot(obj)
        final path = asQuiltPath(obj)
        getQuiltFilesystem(path).delete(path)
    }


    @Override
    void copy(Path from, Path to, CopyOption... options) throws IOException {
        log.info "Attempting `copy`: ${from} -> ${to}"
        assert provider(from) == provider(to)
        if( from == to )
            return // nothing to do -- just return

        checkRoot(from); checkRoot(to)
        final source = asQuiltPath(from)
        final target = asQuiltPath(to)
        final fs = getQuiltFilesystem(source)
        fs.copy(source, target)
    }

    @Override
    void move(Path source, Path target, CopyOption... options) throws IOException {
        copy(source,target,options)
        delete(source)
    }

    @Override
    boolean isSameFile(Path path, Path path2) throws IOException {
        return path == path2
    }

    @Override
    boolean isHidden(Path path) throws IOException {
        return path.getFileName()?.toString()?.startsWith('.')
    }

    @Override
    FileStore getFileStore(Path path) throws IOException {
        throw new UnsupportedOperationException("Operation 'getFileStore' is not supported by QuiltFileSystem")
    }

    @Override
    void checkAccess(Path path, AccessMode... modes) throws IOException {
        checkRoot(path)
        final qPath = asQuiltPath(path)
        readAttributes(qPath, QuiltFileAttributes.class)
        if( AccessMode.EXECUTE in modes)
            throw new AccessDeniedException(qPath.toUriString(), null, 'Execute permission not allowed')
    }

    @Override
    def <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        checkRoot(path)
        throw new UnsupportedOperationException("Operation 'getFileAttributeView' is not supported by QuiltFileSystem")
    }

    @Override
    def <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        throw new NoSuchFileException(path.toUriString())

        if( type == BasicFileAttributes || type == QuiltFileAttributes ) {
            log.info "Mocking call to `readAttributes`: ${path}"
            def qPath = asQuiltPath(path)
            def key = qPath.toString()
            def lastModifiedTime = FileTime.fromMillis(1_000_000_000_000)
            def size = 1000
            def isPackage = qPath.isPackage()
            return (A) new QuiltFileAttributes(key, lastModifiedTime, size, isPackage, !isPackage);
        }
        throw new UnsupportedOperationException("Not a valid Quilt Storage file attribute type: $type")
    }

    @Override
    Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        throw new UnsupportedOperationException("Operation Map 'readAttributes' is not supported by QuiltFileSystem")
    }

    @Override
    void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        throw new UnsupportedOperationException("Operation 'setAttribute' is not supported by QuiltFileSystem")
    }

}
