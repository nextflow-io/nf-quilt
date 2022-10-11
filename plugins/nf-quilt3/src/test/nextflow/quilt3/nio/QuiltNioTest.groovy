package nextflow.quilt3.nio
import nextflow.quilt3.QuiltSpecification

import java.nio.charset.Charset
import java.nio.file.DirectoryNotEmptyException
import java.nio.file.FileAlreadyExistsException
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.SimpleFileVisitor
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.BasicFileAttributes

import spock.lang.IgnoreIf
import spock.lang.Requires
import spock.lang.Shared
import spock.lang.Ignore
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@IgnoreIf({System.getenv('NXF_SMOKE')})
@Requires({System.getenv('AZURE_STORAGE_ACCOUNT_NAME') && System.getenv('AZURE_STORAGE_ACCOUNT_KEY')})
class QuiltNioTest extends QuiltSpecification {

    static String pkg_url = 'quilt3://quilt-example/examples/hurdat'
    static String url = pkg_url+'/folder/file-name.txt'
    static String TEXT = "Hello world!"

    def 'should write a file' () {
        given:
        def path = Paths.get(new URI(url))

        when:
        createObject(path, TEXT)
        then:
        existsPath(url)
        new String(Files.readAllBytes(path)) == TEXT
        Files.readAllLines(path, Charset.forName('UTF-8')).get(0) == TEXT
        readObject(path) == TEXT
    }

    def 'should read file attributes' () {
        given:
        final start = System.currentTimeMillis()

        when:
        createObject(path, TEXT)
        and:

        //
        // -- readAttributes
        //
        def attrs = Files.readAttributes(path, BasicFileAttributes)
        def unprefixed = url.replace(QuiltPathFactory.PREFIX,"")
        then:
        attrs.isRegularFile()
        !attrs.isDirectory()
        attrs.size() == 12
        !attrs.isSymbolicLink()
        !attrs.isOther()
        attrs.fileKey() == unprefixed
        attrs.lastAccessTime() == null
        attrs.lastModifiedTime().toMillis()-start < 5_000
        attrs.creationTime().toMillis()-start < 5_000

        //
        // -- getLastModifiedTime
        //
        when:
        def time = Files.getLastModifiedTime(path)
        then:
        time == attrs.lastModifiedTime()

        //
        // -- getFileAttributeView
        //
        when:
        def view = Files.getFileAttributeView(path, BasicFileAttributeView)
        then:
        view.readAttributes() == attrs

        //
        // -- readAttributes for a directory
        //
        when:
        attrs = Files.readAttributes(path.getParent(), BasicFileAttributes)
        then:
        !attrs.isRegularFile()
        attrs.isDirectory()
        attrs.size() == 0
        !attrs.isSymbolicLink()
        !attrs.isOther()
        attrs.fileKey() == unprefixed.replace("/file-name.txt","")
        attrs.lastAccessTime() == null
        attrs.lastModifiedTime() == null
        attrs.creationTime() == null

        //
        // -- readAttributes for a package
        //
        when:
        attrs = Files.readAttributes(Paths.get(new URI(pkg_url)), BasicFileAttributes)
        then:
        !attrs.isRegularFile()
        attrs.isDirectory()
        attrs.size() == 0
        !attrs.isSymbolicLink()
        !attrs.isOther()
        attrs.fileKey() == pkg_url.replace(QuiltPathFactory.PREFIX,"")
        attrs.creationTime() == null
        attrs.lastAccessTime() == null
        attrs.lastModifiedTime().toMillis()-start < 5_000
    }


    def 'should copy a stream to path' () {
        given:
        def stream = new ByteArrayInputStream(new String(TEXT).bytes)
        Files.copy(stream, path)
        and:
        existsPath(path)
        readObject(path) == TEXT

        when:
        stream = new ByteArrayInputStream(new String(TEXT).bytes)
        Files.copy(stream, path, StandardCopyOption.REPLACE_EXISTING)
        then:
        existsPath(path)
        readObject(path) == TEXT

        when:
        stream = new ByteArrayInputStream(new String(TEXT).bytes)
        Files.copy(stream, path)
        then:
        thrown(FileAlreadyExistsException)
    }

    def 'copy local file to a bucket' () {
        given:
        def source = Files.createTempFile('test','nf')
        source.text = TEXT

        when:
        Files.copy(source, path)
        then:
        readObject(path) == TEXT

        cleanup:
        if( source ) Files.delete(source)
    }

    def 'copy a remote file to a bucket' () {
        given:
        final source_url = url.replace("folder","source")
        final source = Paths.get(new URI(source_url))
        Files.write(source, TEXT.bytes)
        and:
        Files.copy(source, path)
        expect:
        existsPath(source)
        existsPath(path)
        readObject(path) == TEXT
    }

    @Ignore
    def 'move a remote file to a bucket' () {
        given:
        final source_url = url.replace("folder","source")
        final source = Paths.get(new URI(source_url))
        Files.write(source, TEXT.bytes)
        and:
        Files.move(source, path)
        expect:
        !existsPath(source)
        existsPath(path)
        readObject(path) == TEXT
    }

    def 'should create a directory' () {
        given:
        def dir = Paths.get(new URI(pkg_url))

        when:
        Files.createDirectory(dir)
        then:
        existsPath(dir)
    }

    def 'should create a directory tree' () {
        given:
        def dir = Paths.get(new URI("$pkg_url/alpha/bravo/omega/"))
        when:
        Files.createDirectories(dir)
        then:
        Files.exists(Paths.get(new URI("$pkg_url/alpha/")))
        Files.exists(Paths.get(new URI("$pkg_url/alpha/bravo/")))
        Files.exists(Paths.get(new URI("$pkg_url/alpha/bravo/omega/")))

        when:
        Files.createDirectories(dir)
        then:
        noExceptionThrown()
    }

    def 'should create a file' () {
        given:
        Files.createFile(path)
        expect:
        existsPath(path)
    }

    def 'should create temp file and directory' () {
        given:
        def base = Paths.get(new URI(pkg_url))

        when:
        def t1 = Files.createTempDirectory(base, 'test')
        then:
        Files.exists(t1)

        when:
        def t2 = Files.createTempFile(base, 'prefix', 'suffix')
        then:
        Files.exists(t2)
    }

    def 'should delete a file' () {
        given:
        createObject(path,TEXT)

        when:
        Files.delete(path)
        sleep 100
        then:
        !existsPath(path)
    }

    def 'should throw a NoSuchFileException when deleting an object not existing' () {
        when:
        Files.delete(path)
        then:
        thrown(NoSuchFileException)
    }

    def 'should validate exists method' () {
        given:
        createObject(path,TEXT)

        expect:
        Files.exists(Paths.get(new URI("$pkg_url")))
        Files.exists(Paths.get(new URI("$pkg_url/folder/")))
        Files.exists(Paths.get(new URI("$pkg_url/folder/file.txt")))
        !Files.exists(Paths.get(new URI("$pkg_url/fooooo.txt")))
        !Files.exists(Paths.get(new URI("quilt3://missingBucket")))
    }


    def 'should check is it is a directory' () {
        given:
        def dir = Paths.get(new URI("$pkg_url"))
        expect:
        Files.isDirectory(dir)
        !Files.isRegularFile(dir)

        when:
        def file = dir.resolve('this/and/that')
        createObject(file, 'Hello world')
        then:
        !Files.isDirectory(file)
        Files.isRegularFile(file)
        Files.isReadable(file)
        Files.isWritable(file)
        !Files.isExecutable(file)
        !Files.isSymbolicLink(file)

        expect:
        Files.isDirectory(file.parent)
        !Files.isRegularFile(file.parent)
        Files.isReadable(file)
        Files.isWritable(file)
        !Files.isExecutable(file)
        !Files.isSymbolicLink(file)
    }

    def 'should check that is the same file' () {

        given:
        def file1 = Paths.get(new URI("quilt3://some/data/file.txt"))
        def file2 = Paths.get(new URI("quilt3://some/data/file.txt"))
        def file3 = Paths.get(new URI("quilt3://some/data/fooo.txt"))

        expect:
        Files.isSameFile(file1, file2)
        !Files.isSameFile(file1, file3)

    }

    def 'should create a newBufferedReader' () {
        given:
        createObject(path, TEXT)

        when:
        def reader = Files.newBufferedReader(path, Charset.forName('UTF-8'))
        then:
        reader.text == TEXT

        when:
        def unknown = Paths.get(new URI("$pkg_url/unknown.txt"))
        Files.newBufferedReader(unknown, Charset.forName('UTF-8'))
        then:
        thrown(NoSuchFileException)
    }

    def 'should create a newBufferedWriter' () {
        given:
        def writer = Files.newBufferedWriter(path, Charset.forName('UTF-8'))
        TEXT.readLines().each { it -> writer.println(it) }
        writer.close()
        expect:
        readObject(path) == TEXT
    }

    def 'should create a newInputStream' () {
        given:
        createObject(path, TEXT)

        when:
        def reader = Files.newInputStream(path)
        then:
        reader.text == TEXT
    }

    def 'should create a newOutputStream' () {
        given:
        def writer = Files.newOutputStream(path)
        TEXT.readLines().each { it ->
            writer.write(it.bytes);
            writer.write((int)('\n' as char))
        }
        writer.close()
        expect:
        readObject(path) == TEXT
    }

    def 'should read a newByteChannel' () {
        given:
        createObject(path, TEXT)

        when:
        def channel = Files.newByteChannel(path)
        then:
        readChannel(channel, 10) == TEXT
    }

    def 'should write a byte channel' () {
        given:
        def channel = Files.newByteChannel(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
        writeChannel(channel, TEXT, 200)
        channel.close()
        expect:
        readObject(path) == TEXT
    }

    def 'should check file size' () {
        given:
        createObject(path, TEXT)
        expect:
        Files.size(path) == TEXT.size()

        when:
        Files.size(path.resolve('xxx'))
        then:
        thrown(NoSuchFileException)
    }

    def 'should stream directory content' () {
        given:
        createObject("$pkg_url/foo/file1.txt",'A')
        createObject("$pkg_url/foo/file2.txt",'BB')
        createObject("$pkg_url/foo/bar/file3.txt",'CCC')
        createObject("$pkg_url/foo/bar/baz/file4.txt",'DDDD')
        createObject("$pkg_url/foo/bar/file5.txt",'EEEEE')
        createObject("$pkg_url/foo/file6.txt",'FFFFFF')

        when:
        def list = Files.newDirectoryStream(Paths.get(new URI("$pkg_url"))).collect { it.getFileName().toString() }
        then:
        list.size() == 1
        list == [ 'foo' ]

        when:
        list = Files.newDirectoryStream(Paths.get(new URI("$pkg_url/foo"))).collect { it.getFileName().toString() }
        then:
        list.size() == 4
        list as Set == [ 'file1.txt', 'file2.txt', 'bar', 'file6.txt' ] as Set

        when:
        list = Files.newDirectoryStream(Paths.get(new URI("$pkg_url/foo/bar"))).collect { it.getFileName().toString() }
        then:
        list.size() == 3
        list as Set == [ 'file3.txt', 'baz', 'file5.txt' ] as Set

        when:
        list = Files.newDirectoryStream(Paths.get(new URI("$pkg_url/foo/bar/baz"))).collect { it.getFileName().toString() }
        then:
        list.size() == 1
        list  == [ 'file4.txt' ]
    }


    def 'should check walkTree' () {

        given:
        createObject("$pkg_url/foo/file1.txt",'A')
        createObject("$pkg_url/foo/file2.txt",'BB')
        createObject("$pkg_url/foo/bar/file3.txt",'CCC')
        createObject("$pkg_url/foo/bar/baz/file4.txt",'DDDD')
        createObject("$pkg_url/foo/bar/file5.txt",'EEEEE')
        createObject("$pkg_url/foo/file6.txt",'FFFFFF')

        when:
        List<String> dirs = []
        Map<String,BasicFileAttributes> files = [:]
        def base = Paths.get(new URI("$pkg_url"))
        Files.walkFileTree(base, new SimpleFileVisitor<Path>() {

            @Override
            FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException
            {
                dirs << base.relativize(dir).toString()
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
            {
                files[file.getFileName().toString()] = attrs
                return FileVisitResult.CONTINUE;
            }
        })

        then:
        files.size() == 6
        files ['file1.txt'].size() == 1
        files ['file2.txt'].size() == 2
        files ['file3.txt'].size() == 3
        files ['file4.txt'].size() == 4
        files ['file5.txt'].size() == 5
        files ['file6.txt'].size() == 6
        dirs.size() == 4
        dirs.contains("")
        dirs.contains('foo')
        dirs.contains('foo/bar')
        dirs.contains('foo/bar/baz')


        when:
        dirs = []
        files = [:]
        base = Paths.get(new URI("$pkg_url/foo/bar/"))
        Files.walkFileTree(base, new SimpleFileVisitor<Path>() {

            @Override
            FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException
            {
                dirs << base.relativize(dir).toString()
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
            {
                files[file.getFileName().toString()] = attrs
                return FileVisitResult.CONTINUE;
            }
        })

        then:
        files.size()==3
        files.containsKey('file3.txt')
        files.containsKey('file4.txt')
        files.containsKey('file5.txt')
        dirs.size() == 2
        dirs.contains("")
        dirs.contains('baz')
    }

    @Ignore
    def 'should handle dir and files having the same name' () {

        given:
        createObject("$pkg_url/foo",'file-1')
        createObject("$pkg_url/foo/bar",'file-2')
        createObject("$pkg_url/foo/baz",'file-3')
        and:
        def root = Paths.get(new URI("$pkg_url"))

        when:
        def file1 = root.resolve('foo')
        then:
        Files.isRegularFile(file1)
        !Files.isDirectory(file1)
        file1.text == 'file-1'

        when:
        def dir1 = root.resolve('foo/')
        then:
        !Files.isRegularFile(dir1)
        Files.isDirectory(dir1)

        when:
        def file2 = root.resolve('foo/bar')
        then:
        Files.isRegularFile(file2)
        !Files.isDirectory(file2)
        file2.text == 'file-2'

        when:
        def parent = file2.parent
        then:
        !Files.isRegularFile(parent)
        Files.isDirectory(parent)

        when:
        Set<String> dirs = []
        Map<String,BasicFileAttributes> files = [:]
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {

            @Override
            FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException
            {
                dirs << root.relativize(dir).toString()
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
            {
                files[root.relativize(file).toString()] = attrs
                return FileVisitResult.CONTINUE;
            }
        })
        then:
        dirs.size() == 2
        dirs.contains('')
        dirs.contains('foo')
        files.size() == 3
        files.containsKey('foo')
        files.containsKey('foo/bar')
        files.containsKey('foo/baz')

    }

    def 'should handle file names with same prefix' () {
        given:
        def pkg_url = createBucket()
        and:
        createObject("$pkg_url/transcript_index.junctions.fa", 'foo')
        createObject("$pkg_url/alpha-beta/file1", 'bar')
        createObject("$pkg_url/alpha/file2", 'baz')

        expect:
        Files.exists(Paths.get(new URI("$pkg_url/transcript_index.junctions.fa")))
        !Files.exists(Paths.get(new URI("$pkg_url/transcript_index.junctions")))
        Files.exists(Paths.get(new URI("$pkg_url/alpha-beta/file1")))
        Files.exists(Paths.get(new URI("$pkg_url/alpha/file2")))
        Files.exists(Paths.get(new URI("$pkg_url/alpha-beta/")))
        Files.exists(Paths.get(new URI("$pkg_url/alpha-beta")))
        Files.exists(Paths.get(new URI("$pkg_url/alpha/")))
        Files.exists(Paths.get(new URI("$pkg_url/alpha")))
    }

}