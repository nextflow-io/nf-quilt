package nextflow.quilt3.nio
import nextflow.quilt3.QuiltSpecification

import java.nio.file.Paths

import spock.lang.Ignore

/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
class QuiltFileSystemTest extends QuiltSpecification {

    def 'should get a path' () {
        given:
        final provider = Spy(QuiltFileSystemProvider)
        final foo_fs = new QuiltFileSystem('foo', provider)
        final bar_fs = new QuiltFileSystem('bar', provider)

        when:
        def result = foo_fs.getPath(path, more as String[])
        then:
        call * provider.getFileSystem0(_,true) >> { it, create -> it=='foo' ? foo_fs : bar_fs }
        result.toUriString()

        where:
        call| path                  | more          | expected
        1   | '/foo'                | null          | 'quilt3://foo/'
        1   | '/foo/'               | null          | 'quilt3://foo/'
        1   | '/foo/alpha/bravo'    | null          | 'quilt3://foo/alpha/bravo'
        1   | '/foo/alpha/bravo/'   | null          | 'quilt3://foo/alpha/bravo/'
        1   | '/bar'                | null          | 'quilt3://bar/'
        1   | '/bar'                | ['a','b']     | 'quilt3://bar/a/b'
        1   | '/bar/'               | ['a/','b/']   | 'quilt3://bar/a/b'
        1   | '/bar/'               | ['/a','/b']   | 'quilt3://bar/a/b'
        0   | 'this/and/that'       | null          | 'quilt3:/this/and/that'
        0   | 'this/and/that'       | 'x/y'         | 'quilt3:/this/and/that/x/y'

    }

    @Ignore
    def 'should return root path' () {

        given:
        def provider = Mock(QuiltFileSystemProvider)
        def fs = new QuiltFileSystem('bucket-example', provider)

        expect:
        fs.getRootDirectories() == [ new QuiltPath(fs, null) ]
    }

    def 'should test basic properties' () {

        given:
        def BUCKET_NAME = 'bucket'
        def provider = Stub(QuiltFileSystemProvider)

        when:
        def fs = new QuiltFileSystem(BUCKET_NAME, provider)
        then:
        fs.getSeparator() == '/'
        fs.isOpen()
        fs.provider() == provider
        fs.bucket == BUCKET_NAME
        !fs.isReadOnly()
        fs.supportedFileAttributeViews() == ['basic'] as Set
    }

    def 'should test getPath' () {
        given:
        def BUCKET_NAME = 'bucket'
        def provider = Stub(QuiltFileSystemProvider)
        and:
        def fs = new QuiltFileSystem(BUCKET_NAME, provider)

        expect:
        fs.getPath('file-name.txt') == new QuiltPath(fs, Paths.get('file-name.txt'), false)
        fs.getPath('alpha/bravo') == new QuiltPath(fs, Paths.get('alpha/bravo'), false)
        fs.getPath('/alpha/bravo') == new QuiltPath(fs, Paths.get('/bucket/alpha/bravo'), false)
        fs.getPath('/alpha','/gamma','/delta') == new QuiltPath(fs, Paths.get('/bucket/alpha/gamma/delta'), false)
        fs.getPath('/alpha','gamma//','delta//') == new QuiltPath(fs, Paths.get('/bucket/alpha/gamma/delta'), false)
    }

}
