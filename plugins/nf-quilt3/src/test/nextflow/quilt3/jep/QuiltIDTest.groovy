package nextflow.quilt3.jep
import nextflow.quilt3.QuiltSpecification

import spock.lang.Unroll
import spock.lang.Ignore
import groovy.util.logging.Slf4j

/**
 *
 * @author Ernest Prabhakar <ernest@quiltdata.io>
 */
@Slf4j
class QuiltIDTest extends QuiltSpecification {

    def 'should null on invalid pgk_name'() {
        when:
        def id = QuiltID.Fetch("bucket", "pkg")
        then:
        !id
    }

    def 'should decompose pkg names'() {
        when:
        def id = QuiltID.Fetch("bucket", "pkg/name")
        then:
        id.toString() == "name.pkg.bucket"
    }
}
