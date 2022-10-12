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

package nextflow.quilt3

import nextflow.quilt3.jep.QuiltPackage
import nextflow.quilt3.nio.QuiltPath

import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.PathMatcher

import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Global
import nextflow.Session
import nextflow.trace.TraceObserver

/**
 * Plugin observer of workflow events
 *
 * @author Ernest Prabhakar <ernest@quiltdata.io>
 */
@Slf4j
@CompileStatic
class QuiltObserver implements TraceObserver {
    public static String DEFAULT_METADATA_FILENAME='quilt_metadata.json'

    public static void writeString(String text, QuiltPackage pkg, String filename) {
        String dir = pkg.packageDest().toString()
        def path = Paths.get(dir, filename)
        log.info "QuiltObserver.writeString[$path]: $text"
        Files.write(path, text.bytes)
    }

    private Session session
    private Map config
    private Map quilt_config
    private Set<QuiltPackage> pkgs

    @Override
    void onFlowCreate(Session session) {
        log.debug "`onFlowCreate` $this"
        this.session = session
        this.config = session.config
        this.quilt_config = session.config.navigate('quilt') as Map
        this.pkgs = new HashSet<>()
    }

    @Override
    void onFilePublish(Path path) {
        log.debug "onFilePublish.Path[$path]"
        if( path instanceof QuiltPath ) {
            QuiltPath qPath = (QuiltPath)path
            QuiltPackage pkg = qPath.pkg()
            this.pkgs.add(pkg)
            log.debug "onFilePublish.QuiltPath[$qPath]: pkgs=${this.pkgs}"
        }
    }

    @Override
    void onFlowComplete() {
        log.info "`onFlowComplete` ${this.pkgs}"
        // publish pkgs to repository
        this.pkgs.each { pkg -> publish(pkg) }
    }

    void publish(QuiltPackage pkg) {
        String filename = DEFAULT_METADATA_FILENAME // (quilt_config.get('metadata_file') || as String
        String json = getMetadataJSON()
        writeString(json, pkg, filename)
        pkg.push()
    }

    String getMetadataJSON() {
        String config = JsonOutput.toJson(config)
        //String workflow = JsonOutput.toJson(session.getWorkflowMetadata())
        //String params = JsonOutput.toJson(session.getParams())
        def metadata = [config: config]//, workflow: workflow, params: params]
        JsonOutput.toJson(metadata)
    }

}
