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

    private Session session

    private Map config

    private Set<QuiltPackage> pkgs

    @Override
    void onFlowCreate(Session session) {
        log.info "`onFlowCreate` $this"
        this.session = session
        this.config = session.config.navigate('quilt') as Map
        this.pkgs = new HashSet<>()
    }

    @Override
    void onFilePublish(Path path) {
        if( path instanceof QuiltPath ) {
            QuiltPath qPath = (QuiltPath)path
            QuiltPackage pkg = qPath.pkg()
            this.pkgs.add(pkg)
            log.info "`onFilePublish.QuiltPath` $qPath -> ${this.pkgs} "
        }
    }

    @Override
    void onFlowComplete() {
        log.info "`onFlowComplete` ${this.pkgs}"
        // make sure there are packages to publish
        if( this.pkgs.isEmpty() ) {
            return
        }

        // publish pkgs to repository
        this.pkgs.each { pkg -> pkg.push() }
    }
}