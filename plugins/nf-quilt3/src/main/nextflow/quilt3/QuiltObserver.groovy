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
import java.text.SimpleDateFormat

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
    public static void writeString(String text, QuiltPackage pkg, String filename) {
        String dir = pkg.packageDest().toString()
        def path = Paths.get(dir, filename)
        //log.debug "QuiltObserver.writeString[$path]: $text"
        Files.write(path, text.bytes)
    }

    private Session session
    private Map config
    private Map quilt_config
    private Set<QuiltPackage> pkgs

    static String now(){
        def date = new Date()
        def sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        return sdf.format(date)
    }

    Set<QuiltPackage> ensurePkgs() {
        if ( !this.pkgs ) {
            this.pkgs = new HashSet<>()
        }
        this.pkgs
    }

    @Override
    void onFlowCreate(Session session) {
        log.debug "`onFlowCreate` $this"
        this.session = session
        this.config = session.config
        this.quilt_config = session.config.navigate('quilt') as Map
        ensurePkgs()
    }

    @Override
    void onFilePublish(Path path) {
        log.debug "onFilePublish.Path[$path]"
        if( path instanceof QuiltPath ) {
            QuiltPath qPath = (QuiltPath)path
            QuiltPackage pkg = qPath.pkg()
            ensurePkgs().add(pkg)
            log.debug "onFilePublish.QuiltPath[$qPath]: pkgs=${pkgs}"
        }
    }

    @Override
    void onFlowComplete() {
        log.debug "`onFlowComplete` ${pkgs}"
        // publish pkgs to repository
        this.pkgs.each { pkg -> publish(pkg) }
    }

    String readme(Map meta, String msg) {
"""
# ${now()}
## $msg
### params
${meta['params']}

## workflow
### scriptFile: ${meta['workflow']['scriptFile']}
### sessionId: ${meta['workflow']['sessionId']}
- start: ${meta['workflow']['start']}
- complete: ${meta['workflow']['complete']}

### processes
${meta['workflow']['stats']['processes']}
"""
    }

    void publish(QuiltPackage pkg) {
        def meta = getMetadata()
        String msg = "${meta['config']['runName']}: ${meta['workflow']['commandLine']}"
        String text = readme(meta,msg)
        writeString(text, pkg, 'README.md')
        def rc = pkg.push(msg,JsonOutput.toJson(meta))
        log.info "$rc: pushed package $msg"
    }

    private static String[] bigKeys = [
        'nextflow','commandLine','scriptFile','projectDir','homeDir','workDir','launchDir','manifest','configFiles'
    ]

     void clearOffset(Map period) {
        log.debug "clearOffset[]"
        log.debug "$period"

        Map offset = period['offset']
        log.debug "offset:$offset"
        offset.remove('availableZoneIds')
    }

    Map getMetadata() {
        // TODO: Write out config files
        Map cf = config
        log.debug "cf:${cf}"
        Map params = session.getParams()
        log.debug "params:${params}"
        Map wf = session.getWorkflowMetadata().toMap()
        bigKeys.each { k -> wf[k] = "${wf[k]}" }
        log.debug "wf:${wf}"
        //clearOffset(wf.get('complete') as Map)
        //clearOffset(wf['start'] as Map)
        log.debug "wf:${wf['runName']}"
        [config: cf, params: params, workflow: wf]
    }
}
