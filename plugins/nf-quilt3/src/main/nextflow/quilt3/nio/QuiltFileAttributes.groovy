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

import static java.lang.String.format;
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributeView
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.FileTime
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

/**
 * Implements {@link BasicFileAttributes} for Quilt storage blob
 *
 * @author Ernest Prabhakar <ernest@quiltdata.io>
 */

@Slf4j
@CompileStatic
class QuiltFileAttributes implements BasicFileAttributes {
    private final Path path;
    private final String key;
    private final BasicFileAttributes attrs;

 	public QuiltFileAttributes(Path path, String key, BasicFileAttributes attrs) {
        log.info "QuiltFileAttributes[$key] $attrs"
        this.path = path;
 		this.key = key;
 		this.attrs = attrs;
 	}

 	@Override
 	public FileTime lastModifiedTime() {
 		return attrs.lastModifiedTime();
 	}

 	@Override
 	public FileTime lastAccessTime() {
 		return attrs.lastAccessTime();
 	}

 	@Override
 	public FileTime creationTime() {
        return attrs.creationTime();
 	}

 	@Override
 	public boolean isRegularFile() {
        return attrs.creationTime();
 	}

 	@Override
 	public boolean isDirectory() {
        return attrs.creationTime();
 	}

 	@Override
 	public boolean isSymbolicLink() {
        return attrs.isSymbolicLink();
 	}

 	@Override
 	public boolean isOther() {
        return attrs.isOther();
 	}

 	@Override
 	public long size() {
        return attrs.size();
 	}

 	@Override
 	public Object fileKey() {
 		return key;
 	}

 	@Override
 	public String toString() {
 		return format(
 				"[%s: lastModified=%s, size=%s]",
 				key, lastModifiedTime(), size());
 	}
 }
