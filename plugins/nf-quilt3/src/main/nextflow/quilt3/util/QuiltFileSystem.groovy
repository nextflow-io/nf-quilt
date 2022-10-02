/*
 * Copyright 2019, Google Inc
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

import java.nio.file.Path
import java.nio.file.FileSystem

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

    public static final String URI_SCHEME = "quilt"

    public final String registry;

    public static QuiltFileSystem forBucket(String registry) {
      return new QuiltFileSystem(registry);
    }

    public QuiltFileSystem(String registry) {
      this.registry = registry;
    }

    public QuiltPath getPath(String pkg_name, String path) {
      return new QuiltPath(this, pkg_name, path)
    }

}
