/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal
import scala.util.matching.Regex

import org.apache.spark.internal.Logging


/**
 * File system audit for test suites.
 */
trait FileSystemAudit extends Logging {

  def throwExceptionOnError: Boolean = true

  def depth: Int = 5

  def ignoredRegex: Seq[String] = Seq(
    "/dev",
    "/proc",
    "/sys",
//    ".*/var.*",
//    ".*\\.oracle_jre_usage",
//    ".*/var/log.*",
//    ".*/var/db.*",
//    ".*/var/vm.*",
//    ".*/Library.*",
//    ".*/var/log.*",
//    "/Library/Logs.*",
    ".*\\.ivy2"
//    ".*\\.kube"
  )

  lazy val regexes = ignoredRegex.map(r => r.r)

  def shouldIgnorePath(path: String): Boolean = {
    regexes.exists(_.findFirstMatchIn(path).isDefined)
  }

  def takeSnapshot(): FileSystemSnapshot = {
    val fileSystem = FileSystems.getDefault

    val fileSizeMap = mutable.Map[Path, Long]()

    def getFileSize(path: Path): Long = {
      try {
        fileSizeMap.getOrElseUpdate(path, {
          if (Files.isDirectory(path)) {
            var size = 0L

            Files.walkFileTree(path, new FileVisitor[Path] {
              override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes)
              : FileVisitResult = {
                if (dir == path || shouldIgnorePath(dir.toString)) {
                  FileVisitResult.SKIP_SUBTREE
                } else {
                  FileVisitResult.CONTINUE
                }
              }

              override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
                size += getFileSize(file)
                FileVisitResult.CONTINUE
              }

              override def visitFileFailed(file: Path, exc: IOException): FileVisitResult = {
                FileVisitResult.CONTINUE
              }

              override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
                FileVisitResult.CONTINUE
              }
            })
            size

          } else {
            Files.size(path)
          }
        })
      } catch {
        case NonFatal(_) => 0
      }
    }

    var files = Map[String, FileInfo]()

    fileSystem.getRootDirectories.asScala.foreach { root =>
      Files.walkFileTree(root, java.util.Collections.emptySet(), 4, new FileVisitor[Path] {
        override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
          if (shouldIgnorePath(dir.toString)) {
            FileVisitResult.SKIP_SUBTREE
          } else {
            FileVisitResult.CONTINUE
          }
        }

        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          lazy val fileString = file.toString
          if (attrs.isSymbolicLink || shouldIgnorePath(fileString)) {
            return FileVisitResult.CONTINUE
          } // else

          files += fileString -> FileInfo(
            getFileSize(file),
            attrs.creationTime().toMillis,
            attrs.lastModifiedTime().toMillis,
            Files.getOwner(file).toString
          )
          FileVisitResult.CONTINUE
        }

        override def visitFileFailed(file: Path, exc: IOException): FileVisitResult = {
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
          FileVisitResult.CONTINUE
        }
      })
    }

    FileSystemSnapshot(files)
  }

  var snapshot: Option[FileSystemSnapshot] = None

  def preFileSystemAudit(): Unit = {
    snapshot = Some(takeSnapshot())
  }

  def postFileSystemAudit(): Unit = {
    snapshot match {
      case Some(snap) =>
        val currentSnap = takeSnapshot()
        val differences = FileSystemSnapshot.getDifferences(snap, currentSnap)
        if (differences.nonEmpty) {
          logWarning(s"There were differences between the filesystem snapshots taken:\n" +
            differences.mkString("\n"))
          if (throwExceptionOnError) {
            throw new RuntimeException(
              s"Snapshots did not match before and after audit. \n${differences.mkString("\n")}")
          }
        }
    }
  }

}

case class FileInfo(size: Long, created: Long, modified: Long, owner: String)

case class FileSystemSnapshot(files: Map[String, FileInfo])


sealed trait FileDifference

case class FileMissing(info: FileInfo) extends FileDifference

case class FileInfoDifference(oldInfo: FileInfo, newInfo: FileInfo) extends FileDifference

case class NewFile(info: FileInfo) extends FileDifference

object FileSystemSnapshot {
  def getDifferences(
    before: FileSystemSnapshot,
    after: FileSystemSnapshot
  ): Map[String, FileDifference] = {
    var results = Map[String, FileDifference]()

    before.files.foreach { case (file, oldInfo) =>
      after.files.get(file) match {
        case Some(newInfo) =>
          if (newInfo != oldInfo) {
            results += file -> FileInfoDifference(oldInfo, newInfo)
          }
        case None =>
          results += file -> FileMissing(oldInfo)
      }
    }

    after.files.foreach { case (file, newInfo) =>
      before.files.get(file) match {
        case None =>
          results += file -> NewFile(newInfo)
        case _ => // this case was already handled in the previous loop
      }
    }

    results
  }
}
