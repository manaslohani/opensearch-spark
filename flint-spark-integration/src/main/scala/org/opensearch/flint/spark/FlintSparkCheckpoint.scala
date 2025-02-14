/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import java.util.UUID

import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.opensearch.flint.core.metrics.{MetricConstants, MetricsUtil}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.CheckpointFileManager.RenameHelperMethods

/**
 * Manages the checkpoint directory for Flint indexes.
 *
 * @param spark
 *   The SparkSession used for Hadoop configuration.
 * @param checkpointLocation
 *   The path to the checkpoint directory.
 */
class FlintSparkCheckpoint(spark: SparkSession, val checkpointLocation: String) extends Logging {
  /** Checkpoint root directory path */
  logInfo(s"Initializing FlintSparkCheckpoint with location: $checkpointLocation")
  private val checkpointRootDir = new Path(checkpointLocation)
  logInfo(s"Checkpoint root directory created: $checkpointRootDir")

  /** Spark checkpoint manager */
  private val checkpointManager =
    CheckpointFileManager.create(checkpointRootDir, spark.sessionState.newHadoopConf())
  logInfo(s"Created CheckpointFileManager: ${checkpointManager.getClass} with className ${checkpointManager.getClass.getName}")

  logInfo(s"Hadoop configuration: ${spark.sessionState.newHadoopConf().toString}")

  /**
   * Checks if the checkpoint directory exists.
   *
   * @return
   *   true if the checkpoint directory exists, false otherwise.
   */
  def exists(): Boolean = {
    val result = checkpointManager.exists(checkpointRootDir)
    logInfo(s"Checking if checkpoint directory exists: $checkpointRootDir, result: $result")
    result
  }

  /**
   * Creates the checkpoint directory and all necessary parent directories if they do not already
   * exist.
   *
   * @return
   *   The path to the created checkpoint directory.
   */
  def createDirectory(): Path = {
    logInfo(s"Attempting to create checkpoint directory: $checkpointRootDir")
    val createdPath = checkpointManager.createCheckpointDirectory
    logInfo(s"Successfully created checkpoint directory at: $createdPath")
    createdPath
  }

  /**
   * Creates a temporary file in the checkpoint directory.
   *
   * @return
   *   An optional FSDataOutputStream for the created temporary file, or None if creation fails.
   */
  def createTempFile(): Option[FSDataOutputStream] = {
    logInfo(s"Attempting to create temp file at checkpoint location: $checkpointRootDir")
    checkpointManager match {
      case manager: RenameHelperMethods =>
        val tempFilePath =
          new Path(createDirectory(), s"${UUID.randomUUID().toString}.tmp")
        logInfo(s"Attempting to create temp file: $tempFilePath")
        Some(manager.createTempFile(tempFilePath))
      case _ =>
        logInfo(s"Cannot create temp file at checkpoint location: ${checkpointManager.getClass}")
        None
    }
  }

  /**
   * Deletes the checkpoint directory. This method attempts to delete the checkpoint directory and
   * captures any exceptions that occur. Exceptions are logged but ignored so as not to disrupt
   * the caller's workflow.
   */
  def delete(): Unit = {
    try {
      MetricsUtil.withLatencyAsHistoricGauge(
        () => checkpointManager.delete(checkpointRootDir),
        MetricConstants.CHECKPOINT_DELETE_TIME_METRIC)
      logInfo(s"Checkpoint directory $checkpointRootDir deleted")
    } catch {
      case e: Exception =>
        logError(s"Error deleting checkpoint directory $checkpointRootDir", e)
    }
  }
}
