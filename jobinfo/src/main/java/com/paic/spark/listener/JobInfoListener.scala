package com.paic.spark.listener

import java.io.{File, FileInputStream}
import java.text.SimpleDateFormat
import java.util
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.{Date, Properties}

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler._
import org.apache.spark.{SparkConf, SparkFirehoseListener, Success}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * As an extra listener to SparkListenerBus, constructor with one SparkConf parameter is needed.
  *
  * Procedures
  *
  *   1. basic tools and app properties
  *     - use slf4j to write log4j logs
  *     - get jobInfo properties from conf file, default is jobinfo-collector.properties
  *     - define spark app property instances
  *     - define statistics structures
  *
  *   2. event queue definition and initialize listener thread
  *     - limit the length of event queue
  *     - retry if offer event failed
  *
  *   3. stat process functions definition
  *     - statAppStart / statJobStart / statTaskEnd / statStageCompleted / statJobEnd
  */
class JobInfoListener(conf: SparkConf) extends SparkFirehoseListener {

  // tools
  private val logger = LoggerFactory.getLogger("com.paic.spark.listener.JobInfoListener")
  private val loggerInfo = LoggerFactory.getLogger("com.paic.spark.listener.JobInfoListener.info")
  private val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  // job info properties
  private val jobInfoConf = conf.getOption("paic.jobinfo.conf").getOrElse("jobinfo-collector.properties")
  private val sparkConfDir = Option(System.getenv.get("SPARK_CONF_DIR")).getOrElse("/appcom/spark-config")

  private val jobInfoProperties = new Properties()
  jobInfoProperties.load(new FileInputStream(new File(sparkConfDir + "/" + jobInfoConf)))
  private val eventQueueLen = jobInfoProperties.getProperty("event.queue.length", "2000").toInt
  private val eventQueuePollTimeout = jobInfoProperties.getProperty("event.queue.poll.timeout", "2000").toInt
  private val eventQueueOfferTimeout = jobInfoProperties.getProperty("event.queue.offer.timeout", "2000").toInt
  private val eventQueueOfferTry = jobInfoProperties.getProperty("event.queue.offer.try", "3").toInt
  private val dataMapLimit = jobInfoProperties.getProperty("data.map.limit", "10000").toInt

  // application properties
  private val taskQueue = conf.getOption("spark.yarn.queue").getOrElse("root.default")
  private var appName: String = _
  private var userName: String = _
  private var appId: String = _

  // basic type definition
  type JobId = Int
  type StageId = Int
  type TaskId = Long
  type StageAttemptId = Int
  type TaskInputBytes = Long

  // statistics structures
  class StageStatData {
    var inputBytes: Long = _
    val taskData = new mutable.HashMap[TaskId, TaskMetrics]
  }

  case class JobStatData(startTime: Long, var inputBytes: Long)

  // statistics storage - release cache after finish using lru
  private val DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16
  private val DEFAULT_LOAD_FACTOR = 0.75f;
  private val stageIdToData = new util.LinkedHashMap[(StageId, StageAttemptId), StageStatData](
    DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, true) {
    override protected def removeEldestEntry(eldest: util.Map.Entry[(StageId, StageAttemptId), StageStatData]): Boolean = {
      val full = size() > dataMapLimit
      if (full) logger.warn("stageIdToData reach limit")
      full
    }
  }
  private val stageIdToJobId = new util.LinkedHashMap[StageId, JobId](
    DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, true) {
    override protected def removeEldestEntry(eldest: util.Map.Entry[StageId, JobId]): Boolean = {
      val full = size() > dataMapLimit
      if (full) logger.warn("stageIdToJobId reach limit")
      full
    }
  }
  private val jobIdToData = new util.LinkedHashMap[JobId, JobStatData](
    DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, true) {
    override protected def removeEldestEntry(eldest: util.Map.Entry[JobId, JobStatData]): Boolean = {
      val full = size() > dataMapLimit
      if (full) logger.warn("jobIdToData reach limit")
      full
    }
  }

  // event queue definition
  private val eventQueue = new LinkedBlockingQueue[SparkListenerEvent](eventQueueLen)
  private var warnCnt: Int = 0

  /**
    * function needed to realized in SparkFirehoseListener
    */
  override def onEvent(event: SparkListenerEvent): Unit = {
    var cnt = eventQueueOfferTry
    do {
      var success = true
      try {
        success = eventQueue.offer(event, eventQueueOfferTimeout, TimeUnit.MILLISECONDS)
      } catch {
        case e: Throwable =>
          logger.error("offer event to queue failed", e)
          success = false
      }
      if (!success) {
        eventQueue.poll()
        if (warnCnt % 10000 == 0) {
          logger.warn("poll oldest event in queue")
          warnCnt = 0
        }
        warnCnt += 1
        cnt -= 1
      } else {
        warnCnt = 0
        cnt = 0
      }
    } while (cnt > 0)
  }

  // start one listener thread to handle events in the blocking queue
  // this will become thread security problem if use more than one instead
  new Thread("OutputFilesListenerThread") {
    setDaemon(true)

    override def run(): Unit = {
      logger.info("JobInfo Listener startup")
      while (true) {
        var event: SparkListenerEvent = null
        try {
          event = eventQueue.poll(eventQueuePollTimeout, TimeUnit.MILLISECONDS)
        } catch {
          case e: Throwable => logger.error("poll event from queue failed", e)
        }
        event match {
          case e: SparkListenerApplicationStart => statAppStart(e)
          case e: SparkListenerJobStart => statJobStart(e)
          case e: SparkListenerTaskEnd => statTaskEnd(e)
          case e: SparkListenerStageCompleted => statStageCompleted(e)
          case e: SparkListenerJobEnd => statJobEnd(e)
          case _ =>
        }
      }
    }
  }.start()

  /**
    * statistics of AppStart event
    *   - fetch `appName` / `userName` / `appId`
    */
  def statAppStart(event: SparkListenerApplicationStart): Unit = {
    appName = event.appName
    userName = event.sparkUser
    appId = event.appId.get
  }

  /**
    * statistics of JobStart event
    *   - fetch `startTime` and map stageId to JobId
    */
  def statJobStart(event: SparkListenerJobStart): Unit = {
    jobIdToData.put(event.jobId, JobStatData(System.currentTimeMillis(), 0L))
    event.stageIds.foreach(stageIdToJobId.put(_, event.jobId))
  }

  /**
    * statistics of TaskEnd event
    *   - fetch `inputBytes`
    */
  def statTaskEnd(event: SparkListenerTaskEnd): Unit = {
    val info = event.taskInfo
    // stageAttemptId is used for speculation task
    if (info != null && event.stageAttemptId != -1) {
      // get task metrics according to whether task is successful or not
      val metrics = event.reason match {
        case Success =>
          Option(event.taskMetrics)
        case _ =>
          None
      }

      if (metrics.isDefined) {
        // get old metrics
        val taskId = info.taskId
        val key = (event.stageId, event.stageAttemptId)
        var stageStatData = stageIdToData.get(key)
        if (stageStatData == null) {
          stageStatData = new StageStatData
          stageIdToData.put(key, stageStatData)
        }
        val oldMetrics = stageStatData.taskData.get(taskId)

        // accumulate metrics
        metricsAccumulate(metrics, oldMetrics, stageStatData)

        // temp metrics state storage
        stageStatData.taskData(taskId) = metrics.get
      }
    }
  }

  private def metricsAccumulate(currMetrics: Option[TaskMetrics], oldMetrics: Option[TaskMetrics],
                                stageStatData: StageStatData): Unit = {
    // input statistics
    val bytesReadDelta = currMetrics.get.inputMetrics.map(_.bytesRead).getOrElse(0L) -
      oldMetrics.flatMap(_.inputMetrics).map(_.bytesRead).getOrElse(0L)
    if (bytesReadDelta > 0) {
      stageStatData.inputBytes += bytesReadDelta
    }
  }

  /**
    * statistics of StageCompleted event
    *
    */
  def statStageCompleted(event: SparkListenerStageCompleted): Unit = {
    // get stage stat data
    val stage = event.stageInfo
    val key = (stage.stageId, stage.attemptId)
    var stageStatData = stageIdToData.get(key)
    if (stageStatData == null) {
      stageStatData = new StageStatData
      stageIdToData.put(key, stageStatData)
    }
    val inputBytes = stageStatData.inputBytes

    if (inputBytes > 0 && stageIdToJobId.containsKey(stage.stageId)) {
      var jobStatData = jobIdToData.get(stageIdToJobId.get(stage.stageId))
      jobStatData = if (jobStatData == null) JobStatData(System.currentTimeMillis(), 0L) else jobStatData
      jobStatData.inputBytes += inputBytes
    }

    // release cache
    stageIdToJobId.remove(stage.stageId)
    stageIdToData.remove(key)
  }

  /**
    * statistics of JobEnd event
    *   - fetch `jobId` / `currTime` / `jobStatus` and output job info to logs
    */
  def statJobEnd(event: SparkListenerJobEnd): Unit = {
    // fetch job info left
    val currTime = sdf.format(new Date(System.currentTimeMillis()))
    val jobId = event.jobId
    val (startTime, jobInputBytes) = jobIdToData.get(jobId) match {
      case JobStatData(a, b) => (sdf.format(new Date(a)), b)
    }
    val jobStatus = event.jobResult match {
      case JobSucceeded =>
        "SUCCEEDED"
      case _ =>
        "FAILED"
    }

    // write out job info string
    val jobInfoStr = s"$taskQueue\t$appName\t$userName\t$appId\t$jobId\t$startTime\t$jobInputBytes\t$jobStatus\t$currTime"
    try {
      loggerInfo.info(jobInfoStr)
    } catch {
      case e: Throwable => logger.error("write out failed", e)
    }

    // release cache
    jobIdToData.remove(jobId)
  }

}
