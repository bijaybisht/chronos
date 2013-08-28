package com.airbnb.scheduler.api

import java.util.logging.{Level, Logger}
import javax.ws.rs.{WebApplicationException, Path, GET, Produces}
import javax.ws.rs.core.{MediaType, Response}
import javax.ws.rs.core.Response.Status
import scala.Array

import com.airbnb.scheduler.config.SchedulerConfiguration
import scala.collection.mutable.ListBuffer
import com.airbnb.scheduler.jobs._
import com.airbnb.scheduler.graph.JobGraph
import com.google.inject.Inject
import com.yammer.metrics.annotation.Timed
import com.fasterxml.jackson.databind.ObjectMapper
import scala.collection.JavaConversions._
/**
 * The REST API to the PerformanceResource component of the API.
 * @author Matt Redmond (matt.redmond@airbnb.com)
 * Returns a list of jobs, sorted by 50th percentile run times.
 */

@Path(PathConstants.jobBasePath)
@Produces(Array(MediaType.APPLICATION_JSON))
class PerformanceResource @Inject()(
                                     val jobScheduler: JobScheduler,
                                     val jobGraph: JobGraph,
                                     val configuration: SchedulerConfiguration,
                                     val jobMetrics: JobMetrics) {

  private[this] val log = Logger.getLogger(getClass.getName)

  @Path(PathConstants.jobsPerformancePath)
  @Timed
  @GET
  def list(): Response = {
    try {
      var jobs = ListBuffer[(String, Double)]()
      val mapper = new ObjectMapper()
      for (jobNameString <- jobGraph.dag.vertexSet()) {
        val node = mapper.readTree(jobMetrics.getJsonStats(jobNameString))
        val time =
          if (node.has("median") && node.get("median") != null) node.get("median").asDouble()
          else -1
        jobs.append((jobNameString, time))
      }
      jobs = jobs.sortBy(_._2).reverse

      Response.ok(jobs).build
    } catch {
      case ex: Throwable => {
        log.log(Level.WARNING, "Exception while serving request", ex)
        throw new WebApplicationException(Status.INTERNAL_SERVER_ERROR)
      }
    }
  }
}
