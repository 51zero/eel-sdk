//package io.eels.plan
//
//import com.typesafe.config.ConfigFactory
//
//abstract class Plan {
//
//  val config = ConfigFactory.load()
//
//  val timeout = config.getDuration("eel.execution.timeout").toNanos()
//
//  // do we want to fail execution if one of the plan tasks fail
////  val failureEnabled = config.getBoolean("eel.execution.plan.fail.enable")
//
//  //  fun <T> raiseExceptionOnFailure(futures: List<Future<T>>): Unit {
//  //    if (failureEnabled) {
//  //      Await.result(Futures.firstThrowableOf(futures), 1.minute) match {
//  //        case Some(t) => throw t
//  //        case _ =>
//  //      }
//  //    }
//  //  }
//}