package launchers

import java.io.{BufferedWriter, File, FileWriter}
import java.util.TimerTask

import org.joda.time.{DateTime, Period}

import scala.io.Source


class DummyLauncher() extends TimerTask {

  def run {
    println("Dummy Launcher")
    Thread.sleep(3000)
    println("I'm done!")

  }
}

