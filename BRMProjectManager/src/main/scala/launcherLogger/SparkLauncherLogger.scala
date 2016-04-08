package launcherLogger

import java.io._

/**
 * Created by cnavarro on 5/04/16.
 */
class SparkLauncherLogger extends Runnable {
  private var reader: BufferedReader = null
  private var name: String = null

  def this(is: InputStream, name: String) {
    this()
    this.reader = new BufferedReader(new InputStreamReader(is))
    this.name = name
  }

  def run {
    System.out.println("InputStream " + name + ":")
    val file = new File(name+".log")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("starting"+"\n")

    try {
      var line: String = reader.readLine
      while (line != null) {
        System.out.println(line)
        line = reader.readLine
        bw.write(line+"\n")
      }

    }
    catch {
      case e: IOException => {
        e.printStackTrace
      }
    }finally {
      reader.close
      bw.close()
    }
  }
}