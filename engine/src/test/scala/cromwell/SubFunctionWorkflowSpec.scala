package cromwell

import java.nio.file.Files

import akka.testkit.EventFilter
import cromwell.util.SampleWdl
import wdl4s._
import wdl4s.types.{WdlArrayType, WdlFileType}
import wdl4s.values.{WdlArray, WdlFile, WdlString}

class SubFunctionWorkflowSpec extends CromwellTestkitSpec {

  object SubEngineFunction extends SampleWdl {
    override def wdlSource(runtime: String = "") =
      """task sub {
        |  String myBamString = "myfilename.bam"
        |  File myBamFile
        |  String swappedStr = sub(myBamString, ".bam$", ".txt")
        |  # at this point myBamFile is not localized so path must be removed
        |  String swappedFile = sub(sub(myBamFile,"/.*/",""), ".bam$", ".txt")
        |
        |  command {
        |    echo ${sub(myBamString, ".bam$", ".txt")}
        |    touch ${swappedFile}
        |  }
        |
        |  output {
        |    Array[File] o = [read_string(stdout()), swappedFile]
        |    String o2 = swappedStr
        |  }
        |}
        |
        |workflow wf {
        |  String test = sub("ab", "a", "b")
        |  call sub
        |}
      """.stripMargin

    val tempDir = Files.createTempDirectory("SubEngineFunction")
    override val rawInputs: WorkflowRawInputs = Map("wf.sub.myBamFile" -> createFile("myfilename.bam", tempDir, "arbitrary content").getAbsolutePath)
  }

  "sub engine function" should {
    "apply a regex to a string-like WdlValue" in {
      val outputs = Map(
        "wf.sub.o" -> WdlArray(WdlArrayType(WdlFileType), Seq(
          WdlFile("myfilename.txt"),
          WdlFile("myfilename.txt")
        )),
        "wf.sub.o2" -> WdlString("myfilename.txt")
      )

      runWdlAndAssertOutputs(
        sampleWdl = SubEngineFunction,
        eventFilter = EventFilter.info(pattern = "Starting calls: wf.sub", occurrences = 1),
        expectedOutputs = outputs
      )
    }
  }

}

