package cromwell.engine.backend.sge

import java.nio.file.FileSystem

import cromwell.core.OldCallContextOld
import cromwell.engine.backend.local.OldStyleLocalCallEngineFunctions

class SgeCallEngineFunctionsOld(fileSystems: List[FileSystem], callContext: OldCallContextOld) extends OldStyleLocalCallEngineFunctions(fileSystems, callContext)
