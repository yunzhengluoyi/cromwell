package cromwell.database.sql.tables

case class JobStoreResultSimpletonEntry
(
  simpletonKey: String,
  simpletonValue: String,
  wdlType: String,
  jobStoreEntryId: Int,
  jobStoreSimpletonEntryId: Option[Int] = None
)

