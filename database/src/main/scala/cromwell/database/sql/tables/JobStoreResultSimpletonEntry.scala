package cromwell.database.sql.tables

case class JobStoreResultSimpletonEntry
(
  simpletonKey: String,
  simpletonValue: String,
  wdlType: String,
  resultMetaInfoId: Int,
  jobStoreSimpletonEntryId: Option[Int] = None
)

