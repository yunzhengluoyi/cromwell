package cromwell.database.slick.tables

import cromwell.database.sql.tables.CallCachingHashEntry

trait CallCachingHashComponent {

  this: DriverComponent with CallCachingResultMetaInfoComponent =>

  import driver.api._

  class CallCachingHashEntries(tag: Tag) extends Table[CallCachingHashEntry](tag, "CALL_CACHING_HASH") {
    def callCachingHashId = column[Int]("CALL_CACHING_HASH_ID", O.PrimaryKey, O.AutoInc)
    def hashKey = column[String]("HASH_KEY")
    def hashValue = column[String]("HASH_VALUE")
    def resultMetaInfoId = column[Int]("RESULT_METAINFO_ID")

    override def * = (hashKey, hashValue, resultMetaInfoId, callCachingHashId.?) <>
      (CallCachingHashEntry.tupled, CallCachingHashEntry.unapply)

    def cchUniquenessConstraint = index("UK_CALL_CACHING_HASH", (hashKey, resultMetaInfoId), unique = true)
    def resultMetaInfoIdForeignKey = foreignKey("CCH_RESULT_METAINFO_ID_FK", resultMetaInfoId, callCachingResultMetaInfo)(_.callCachingResultMetaInfoId)
  }

  protected val callCachingHashes = TableQuery[CallCachingHashEntries]

  val callCachingHashAutoInc = callCachingHashes returning callCachingHashes.map(_.callCachingHashId)

  /**
    * Find all RESULT_METAINFO_IDs which match a given hashkey/hashvalue combination
    */
  val resultMetaInfoIdsForHashMatch = Compiled(
    (hashKey: Rep[String], hashValue: Rep[String]) => for {
      hashEntry <- callCachingHashes
      if hashEntry.hashKey === hashKey && hashEntry.hashValue === hashValue
    } yield hashEntry.resultMetaInfoId)
}
