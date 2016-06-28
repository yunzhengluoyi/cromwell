// Update this when a new patch of Reactive Platform is available
val rpVersion = "15v09p04i01"

// Update this when a major version of Reactive Platform is available
val rpUrl =  "https://dl.bintray.com/typesafe/instrumented-reactive-platform"

addSbtPlugin("com.typesafe.rp" % "sbt-typesafe-rp" % rpVersion)

// The resolver name must start with typesafe-rp
resolvers += "typesafe-rp-mvn" at rpUrl

// The resolver name must start with typesafe-rp
resolvers += Resolver.url("typesafe-rp-ivy",
  url(rpUrl))(Resolver.ivyStylePatterns)