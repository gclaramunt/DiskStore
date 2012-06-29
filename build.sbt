name := "DiskStore"

version := "1.0"

scalaVersion := "2.9.1"

libraryDependencies += "org.specs2" %% "specs2" % "1.10" % "test"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.9" % "test"

scalacOptions ++= Seq("-unchecked", "-deprecation")
