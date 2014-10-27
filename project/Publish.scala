import sbt._, Keys._

object Publish {

  val settings =
    bintray.Plugin.bintrayPublishSettings ++ Seq(
      bintray.Keys.bintrayOrganization in bintray.Keys.bintray := Some("pellucid")
    )
}
