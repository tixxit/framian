import scala.xml.{ Node, NodeSeq }
import scala.xml.transform.{ RewriteRule, RuleTransformer }
import sbt._, Keys._

object Publish {
  val pomDependencyExclusions = SettingKey[Seq[(String, String)]]("pom-dependency-exclusions", "Group ID / Artifact ID dependencies to exclude from the maven POM.")

  val settings =
    bintray.Plugin.bintrayPublishSettings ++ Seq(
      bintray.Keys.bintrayOrganization in bintray.Keys.bintray := Some("pellucid"),
      pomDependencyExclusions <<= pomDependencyExclusions ?? Seq(),
      pomPostProcess := { (node: Node) =>
        val exclusions = pomDependencyExclusions.value.toSet
        val rule = new RewriteRule {
          override def transform(n: Node): NodeSeq = {
            if (n.label == "dependency") {
              val groupId = (n \ "groupId").text
              val artifactId = (n \ "artifactId").text
              if (exclusions.contains(groupId -> artifactId)) {
                NodeSeq.Empty
              } else {
                n
              }
            } else {
              n
            }
          }
        }
        new RuleTransformer(rule).transform(node)(0)
      }
    )
}
