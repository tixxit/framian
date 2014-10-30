
scalacOptions in (Compile, doc) ++=
  Seq(
    "-implicits",
    "-sourcepath", baseDirectory.value.getAbsolutePath,
    "-doc-source-url", s"https://github.com/pellucidanalytics/framian/tree/v${version.value}/${baseDirectory.value.getName}€{FILE_PATH}.scala")


autoAPIMappings := true

apiURL := Some(url("https://pellucidanalytics.github.io/framian/api/current/"))
