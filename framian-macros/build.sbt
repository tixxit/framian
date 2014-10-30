
name := "framian-macros"

libraryDependencies += Dependencies.Compile.spire

unmanagedSourceDirectories in Compile += (sourceDirectory in Compile).value / s"scala_${scalaBinaryVersion.value}"

Dependencies.macroParadise

publish := ()

publishLocal := ()
