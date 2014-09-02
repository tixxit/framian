
import net.tixxit.sbt.benchmark.BenchmarkPlugin._

name := "framian-column"

(sourceGenerators in Compile) <+= (sourceManaged in Compile) map Boilerplate.gen

benchmark.settings

libraryDependencies += Dependencies.Benchmark.jmh
