
name := "framian-column"

(sourceGenerators in Compile) <+= (sourceManaged in Compile) map Boilerplate.gen
