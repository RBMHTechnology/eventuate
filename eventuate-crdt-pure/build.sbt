// make protobuf definitions of core module available in this module (needed for compilation with protoc)
includePaths in protobufConfig ++= (sourceDirectories in (LocalProject("core"), protobufConfig)).value
