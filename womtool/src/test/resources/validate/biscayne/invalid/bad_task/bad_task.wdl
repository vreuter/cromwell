version 1.1-draft

workflow oops {
  call oopsie
}

task oopsie {
  input {
    String str
  }
  command { echo ${str} }
  runtime { docker: docker_image }
}
