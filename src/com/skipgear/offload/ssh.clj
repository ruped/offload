(ns com.skipgear.offload.stream-runner
  "Process:
  - check if uberjar exists locally and younger than current uptime
  - generate uberjar
  - check if uberjar already exists on server
  - push jar file server
  - rsync UP working directory
  - execute remote program
  - rsync DOWN working directory
  - return result
  "
  (:require [clojure.java.io :as jio]
            [clojure.java.shell :as sh]
            [com.skipgear.offload.jar :as jar]))

(defn call-scp
  [{:keys [classpath jvm-opts]}]
  (let [{:keys [exit out err]}
        (time (sh/sh "java" "-cp" classpath
                     "com.skipgear.offload.stream_runner"
                     :out-enc :bytes
                     :in (jar/serialize eg1)))]
    (when-not (zero? exit)
      (throw (Exception. "Call-Local - non zero exit")))
    (jar/deserialize out)))

(defn call-ssh
  [{:keys [classpath jvm-opts]}]
  (let [{:keys [exit out err]}
        (time (sh/sh "java" "-cp" classpath
                     "com.skipgear.offload.stream_runner"
                     :out-enc :bytes
                     :in (jar/serialize eg1)))]
    (when-not (zero? exit)
      (throw (Exception. "Call-Local - non zero exit")))
    (jar/deserialize out)))
