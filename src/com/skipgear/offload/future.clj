(ns com.skipgear.offload.future
  (:gen-class)
  (:refer-clojure :exclude [future future-call])
  (:require [com.skipgear.offload.jar :as jar]
            [clojure.java.io :as jio]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            [com.skipgear.offload.stream-runner :as stream-runner]))

(def ^:dynamic *instance-identity*
  "Use this to uniquely identify the instantiation of the
  application. It should not change whilst running."
  (str (java.util.UUID/randomUUID)))

(defn in-thread
  [opts ns f]
  {:result (f)})

(defn in-process
  [opts ns f]
  (clojure.core/future-call (fn [] {:result (f)})))

(def ^:dynamic  *core-bound* false)

(defn core-bound
  "Execute a CPU intensive workload limited by the number of cores + 2
  on the machine.

  Running a core-bound function in a stack that is already core bound
  can lead to deadlocks, so exceptions are thrown if this is
  attempted."
  [opts ns f]
  (when *core-bound*
    (throw (Exception. "Cannot be core-bound within a core-bound stack")))
  (.submit clojure.lang.Agent/pooledExecutor
           ^Callable (fn []
                       (binding [*core-bound* true] {:result (f)}))))
(defn repl?
  "Attempts to discern if code is running in a repl.
   Based on heuristics."
  []
  (str/includes? (System/getProperty "java.class.path") "/src"))

(defn jar-name
  []
  (str "offload-" *instance-identity* ".jar"))

(defn local-stream
  "The function to be exeucted is streamed to the process
   and the response is streamed back."
  [{:keys [] :as config} ns fx]
  (clojure.core/future
    (jar/cached-make-uber-jar (jar-name))
    (log/info (jar-name) " Size: " (.length (jio/file (jar-name))))
    (.deleteOnExit (jio/file (jar-name)))
    (stream-runner/call-local {:classpath (jar-name)
                               :fx fx
                               :ns ns})))

(defn ssh-stream
  "The function to be exeucted is streamed to the server
   and the response is streamed back."
  [{:keys [host] :as config} ns fx]
  (clojure.core/future
    (jar/cached-make-uber-jar (jar-name))
    (log/info (jar-name) " Size: " (.length (jio/file (jar-name))))
    (.deleteOnExit (jio/file (jar-name)))
    ;;Push file to server
    (stream-runner/push-file-once (jar-name) host (jar-name) config)
    (stream-runner/call-remote {:classpath (jar-name)
                                :host host
                                :fx fx
                                :ns ns})))

(defn object-stream-shell
  [{:keys [host xmx envs user port identity-file] :as config}]
  (let [create (fn []
                 (locking jar-name
                   (jar/cached-make-uber-jar (jar-name))
                   (log/info (jar-name) " Size: " (.length (jio/file (jar-name))))
                   (.deleteOnExit (jio/file (jar-name)))
                   ;;Push file to server
                   (when host
                     (stream-runner/push-file-once (jar-name) host (jar-name) config))
                   (let [command (filter some?
                                         ["java"
                                          "-XX:+ExitOnOutOfMemoryError"
                                          (when xmx (str "-Xmx" xmx))
                                          "-cp"
                                          (jar-name)
                                          "com.skipgear.offload.stream_runner" "object"])
                         command (if host
                                   (filter
                                    some?
                                    ["ssh"
                                     "-o" "StrictHostKeyChecking=no"
                                     "-o" "UserKnownHostsFile=/dev/null"
                                     (when port "-p") port
                                     (when identity-file "-i") identity-file
                                     (str (when (not (str/blank? user))
                                            (str user "@"))
                                          host)
                                     (str/join " "
                                               (concat (map (fn [[k v]]
                                                              (str (name k) "="v)) envs)
                                                       command))])
                                   command)]
                     (stream-runner/object-stream-shell command {:env envs}))))
        at (atom (create))]
    (fn shell-getter[]
      (let [a @at]
        (assoc a :release
               (fn shell-release [healthy?]
                 (when-not healthy?
                   (.destroy(:proc a))
                   (reset! at (create)))))))))

(defn object-stream-pool
  [{:keys [host instances] :as config}]
  (let [q (java.util.concurrent.ArrayBlockingQueue. instances true)]
    (dotimes [i instances]
      (.put q (object-stream-shell config)))
    (fn pool-getter []
      (let [shell (.take q)
            {:keys [os is proc release] :as x} (shell)]
        (assoc x
               :release
               (fn pool-release [healthy?]
                 (release healthy?)
                 (.put q shell)))))))

(defn healthy-result?
  [{:keys [result]}]
  (try (not (and (some? result)
                 (instance? Throwable result)
                 (some? (.getCause ^Throwable result))
                 (instance? java.lang.OutOfMemoryError (.getCause ^Throwable result))))
       (catch Exception e true)))

(defn object-stream
  [{:keys [host object-stream-shell]
    :as config} ns fx]
  (clojure.core/future
    (let [attempt (fn []
                    (let [{:keys [os is release]} (object-stream-shell)
                          {:keys [result exception]}
                          (locking os
                            (try
                              (.writeObject os {:ns ns :fx fx})
                              (.flush os)
                              {:result (.readObject @is)}
                              (catch Exception e
                                {:exception e})))]
                      (release (and (nil? exception)
                                    (healthy-result? result)))
                      (if exception
                        (throw exception)
                        result)))]
      (try (attempt)
           (catch Exception e (attempt))))))

(def ^:dynamic *engine* in-process)
(def ^:dynamic *opts* {})

(defn future-call
  [engine opts ns f]
  (let [engine (or engine *engine*)
        opts (or opts *opts*)]
    (engine opts ns f)))

(defmacro future-exec
  "Primary entry point that combines engine + opts + body"
  [engine opts ns & body]
  `(future-call ~engine ~opts ~ns (^{:once true} fn* [] ~@body)))

(defmacro future
  "Primary entry point - use with 'with' macro to provide engine and opts"
  [& body]
  `(future-call nil nil ~(name (ns-name *ns*)) (^{:once true} fn* [] ~@body)))

(defmacro with
  [[engine opts] & body]
  `(binding [*engine* ~engine
             *opts* ~opts]
     ~@body))
