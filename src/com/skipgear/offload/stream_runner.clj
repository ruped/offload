(ns com.skipgear.offload.stream-runner
  "A simplified public static void main entry point:
   - reads function from standard in
   - writes output to standard out
  "
  (:gen-class)
  (:require [clojure.string :as str]
            [clojure.java.io :as jio]
            [clojure.java.shell :as sh]
            [taoensso.timbre :as log]
            [com.skipgear.offload.jar :as jar]))

(defn eg1 [] (str "[ABC" (+ 123 234) "DEF]"))

(defn push-file
  [local-path remote-host remote-path & [{:keys [port user identity-file] :as props}]]
  (log/info (str "Push File size*: " (.length (jio/file local-path))))
  (let [command (filter
                 some?
                 ["scp"
                  "-o" "StrictHostKeyChecking=no"
                  "-o" "UserKnownHostsFile=/dev/null"
                  (when port "-P") port
                  (when identity-file "-i") identity-file
                  local-path
                  (str (when (not (str/blank? user))
                         (str user "@"))
                       remote-host ":"remote-path)])
        {:keys [exit out err]} (apply sh/sh command)]
    (when-not (zero? exit)
      (throw (Exception. (str "Call-Remote - non zero exit (" exit ")\n"
                              "COMMAND:" command "\n"
                              "OUT: " out "\n"
                              "ERR: " err))))))

(def push-file-once (memoize push-file))

(defn call
  [command input]
  (let [_ (log/info (str "Call- Input size: " (alength input)))
        {:keys [exit out err]}
        (apply sh/sh
               command
               [:out-enc :bytes
                :in input])]
    (log/info (str "Call- Output size: " (alength out)))
    (when-not (zero? exit)
      (throw (Exception. "Call- non zero exit")))
    out))

(defn call-local
  [{:keys [classpath jvm-opts fx ns]}]
  (let [input (jar/serialize {:fx fx :ns ns})
        out (call ["java" "-cp" classpath
                   "com.skipgear.offload.stream_runner"]
                  input)]
    (jar/deserialize out)))

(defn call-remote
  [{:keys [classpath jvm-opts host fx ns]}]
  (let [input (jar/serialize {:fx fx :ns ns})
        out (call
             ["ssh" host
              (str "java " "-cp " classpath
                   " com.skipgear.offload.stream_runner")])]
    (jar/deserialize out)))

(defn framed-shell
  "Messages are passed to and from the"
  [cmd & [opts]]
  (let [proc (.exec (Runtime/getRuntime)
                    ^"[Ljava.lang.String;" (into-array cmd)
                    (#'sh/as-env-strings (:env opts))
                    (jio/as-file (:dir opts)))
        {:keys [in in-enc out-enc]} opts]
    {:proc proc
     :is (java.io.DataInputStream. (.getInputStream proc))
     :os (java.io.DataOutputStream. (.getOutputStream proc))}))

(defn object-stream-shell
  "Messages are passed to and from the"
  [cmd & [opts]]
  (let [proc (.exec (Runtime/getRuntime)
                    ^"[Ljava.lang.String;" (into-array cmd)
                    (#'sh/as-env-strings (:env opts))
                    (jio/as-file (:dir opts)))
        {:keys [in in-enc out-enc]} opts]
    {:proc proc
     :is (delay (java.io.ObjectInputStream.
                 (java.io.BufferedInputStream.
                  (.getInputStream proc)
                  (* 128 1024))))
     :os (java.io.ObjectOutputStream.
          (java.io.BufferedOutputStream.
           (.getOutputStream proc)
           (* 128 1024)))}))

(defn send-frame
  [{:keys [proc is os]} bytes]
  (.writeLong os (alength bytes))
  (.write os bytes)
  (.flush os))

(defn get-frame
  [{:keys [proc is os]}]
  (let [len (.readLong is)
        bs (byte-array len)]
    (.readFully is bs)
    bs))

(defn close
  [{:keys [proc is os]}]
  (.close proc))

(defn call-framed
  [shell {:keys [fx ns]}]
  (let [input (jar/serialize {:fx fx :ns ns})
        _ (send-frame shell input)
        out (get-frame shell)]
    (jar/deserialize out)))

(defmacro with-out-and-err
  "Similar to with-out-str - docstring:
  Evaluates exprs in a context in which *out* is bound to a fresh
  StringWriter.  Returns the string created by any nested printing
  calls."
  [& body]
  `(let [s# (new java.io.StringWriter)
         e# (new java.io.StringWriter)]
     (binding [*out* s#
               *err* e#]
       (let [x# (do ~@body)]
         {:result x#
          :out (str s#)
          :err (str e#)}))))

(defn handle-request
  [in out]
  (let [{ns :ns
         fx :fx} (jar/deserialize (jar/slurp-bytes in))
        result (with-out-and-err
                 (try (when ns (require (symbol ns)))
                      (fx)
                      (catch Throwable t
                        (Exception. (str "NS:" (type ns) ns) t))))
        result-serial (jar/serialize result)]
    (with-open [o out]
      (jio/copy (jio/input-stream result-serial) o))))

(defn handle-framed-requests
  [in out]
  (loop []
    (let [shell {:is in :os out}
          bytes (get-frame shell)
          {ns :ns
           fx :fx} (jar/deserialize shell)
          result (with-out-and-err
                   (try (when ns (require (symbol ns)))
                        (fx)
                        (catch Throwable t
                          (Exception. (str "NS:" (type ns) ns) t))))
          result-serial (jar/serialize result)]
      (send-frame shell result-serial))
    (recur)))

(defn handle-object-requests
  [in out]
  (loop []
    (let [{ns :ns
           fx :fx} (.readObject @in)
          ns (if (string? ns) (symbol ns) ns)
          result (with-out-and-err
                   (try (when (and ns
                                   (nil? (find-ns ns)))
                          (require ns))
                        (fx)
                        (catch Throwable t
                          (Exception. (str "NS:" (type ns) "-" ns) t))))]
      (.writeObject out result)
      (.flush out))
    (recur)))

(defn -main
  "A simplified entry point that reads "
  [& [task]]
  (case task
    "framed"
    (handle-framed-requests (java.io.DataInputStream. System/in)
                            (java.io.DataOutputStream. System/out))
    "object"
    (handle-object-requests (delay (java.io.ObjectInputStream.
                                    (java.io.BufferedInputStream.  System/in (* 128 1024))))
                            (java.io.ObjectOutputStream.
                             (java.io.BufferedOutputStream. System/out (* 128 1024))))
    (handle-request System/in System/out))
  (shutdown-agents))
