(ns com.skipgear.offload.jar
  (:gen-class)
  (:require [clojure.string :as s]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [taoensso.timbre :as log]
            [clojure.java.classpath :as cp]
            [leiningen.uberjar :as uberjar]
            [clojure.java.io :as jio]
            [leiningen.jar :as ljar])
  (:import java.util.Base64))

;;;;;;;;;;;;;;;;;;;; IO Helpers

(defn serializable? [v]
  (instance? java.io.Serializable v))

(defn serialize
  "Serializes value, returns a byte array"
  [v]
  (let [buff (java.io.ByteArrayOutputStream. 1024)]
    (with-open [dos (java.io.ObjectOutputStream. buff)]
      (.writeObject dos v))
    (.toByteArray buff)))

(defn deserialize
  "Accepts a byte array, returns deserialized value"
  [bytes]
  (with-open [dis (java.io.ObjectInputStream.
                   (java.io.ByteArrayInputStream. bytes))]
    (.readObject dis)))

(defn slurp-bytes
  "Slurp the bytes from a slurpable thing"
  [x]
  (with-open [out (java.io.ByteArrayOutputStream.)]
    (clojure.java.io/copy (clojure.java.io/input-stream x) out)
    (.toByteArray out)))

(defn encode [to-encode]
  (.encodeToString (Base64/getEncoder) to-encode))

(defn decode [to-decode]
  (.decode (Base64/getDecoder) to-decode))

;;;;;;;;;;;;;;;;;;;; Eval Helpers

(do (ns leiningen.uberjar)
    (def default-merger
      [(fn [in out file prev]
         (when-not prev
           (.setMethod file  java.util.zip.ZipEntry/DEFLATED)
           (.setCompressedSize file -1)
           (.putNextEntry out file)
           (io/copy (.getInputStream in file) out)
           (.closeEntry out))
         ::skip)
       (constantly nil)])
    (ns  com.skipgear.offload.jar))

(defn make-uber-jar*
  "Borrowed and modified from lein uberjar."
  [uber-jar-path]
  (log/info "Making Uber Jar " (io/file uber-jar-path))
  (with-open [jar-os (-> (str uber-jar-path ".prejar")
                         (java.io.FileOutputStream.)
                         (java.io.BufferedOutputStream.)
                         (java.util.jar.JarOutputStream. (ljar/make-manifest  {})))]
    (#'ljar/copy-to-jar
     {} jar-os #{}
     {:type :paths :paths (remove (comp #(s/includes? % "/classes")
                                        #(.getAbsolutePath %))
                                  (cp/classpath-directories))}))
  (with-open [out (-> uber-jar-path
                      (java.io.FileOutputStream.)
                      (java.util.zip.ZipOutputStream.))]
    (.setLevel out 3) ;; 9 = 37m 17s, 5 = 37m, 18s,  2/3 = 38M, 14s,  1 = 39 M 14s, 0 = 16s 81M
    (.setMethod out java.util.zip.ZipOutputStream/DEFLATED)
    (let [jars (cons (java.io.File. (str uber-jar-path ".prejar"))
                     (filter cp/jar-file? (cp/classpath)))]
      (uberjar/write-components {:uberjar-exclusions
                                 [#"(?i)^META-INF/[^/]*\.(SF|RSA|DSA)$"]} jars out)
      (.delete (java.io.File. (str uber-jar-path ".prejar"))))))

(defn make-uber-jar
  [uber-jar-path]
  (let [non-jdk-classpath (remove (comp #(s/includes? % "/openjdk") #(.getAbsolutePath %)) (cp/classpath))]
    (if (and (empty? (cp/classpath-directories))
             (= 1 (count non-jdk-classpath)))
      (do
        (log/info "Copying uber jar " (first (cp/classpath)) (io/file uber-jar-path))
        (io/copy (first (cp/classpath)) (io/file uber-jar-path)))
      (do
        (log/info "Creating uber jar cp:" (count non-jdk-classpath) "dirs:" (count (cp/classpath-directories)))
        (make-uber-jar* uber-jar-path)))))

(def cached-make-uber-jar
  (memoize make-uber-jar))
