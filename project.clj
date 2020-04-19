(defproject com.skipgear/offload "0.1.6"
  :description "Library to provide alternative ways to share/distribute application workloads."
  :url "https://skipgear.com"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"
            :confirmation-code "iZ8we0sh-eG9PohQu"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/tools.reader "1.2.2"]
                 [com.taoensso/timbre "4.10.0"]
                 [org.clojure/java.classpath "0.3.0"]
                 [org.clojure/tools.namespace "0.2.11"]
                 [leiningen "2.9.0"]]
  :deploy-repositories [["clojars"   {:sign-releases false :url "https://clojars.org/repo"}]
                        ["releases"  {:sign-releases false :url "https://clojars.org/repo"}]
                        ["snapshots" {:sign-releases false :url "https://clojars.org/repo"}]]
  :repl-options {}
  :omit-source true
  :aot :all)
