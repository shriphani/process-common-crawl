(defproject process_common_crawl "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[cheshire "5.3.1"]
                 [clj-aws-s3 "0.3.9"]
                 [clj-http "0.9.2"]
                 [clj-time "0.8.0"]
                 [org.clojure/clojure "1.6.0"]
                 [org.netpreserve.commons/webarchive-commons "1.1.3"]
                 [subotai "0.2.12"]]
  :main process-common-crawl.download-locations-content)
