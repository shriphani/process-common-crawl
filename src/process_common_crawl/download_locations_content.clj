(ns process-common-crawl.download-locations-content
  "LaHashem Haaretz Umloah"
  (:require [clj-http.client :as client]
            [clojure.java.io :as io]
            [clojure.string :as string]))

(def common-crawl-dir "/bos/tmp19/spalakod/clueweb12pp/common_crawl/host_locations/")

(defn make-url
  [a-host]
  (str "http://urlsearch.commoncrawl.org/download?q="
       a-host))

(defn handle-host
  [host]
  (let [wrtr (io/writer (str common-crawl-dir host ".json"))]
    (do (io/copy (:body (client/get (make-url host) {:as :stream}))
                 wrtr)
        (.close wrtr))))

(defn process-hosts-list
  [hosts-list]
  (let [hosts (string/split-lines
               (slurp hosts-list))]
    (doseq [host hosts]
      (handle-host host))))

(defn -main
  [& args]
  (-> args
      first
      process-hosts-list))
