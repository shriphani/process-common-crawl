(ns process-common-crawl.download-warcs
  "Download warc files from the common crawl corpus"
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [process-common-crawl.core :as core]))

(def output-dir "/bos/tmp19/spalakod/clueweb12pp/common_crawl/")

(def json-locs-files (string/split-lines
                      (slurp "locations_jsons")))

(defn -main
  [& args]
  (doseq [json-file json-locs-files]
    (let [loc-dir (string/replace json-file
                                  #".json$"
                                  "")]
     (do (.mkdir
          (io/as-file
           (str output-dir
                loc-dir)))
         (core/process-common-crawl-data (str output-dir
                                              "host_locations/"
                                              json-file)
                                         (str output-dir
                                              loc-dir))))))
