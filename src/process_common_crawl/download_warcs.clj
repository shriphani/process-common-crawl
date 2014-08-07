(ns process-common-crawl.download-warcs
  "Download warc files from the common crawl corpus"
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [process-common-crawl.core :as core])
  (:use [clojure.pprint :only [pprint]])
  (:import [java.util.zip GZIPOutputStream]))

(def host-locs-dir "/bos/tmp19/spalakod/clueweb12pp/common_crawl/")

(def output-dir "/bos/tmp17/spalakod/common_crawl/")

(defn handle-json-file
  [json-file]
  (let [out-filename (string/replace json-file
                                     #".json$"
                                     ".corpus")

        out-handle (io/writer
                    (str output-dir out-filename))
        
        keys-offsets (core/get-keys-offsets
                      (str host-locs-dir
                           "host_locations/"
                           json-file))]
    (do (doall
         (doseq [[k u o] keys-offsets]
           (let [rdr (core/get-record-at-offset k)
                 record (.get rdr (long o))]
             (binding [*out* out-handle]
               (pprint {:url  u
                        :body (String.
                               (char-array
                                (map
                                 char
                                 (take-while
                                  pos?
                                  (repeatedly
                                   (fn []
                                     (.read record)))))))})
               (flush)
               (.close rdr)))))
        (.close out-handle))))

(defn -main
  [& args]
  (let [json-locs-files (string/split-lines
                         (slurp (first args)))]
   (doseq [json-file json-locs-files]
     (handle-json-file json-file))))
