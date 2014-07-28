(ns process-common-crawl.core
  "Load a locations file and download stuff from
   S3 and speak / process it."
  (:gen-class :main true)
  (:require [aws.sdk.s3 :as s3]
            [cheshire.core :as json]
            [clojure.java.io :as io])
  (:use [clojure.walk]))

(def s3-bucket "aws-publicdatasets")

(def aws-creds (read-string (slurp "credentials.clj")))

(def downloaded-keys (atom (set [])))

(defn load-locations-file
  [rdr]
  (map
   keywordize-keys
   (json/parsed-seq rdr)))

(defn get-document-key
  [a-document]
  (str "common-crawl/parse-output/segment/"
                          (:arcSourceSegmentId a-document)
                          "/"
                          (:arcFileDate a-document)
                          "_"
                          (:arcFileParition a-document)
                          ".arc.gz"))

(defn get-document-blob
  [a-document]
  (let [document-key (get-document-key a-document)]
    (swap! downloaded-keys (fn [x]
                             (clojure.set/union x
                                                (set [document-key]))))
    (s3/get-object aws-creds
                   s3-bucket
                   document-key)))

(defn process-common-crawl-data
  [locations-file]
  (let [rdr (io/reader locations-file)

        documents
        (load-locations-file rdr)
        idv (map vector (iterate inc 0) documents)]
    (doseq [[index document] idv]
      (let [document-key (get-document-key document)]
        (when (nil?
               (get @downloaded-keys document-key))
          (let [instream  (:content (get-document-blob document))
                outstream (io/output-stream (str index ".arc.gz"))]
            (io/copy instream outstream)))))
    (.close rdr)))

(defn -main
  [& args]
  (let [locations-file (first args)]
    (process-common-crawl-data locations-fil)))
