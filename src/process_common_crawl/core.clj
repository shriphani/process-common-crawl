(ns process-common-crawl.core
  "Load a locations file and download stuff from
   S3 and speak / process it."
  (:require [aws.sdk.s3 :as s3]
            [cheshire.core :as json]
            [clojure.java.io :as io])
  (:use [clojure.walk])
  (:import [java.util.zip GZIPInputStream]
           [org.archive.io.arc ARCReader ARCReaderFactory]))

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

(defn get-document-blob-key
  [key]
  (s3/get-object aws-creds
                 s3-bucket
                 key))

(defn get-document-blob
  [a-document]
  (let [document-key (get-document-key a-document)]
    (get-document-blob-key document-key)))

(defn get-keys-offsets
  [a-json-file]
  (let [rdr (io/reader a-json-file)
        documents (load-locations-file rdr)]
    (map
     (fn [doc]
       [(get-document-key doc)
        (:url doc)
        (:arcFileOffset doc)])
     documents)))

(defn get-record-at-offset
  [key]
  (let [instream (-> key
                     get-document-blob-key
                     :content)

        rdr   (ARCReaderFactory/get "wtfisthisarg"
                                    instream
                                    true)]
    rdr))
