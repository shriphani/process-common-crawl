(ns process-common-crawl.process-arc-file
  "Code to process arc file"
  (:require [aws.sdk.s3 :as s3]
            [cheshire.core :as json]
            [clj-time.core :as t]
            [clojure.java.io :as io]
            [subotai.timestamps :as ts])
  (:use [clojure.walk])
  (:import [java.io ByteArrayOutputStream StringWriter]
           [java.util.zip GZIPInputStream]
           [org.archive.io.arc ARCReaderFactory]))

(defn load-locations-file
  [rdr]
  (map
   keywordize-keys
   (json/parsed-seq rdr)))

(defn process-record
  [a-record]
  (let [os (ByteArrayOutputStream.)

        payload
        (do (.dump a-record os)
            (String. (.toByteArray os)
                     "UTF-8"))]
    (ts/document-detected-timestamps payload)))

(defn get-document-key
  [a-document]
  (str "common-crawl/parse-output/segment/"
                          (:arcSourceSegmentId a-document)
                          "/"
                          (:arcFileDate a-document)
                          "_"
                          (:arcFileParition a-document)
                          ".arc.gz"))

(defn document-arc-file
  [locations-file]
  (let [rdr (io/reader locations-file)
        documents
        (load-locations-file rdr)

        idv (map vector (iterate inc 0) documents)

        to-return
        (reduce
         (fn [acc [i d]]
           (let [doc-key (get-document-key d)]
             (if (nil? (get acc doc-key))
               (merge acc {doc-key (str i ".arc.gz")})
               acc)))
         {}
         idv)]
    (do (.close rdr)
        to-return)))

(defn in-clueweb?
  [a-date]
  (t/within?
   (t/interval (t/date-time 2012 2)
               (t/date-time 2012 5))
   a-date))

(defn close-to-clueweb?
  [a-date]
  (t/within?
   (t/interval (t/date-time 2012 1)
               (t/date-time 2012 7))
   a-date))

(defn process-downloaded-corpus
  [locations-file corpus-loc]
  (let [document-loc (document-arc-file locations-file)

        rdr (io/reader locations-file)

        documents
        (load-locations-file rdr)

        idv (map vector (iterate inc 0) documents)]
    (doseq [[i d] idv]
      (let [doc-key (get-document-key d)
            arc-file (str corpus-loc (get document-loc doc-key))
            offset (:arcFileOffset d)

            arc-rdr (ARCReaderFactory/get arc-file)
            arc-record (.get arc-rdr offset)]
        (when (some
               close-to-clueweb?
               (process-record arc-record))
          (println (.getHeaderString arc-record)))
        (.close arc-rdr)))
    (.close rdr)))

(defn -main
  [& args]
  (let [out-file (last args)
        out-handle (io/writer out-file)]
    (binding [*out* out-handle]
      (apply process-downloaded-corpus (take 2 args)))
    (.close out-handle)))
