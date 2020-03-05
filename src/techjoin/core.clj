(ns techjoin.core
  (:require [spork.util [table :as tbl]]
            [tech.ml.dataset :as ds]
            [tech.io :as io]
            [tech.libs.tablesaw :as tablesaw]
            [tech.v2.datatype.functional :as dfn]
            [tech.ml.dataset.pipeline :as dsp]
            [tech.ml.dataset.pipeline.column-filters :as cf]))

;; lhs - 165,000 rows -

;; 3 columns
;; [single letter,
;;  number as string for id,
;;  larger numbers as string for an other id]

(defn customers []
  (for [i (range 100000)]
    (let [city   (str (rand 10))]
      {:address    (str "Address" i)
       :gender     (rand-nth ["m" "f" "n"])
       :address-id i
       :country-code "99"
       :first-name (str "customer_" i "first")
       :last-name  (str "customer_" i "last")
       :city       city
       :zip-code   (str (repeat 5 city))
       :email      (str "customer_" i "@the-net")
       :huge-field (str "this is a huge field containing a lot of dumb info for bloat" i)})))

(def rhs-fields
  [:operator-id
   :address
   :gender
   :address-id
   :country-code
   :first-name
   :last-name
   :city
   :zip-code
   :email])

(defn random-lhs []
  (for [i (range 165000)]
    {:gender         (rand-nth ["m" "f" "n"])
     :address-id     (str (rand-int 100000))
     :operator-id    (str (rand-int 10000))}))

(defn random-rhs []
  (let [cs  (vec (customers))]
    (for [i (range 180000)]
      (let [c (rand-nth cs)]
        (assoc c :operator-id (rand-int 10000))))))

(defn spit-tables []
  (tbl/records->file (random-lhs) "lhs.csv" :sep "," :field-order [:gender :address-id :operator-id])
  (tbl/records->file (random-rhs) "rhs.csv" :sep "," :field-order rhs-fields))

;; rhs - 180,000 rows -

;; [:gender
;;  :address-id
;;  :country-code
;;  :first-name
;;  :city
;;  :street
;;  :zip-code
;;  :date_of_birth
;;  :email
;;  :ph3
;;  :operator-id
;;  :ph1
;;  :ph2
;;  :last-name]
;; All of them are in string format.


;;If you've already generated your stuff...
;;we have files locally we can read.

(def lhs (ds/->dataset "lhs.csv"))
(def rhs (ds/->dataset "rhs.csv"))

(def res (ds/join-by-column "operator-id" lhs rhs))
(io/mapseq->csv! "file://test.tsv"
                 (ds/mapseq-reader res)
                 :separator \tab)
