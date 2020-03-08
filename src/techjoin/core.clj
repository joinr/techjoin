(ns techjoin.core
  (:require [spork.util [table :as tbl]]
            [tech.ml.dataset :as ds]
            [tech.io :as io]
            [techjoin.derivedcolumn :as derived]
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
    (let [city   (str (rand-int 10))]
      {:address    (str "Address" i)
       :gender     (rand-nth ["m" "f" "n"])
       :address-id i
       :country-code "99"
       :first-name (str "customer_" i "first")
       :last-name  (str "customer_" i "last")
       :city       city
       :zip-code   (clojure.string/join (repeat 5 city))
       :email      (str "customer_" i "@the-net")
       :huge-field (str "this is a huge field containing a lot of dumb info for
       bloat which will make the file so much larger for our poor machine how
       unkind of us to do so in this day and age" i)})))

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
    {:size           (rand-nth ["s" "m" "l"])
     :day            (str (rand-int 100000))
     :operator-id    (str (rand-int 10000))
     :notes          "THis is some bloated information we'll add in"
     :more-notes     "to make the table larger"
     :even-more-notes "Also this will make things big as well"
     :how-can-there-be-more "Yet another text field will add overhead jabroni"}))

(defn random-rhs []
  (let [cs  (vec (customers))]
    (for [i (range 180000)]
      (let [c (rand-nth cs)]
        (assoc c :operator-id (rand-int 10000))))))

(defn spit-tables []
  (tbl/records->file (random-lhs) "lhs.csv" :sep ","
      :field-order [:size :day :operator-id :notes :more-notes :even-more-notes :how-can-there-be-more])
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

;;fast 214ms
(def lhs (ds/->dataset "lhs.csv"))
;;fast 736ms
(def rhs (ds/->dataset "rhs.csv"))

;;this is fast and handled quickly (makes sense since I think we are computing
;;the join solely in index space) (about 2s)
(def res (ds/join-by-column "operator-id" lhs rhs))
;;testing with new derivedcolumn.
(def res2 (derived/join-by-column "operator-id" lhs rhs))

;;slow (~131s on my hardware)
;;I think we are realizing the joined rows as we go,
;;and we are creating a lot of intermediate vectors and doing
;;a lot of hashmap lookups in data.csv (according to profiling).
;;I'm guessing these aren't necessary.
(io/mapseq->csv! "file://test.tsv"
                 (ds/mapseq-reader res)
                 :separator \tab)

;;reducer-based record traversal is ~4-5x faster than
;;default data.csv based variant above.
(tbl/records->file (ds/mapseq-reader res)  "test.tsv" :sep \tab)

(comment
  ;;curious to see how this works out.
  (def lhs (tbl/tabdelimited->table "lhs.csv"))
  (def rhs (tbl/tabdelimited->table "rhs.csv"))
  ;;legacy implementation will blow up on this dataset,
  ;;we inefficiently build the whole thing, and do it using
  ;;`merge` which is slow and allocation heavy.  Punishes the heap, don't run.
  (def res (tbl/join-on [:operator-id] lhs rhs))
  )
