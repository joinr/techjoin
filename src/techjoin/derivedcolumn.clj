;;implements a sparse column type, similar to a subvector
;;around tech.libs.tablesaw.tablesaw-column protocol
;;extensions for the tech.tablesaw.columns.Column class.
(ns techjoin.derivedcolumn
  (:require [tech.v2.datatype.base :as base]
            [tech.v2.datatype.typecast :as typecast]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.readers.indexed :as indexed-rdr]
            [tech.ml.protocols.column :as col-proto]
            [tech.ml.utils :refer [column-safe-name]]
            [tech.jna :as jna]
            [tech.ml.dataset :as ds]
            [tech.libs.tablesaw.tablesaw-column :as tc]
            [tech.v2.datatype.pprint :as dtype-pp]
            ;;temporary for get-column-value
            [techjoin.patch])
  (:import [tech.tablesaw.api ShortColumn IntColumn LongColumn
            FloatColumn DoubleColumn StringColumn BooleanColumn
            NumericColumn]
           [tech.tablesaw.selection Selection BitmapBackedSelection ]
           [tech.v2.datatype ObjectReader ObjectWriter ObjectMutable ByteReader
            ShortReader IntReader LongReader FloatReader DoubleReader
            BooleanReader]
           [tech.tablesaw.columns Column]
           [it.unimi.dsi.fastutil.shorts ShortArrayList]
           [it.unimi.dsi.fastutil.ints IntArrayList]
           [it.unimi.dsi.fastutil.longs LongArrayList]
           [it.unimi.dsi.fastutil.floats FloatArrayList]
           [it.unimi.dsi.fastutil.doubles DoubleArrayList]
           [it.unimi.dsi.fastutil.booleans BooleanArrayList]
           [it.unimi.dsi.fastutil.objects ObjectArrayList]
           [java.nio ByteBuffer ShortBuffer IntBuffer LongBuffer
            FloatBuffer DoubleBuffer Buffer]
           [java.lang.reflect Field]
           [it.unimi.dsi.fastutil.ints IntLinkedOpenHashSet]))

;;working around my limitations of expressing this
;;in tech.datatype system...
(defmacro typed-read [readertype]
  (let [reader (with-meta (gensym "reader") `{:tag ~readertype})]
    `(fn [~reader idx#] (.read ~reader idx#))))
;;janky hack.
(defn read-fn [data-type]
  (case data-type
    :object  (typed-read ObjectReader)
    :int8    (typed-read ByteReader)
    :int16   (typed-read ShortReader)
    :int32   (typed-read IntReader)
    :int64   (typed-read LongReader)
    :float32 (typed-read FloatReader)
    :float64 (typed-read DoubleReader)
    :boolean (typed-read BooleanReader)
    :string  (typed-read ObjectReader)))

(defn derived-reader [col size options ^LongArrayList idxs]
  (let [dtype (dtype-proto/get-datatype col)]
    (let [rdr   (dtype-proto/->reader col options)
          read! (read-fn dtype)]
      (reify ObjectReader
        (getDatatype [reader]      dtype)
        (lsize       [reader]      size)
        (read        [reader idx]  (read! rdr (.get idxs (long idx))))))))

(deftype derivedcolumn [col name ^LongArrayList idxs]
  col-proto/PIsColumn
  (is-column? [this] true)
  col-proto/PColumn
  (column-name [this]         (or name (col-proto/column-name col)))
  (set-name [this colname]    (derivedcolumn. col name idxs))
  (supported-stats [this]     tc/available-stats)
  ;;this porbably isn't implemented correctly.
  (metadata [this] {:name     (col-proto/column-name this)
                    :size     (dtype/ecount this)
                    :datatype (base/get-datatype col)})

  (cache [this] {})
  ;;NOTE: this is a very bulky operation.  We could do better.
  (missing [this]
    (into (vector-of :long)
          (filter (fn [idx]
                    (col-proto/is-missing? this idx)))
          (range (.size idxs))))

  (is-missing? [this idx]
    (col-proto/is-missing? col (.get idxs (long idx))))

  (unique [this]
     (into #{} (col-proto/column-values this)))

  ;;TBD, we can't just simply delegate; have to pass on the indices.
  ;;Kind of a weakness of relying on tablesaw column here, since all the
  ;;methods are column-local.  Maybe having a true Column implementation
  ;;that does our subcolumn stuff would be useful.
  (stats [col stats-set]
     (throw (ex-info "Stats not currently enabled for subcolumns!" {}))
    #_(col-proto/stats col stats-set))

  (correlation
   [col other-column correlation-type]
   (throw (ex-info "Correlation not currently enabled for subcolumns!" {}))
    #_(let [^NumericColumn column (jna/ensure-type NumericColumn col)
          ^NumericColumn other-column (jna/ensure-type NumericColumn other-column)]
      (case correlation-type
        :pearson (.pearsons column other-column)
        :spearman (.spearmans column other-column)
        :kendall (.kendalls column other-column))))

  (get-column-value [this idx]
    (col-proto/get-column-value col (.get idxs (long idx))))

  ;;this isn't great, since it's not picking up the type information
  ;;and returning a boxed array.  placeholder for now.  going lazy
  ;;intentionally, since we want to avoid big eager allocations...
  (column-values [this]
     (when-not (= 0 (.size idxs))
       (->> idxs
            (map (fn [idx] (col-proto/get-column-value col (.get idxs (long idx))))))))

  ;;Now we just invert the indexes onto the old (possibly another
  ;;subcolumn, or somewhere at bottom, the originating column data.)
  ;;This should be significantly faster than creating and boxing via subset.
  (select [this idx-seq]
    (col-proto/select col (map (fn [idx] (.get idxs (long idx))) idx-seq)))

  (empty-column [this datatype elem-count metadata]
    (col-proto/empty-column col datatype elem-count metadata))

  (new-column [this datatype elem-count-or-values metadata]
    (col-proto/new-column col datatype elem-count-or-values metadata))

  ;;NOTE:
  ;;dunno if we should keep reference semantics for clone
  (clone [this]
     (derivedcolumn. col name idxs))

  ;;NOTE: this is a janky rewrite using the bulky missing?
  ;;Ideally we'd have a missing count or something like
  ;;Like the built-in .countMissing in tablesaw (likely O(1))
  (to-double-array [this error-missing?]
    (when error-missing?
      (let [total (count (col-proto/missing col))] ;;hack.
        (when (pos? total)
          (throw (ex-info (format "Missing values detected: %s - %s"
                                  (col-proto/column-name col)
                                  total))))))
    (double-array (col-proto/column-values this)))
  dtype-proto/PDatatype
  (get-datatype [this] (base/get-datatype col))

  dtype-proto/PToList
  (convertible-to-fastutil-list? [item]
    (dtype-proto/convertible-to-fastutil-list? col))

  (->list-backing-store [item]
    (dtype-proto/as-list col))

  dtype-proto/PToIterable
  (convertible-to-iterable? [item] true)

  (->iterable [item options]
    (dtype-proto/->reader col options))

  dtype-proto/PCountable
  (ecount [item] (.size idxs))

  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (throw (ex-info "not-implemented" {:fn :copy-raw->item!}))
    #_(base/raw-dtype-copy! (dtype/->reader raw-data)
                          ary-target
                          target-offset options))


  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  ;;dunno if this is right.
  (->reader [this options]
    (derived-reader col (base/ecount this) options idxs))


  dtype-proto/PBuffer
  (sub-buffer [this offset len]
    (let [offset (long offset)
          len    (long len)
          ^ObjectReader  rdr (dtype-proto/->reader col {})]
      (reify
        ObjectReader
        (getDatatype [reader] (dtype-proto/get-datatype this))
        (lsize [reader] len)
        (read [reader idx] (.read rdr (+ idx offset)))
        ObjectWriter
        (write [writer idx value]
          (throw (ex-info "not-implemented" {:fn :->write})))
          #_(.set this (+ idx offset) value))))


  dtype-proto/PToWriter
  (convertible-to-writer? [item] false #_true)
  (->writer [this options]
    (throw (ex-info "not-implemented" {:fn :->writer}))
    #_(-> (reify ObjectWriter
          (getDatatype [writer] (dtype-proto/get-datatype col))
          (lsize [writer] (base/ecount col))
          (write [writer idx value]
            (locking writer
              (.set col idx value))))
        (dtype-proto/->writer options)))

  ;;Need to think about this...
  dtype-proto/PToMutable
  (convertible-to-mutable? [item] false #_true)
  (->mutable [this options]
      (throw (ex-info "not-implemented" {:fn :->mutable}))
    #_(-> (reify ObjectMutable
          (getDatatype [mut] (dtype-proto/get-datatype col))
          (lsize [mut] (base/ecount col))
          (insert [mut idx value]
            (when-not (= idx (.lsize mut))
              (throw (ex-info "Only insertion at the end of a column is supported." {})))
            (.append col value)))
        (dtype-proto/->mutable options)))


  dtype-proto/PPrototype
  (from-prototype [this datatype shape]
    (dtype-proto/from-prototype col datatype shape))

  dtype-proto/PToArray
  (->sub-array [this] nil)
  (->array-copy [this] (to-array (col-proto/column-values col)))

  dtype-proto/PCountable
  (ecount [this]
    (.size idxs))

  ObjectReader
  (lsize [this] (long (dtype-proto/ecount this)))
  (read  [this idx]
    (col-proto/get-column-value this idx))

  Object
  (toString [this]
    (let [n-items (dtype/ecount this)
          format-str (if (> n-items 20)
                       "#derivedcolumn<%s>%s\n%s\n[%s...]"
                       "#derivedcolumn<%s>%s\n%s\n[%s]")]
      (format format-str
              (name (dtype/get-datatype col))
              [n-items]
              (col-proto/column-name this)
              (-> (dtype/->reader this)
                  (dtype-proto/sub-buffer 0 (min 20 n-items))
                  (dtype-pp/print-reader-data)))))
  )

(defmethod print-method derivedcolumn
  [col ^java.io.Writer w]
  (.write w (.toString ^Object col)))

;;replacement for the default .subset operation on columns.
;;this will provide a selection operation that allows defining
;;new derived columns from input column c, and a seq of indexes.
;;The operation is identical to select, except we don't allocate
;;the new column, only a flyweight derivedcolumn object and
;;an arraylist of the indices referenced from the progenitor.
;;This can nest arbitrarily (e.g. derive of derive of derive),
;;providing slice-like operations ad nauseum, at the small potential
;;of causing a memory leak due to maintaining references to the original
;;underlying column.  Could look into optimizing this with some
;;heuristics, but meh.
(defn subset
  ([c name idxs]
   (derivedcolumn. c name (doto (LongArrayList.) (.addAll ^java.util.List (seq idxs)))))
  ([c idxs] (subset c (keyword (str (name (col-proto/column-name c)) "_" (gensym "subcolumn"))) idxs)))


;;Identical to join-by-column in the base library, but leverages derived columns
;;and operates in index space as much as possible.  This should allow better performance
;;on giant datasets where the joins produce millions of rows, since we don't need to
;;realize the rows until querying them (on demand) e.g. when spitting to files.
(defn join-by-column
  "Join by column.  For efficiency, lhs should be smaller than rhs as it is sorted
  in memory. Uses subcolumns to operate in index space without building intermediate
  columns."
  [colname lhs rhs & {:keys [error-on-missing?]}]
  (let [idx-groups (ds/group-by-column->indexes colname lhs)
        rhs-col (typecast/datatype->reader :object (rhs colname))
        lhs-indexes (LongArrayList.)
        rhs-indexes (LongArrayList.)
        n-elems (dtype/ecount rhs-col)]
    ;;This would have to be parallelized and thus have a separate set of indexes
    ;;that were merged later in order to really be nice.  tech.datatype has no
    ;;parallized operation that will result in this.
    (loop [idx 0]
      (when (< idx n-elems)
        (if-let [^LongArrayList item (get idx-groups (.read rhs-col idx))]
          (do
            (dotimes [n-iters (.size item)] (.add rhs-indexes idx))
            (.addAll lhs-indexes item))
          (when error-on-missing?
            (throw (Exception. (format "Failed to find column value %s"
                                       (.read rhs-col idx))))))
        (recur (unchecked-inc idx))))
    (let [lhs-cols (->> (ds/columns lhs)
                        (map #(subset % lhs-indexes)))
          rhs-cols (->> (ds/columns (ds/remove-column rhs colname))
                        (map #(subset % rhs-indexes)))]
      (tech.ml.dataset.base/from-prototype lhs "join-table" (concat lhs-cols rhs-cols)))))


;;testing
(comment 
(require '[tech.ml.dataset :as ds])

(def lhs
  (ds/->dataset
   (for [[i k] (map-indexed vector (seq "abcdefghijklmnopqrstuvwxyz"))]
     {:idx i :letter (str k) :square (* i 2)})))

(def rhs
  (ds/->dataset
   (for [[idx c] (map-indexed vector (take 52 (cycle (seq "abcdefghijklmnopqrstuvwxyz"))))]
     {:ridx idx
      :letter (str c)
      :capitalized (clojure.string/upper-case (str c))
      :doubled    (str c c)})))

(def c1 (ds/column lhs :idx))
;; #tablesaw-column<int16>[26]
;; :idx
;; [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, ...]
(def sub2 (subset c1 (filter even? (range 20))))
;; #derivedcolumn<null>[10]
;; :idx_derivedcolumn68801
;; [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, ]
(def sub3 (subset c1 (filter odd? (range 20))))
;; #derivedcolumn<null>[10]
;; :idx_derivedcolumn68802
;; [1, 3, 5, 7, 9, 11, 13, 15, 17, 19, ]
;;(join-by-column :letter lhs rhs)  ;;should get 52...
;; techjoin.derivedcolumn> (join-by-column :letter lhs rhs)
;; join-table [52 6]:

;; | :idx | :letter | :square | :ridx | :capitalized | :doubled |

;; |------+---------+---------+-------+--------------+----------|

;; |    0 |       a |       0 |     0 |            A |       aa |

;; |    1 |       b |       2 |     1 |            B |       bb |

;; |    2 |       c |       4 |     2 |            C |       cc |

;; |    3 |       d |       6 |     3 |            D |       dd |

;; |    4 |       e |       8 |     4 |            E |       ee |

;; |    5 |       f |      10 |     5 |            F |       ff |

;; |    6 |       g |      12 |     6 |            G |       gg |

;; |    7 |       h |      14 |     7 |            H |       hh |

;; |    8 |       i |      16 |     8 |            I |       ii |

;; |    9 |       j |      18 |     9 |            J |       jj |

;; |   10 |       k |      20 |    10 |            K |       kk |

;; |   11 |       l |      22 |    11 |            L |       ll |

;; |   12 |       m |      24 |    12 |            M |       mm |

;; |   13 |       n |      26 |    13 |            N |       nn |

;; |   14 |       o |      28 |    14 |            O |       oo |

;; |   15 |       p |      30 |    15 |            P |       pp |

;; |   16 |       q |      32 |    16 |            Q |       qq |

;; |   17 |       r |      34 |    17 |            R |       rr |

;; |   18 |       s |      36 |    18 |            S |       ss |

;; |   19 |       t |      38 |    19 |            T |       tt |

;; |   20 |       u |      40 |    20 |            U |       uu |

;; |   21 |       v |      42 |    21 |            V |       vv |

;; |   22 |       w |      44 |    22 |            W |       ww |

;; |   23 |       x |      46 |    23 |            X |       xx |

;; |   24 |       y |      48 |    24 |            Y |       yy |
)
