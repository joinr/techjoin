;;implements a sparse column type, similar to a subvector
;;around tech.libs.tablesaw.tablesaw-column protocol
;;extensions for the tech.tablesaw.columns.Column class.
(ns techjoin.subcolumn
  (:require [tech.v2.datatype.base :as base]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.readers.indexed :as indexed-rdr]
            [tech.ml.protocols.column :as col-proto]
            [tech.ml.utils :refer [column-safe-name]]
            [tech.jna :as jna]
            [tech.libs.tablesaw.tablesaw-column :as tc]
            [tech.v2.datatype.pprint :as dtype-pp])
  (:import [tech.tablesaw.api ShortColumn IntColumn LongColumn
            FloatColumn DoubleColumn StringColumn BooleanColumn
            NumericColumn]
           [tech.tablesaw.selection Selection BitmapBackedSelection ]
           [tech.v2.datatype ObjectReader ObjectWriter ObjectMutable]
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

;;in tablesaw..there's the connotation of missing data. That is, we have indices
;;in say an underlying int arraylist that correspond to the missing data type
;;for ints. They have special operations to detect within a type if the datum
;;encodes a missing value. One way to mess with this to represent a selection as
;;a subset of original indices. And just leave the encoding alone. When used via
;;the .get method, if the value is deemed missing, nil is returned.

;;Another way would be to copy the original indices and mark the non-selected
;;data as missing. I think the set-selection approach is simpler for now, we'll
;;see if it works out. It could be problematic if we have gajillions of ints to
;;hash though, but for now I'm not too worried.

(defn ^BitmapBackedSelection  ->selection [xs]
  (if (instance? BitmapBackedSelection xs)
    xs
    (BitmapBackedSelection. (int-array xs))))

(defn empty-selection? [^Selection s]
  (zero? (.size s)))

(defn ^Selection select-and! [^Selection l ^Selection r]
  (.and l r))

(defn ^Selection select-or! [^Selection l ^Selection r]
  (.or l r))


;; public IntColumn subset(final int[] rows)
;; {
;;  final IntColumn c = this.emptyCopy();
;;  for (final int row : rows)
;;  {c.append(getInt(row));}
;;  return c;
;;   }



;;need an operation to get us from
;;original idxs -> masked idxs
;;[0 1 2 3 4 5] ->
;;[0 1   3   5] -> mask 1 [(nth data 0)  (nth data 1) (nth data 5)]
;;[0     3]     -> mask 2 [(nth mask1 0) (nth mask1 3)]
;;[0]           -> mask 3 [(nth mask2 0)]


;; (sub-map [0 1 2 3 4 5] [0 1 3 5]) =>
;; sm {:origin [0 1 2 3 4 5] :selection [0 1 3 5]}
;; (seq sm) => (map #(.get origin %) (:selection sm))
;; (nth sm idx) => (.get origin (.get (:selection sm idx)) 

(defmacro extend-masked-subset [coltype column-get]
  (extend-protocol ~'techjoin.subcolumn/IMaskedSubset
    ~coltype
    (masked-subset [this# idxs# mask#]
      (let [visibles# (select-and! (->selection idxs#) mask#)
            ~c (.emptyCopy this#)]
        (doseq [idx# visibles#]
          (.append ~c (~column-get ~c (int idx#)))
          )
        ~c))))

(defprotocol ISubMap
  (sub-get  [this  idx]
    "Look up the local idx in sub-map transforming via its
     selection mask")
  (sub-vals [this]
     "Get a sequence of  indirect, relative values for this submap")
  (sub-keys [this]
     "Get a sequence of indirect, relative keys for this submap")
  (sub-mask [this]
     "Returns the Selection object used for masking and indirection.")
  (project  [this idxs]
     "Create a new submap of the same kind, where the local
      indexes idxs serve as the new local mask when looking up values
      into this from the new submap.")
  (invert   [this idxs]
      "Transform indexes into the index space of the parent
       of this by way of the submap's mask."))

(extend-type nil
  ISubMap
  (sub-get  [this  idx] nil)
  (sub-vals [this] nil)
  (sub-keys [this ] nil )
  (sub-mask [this]  nil )
  (project  [this idxs] nil)
  (invert   [this idxs] idxs))

(defn invert-default
  "Given some local indexes (e.g. index->(nth mask idx)),
   look them up in the previous layer given a selection mask."
  [^BitmapBackedSelection mask idxs]
  (map (fn [n]
         (let [n (int n)]
           (if (.contains mask n)
             (.get mask n)
             (throw (ex-info "index out of range!" {:n n})))))
       idxs))


;;going to assume this is a finite map.  So, the indices
;;defined by idxs are a bounded range, rather than an open interval
;;of indices.  That is, we expect projections to be a subset
;;of the existing indices.
(comment
 ;;proof of concept built with a normal clojure hashmap.

  ;;get already taken, I'm lazy.
  (defn lookup [m k]
    (if (map? m) (get m k)
        (sub-get m k)))

(deftype subintmap [original ^BitmapBackedSelection idxs]
  ISubMap
  (sub-get  [this  idx]
    (lookup original (.get idxs (int idx))))
  (sub-vals [this]
    (for [n (range (.size idxs))]
      (lookup this n)))
  (sub-keys [this ]
    (when (pos? (.size idxs))
                (range (.size idxs))))
  (sub-mask [this]   idxs)
  (project  [this new-idxs]
    (subintmap. this  (->selection new-idxs)))
  (invert   [this xs] (invert-default this idxs xs)))
)



;;subcolumn implementation.  This should establish most of
;;the features of the normal tablesaw column, except
;;instead of using .subset which will create a new column
;;force boxing/allocation in some cases, we have
;;a somewhat flyweight representation that can be
;;coerced into sequences of values, selected/projected
;;on to create smaller subcolumn ala subvector,
;;and generally support all of the protocols necessary
;;for columns.

;;So tablesaw already provides a selection object.
(deftype subcolumn [col name ^BitmapBackedSelection idxs]
  ISubMap
  ;;this is somewhat vestigial in the face of
  ;;get-column-value.
  (sub-get  [this  idx]
    (col-proto/get-column-value this idx))
  (sub-vals [this]
    (for [n (range (.size idxs))]
      (col-proto/get-column-value this n)))
  (sub-keys [this ]
    (when (pos? (.size idxs))
      (range (.size idxs))))
  (sub-mask [this]   idxs)
  (project  [this new-idxs]
    (subcolumn. this  name (->selection new-idxs)))
  (invert   [this xs] (invert-default idxs xs))

  col-proto/PIsColumn
  (is-column? [this] true)
  col-proto/PColumn
  (column-name [this]         (or name (col-proto/column-name col)))
  (set-name [this colname]    (subcolumn. col name idxs))
  (supported-stats [this]     tc/available-stats)
  ;;this porbably isn't implemented correctly.
  (metadata [this] {:name     (col-proto/column-name this)
                    :size     (dtype/ecount this)
                    :datatype (base/get-datatype col)})

  (cache [this] {})
  ;;NOTE: this is a very bulky operation.  We could do better.
  (missing [this]
     (-> (col-proto/missing col)
         (->selection)
         (select-and! idxs)
         (.toArray)))

  (unique [this]
     (into #{} (map (fn [idx] (col-proto/get-column-value this idx))) idxs))

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
    (col-proto/get-column-value col (.get idxs (int idx))))
  ;;this isn't great, since it's not picking up the type information
  ;;and returning a boxed array.  placeholder for now.
  (column-values [this]
    (when-not (= 0 (.size idxs))
      (into-array (sub-vals this))
      #_(or (dtype-proto/->array this)
          (dtype-proto/->array-copy this))))

  ;;NOTE:
  ;;Original implementation of this is suspect; I think tablesaw
  ;;provides a direct lookup for isMissing with a 2 arity version
  ;;that can check entries...
  (is-missing? [this idx]
      (when (.contains idxs idx)
         (col-proto/is-missing? col (.get idxs idx)))
    #_(-> (.isMissing col)
          (.contains (int idx))))

  ;;Now we just invert the indexes onto the old (possibly another
  ;;subcolumn, or somewhere at bottom, the originating column data.)
  ;;This should be significantly faster than creating and boxing via subset.
  (select [this idx-seq]
    (col-proto/select col (invert this idx-seq)))

  (empty-column [this datatype elem-count metadata]
    (col-proto/empty-column col datatype elem-count metadata))

  (new-column [this datatype elem-count-or-values metadata]
    (col-proto/new-column col datatype elem-count-or-values metadata))

  (clone [this]
     (subcolumn. col name (-> idxs seq ->selection)))

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
    (double-array (sub-vals this)))
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
  (->reader [this options]
    (#'tech.libs.tablesaw.tablesaw-column/column-reader->reader
     col
     (reify ObjectReader
       (getDatatype [reader] (dtype-proto/get-datatype col))
       (lsize [reader] (base/ecount this))
       (read [reader idx] (col-proto/get-column-value this idx) #_(.get item idx)))
     options))


  dtype-proto/PBuffer
  (sub-buffer [item offset len]
    (throw (ex-info "not-implemented" {:fn :sub-buffer}))
    #_(let [offset (long offset)
          len    (long len)]
      (reify
        ObjectReader
        (getDatatype [reader] (dtype-proto/get-datatype item))
        (lsize [reader] len)
        (read [reader idx] (.get item (+ idx offset)))
        ObjectWriter
        (write [writer idx value]
          (.set item (+ idx offset) value)))))


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
  (->array-copy [this] (col-proto/column-values col))

  dtype-proto/PCountable
  (ecount [this]
    (.size idxs))

  ObjectReader
  (lsize [this] (long (dtype-proto/ecount this)))
  (read [this idx]
    (col-proto/get-column-value this idx))

  Object
  (toString [this]
    (let [n-items (dtype/ecount this)
          format-str (if (> n-items 20)
                       "#subcolumn<%s>%s\n%s\n[%s...]"
                       "#subcolumn<%s>%s\n%s\n[%s]")]
      (format format-str
              (name (dtype/get-datatype col))
              [n-items]
              (col-proto/column-name this)
              (-> (dtype/->reader this)
                  (dtype-proto/sub-buffer 0 (min 20 n-items))
                  (dtype-pp/print-reader-data)))))
  )


(defmethod print-method subcolumn
  [col ^java.io.Writer w]
  (.write w (.toString ^Object col)))


(defn subset
  ([c name idxs]
   (subcolumn. c name (->selection idxs)))
  ([c idxs] (subset c (keyword (str (name (col-proto/column-name c)) "_" (gensym "subcolumn"))) idxs)))
;;testing
(require '[tech.ml.dataset :as ds])

(def the-data
  (ds/->dataset
   (for [[i k] (map-indexed vector (seq "abcdefghijklmnopqrstuvwzyz"))]
     {:idx i :letter (str k) :square (* i 2)})))