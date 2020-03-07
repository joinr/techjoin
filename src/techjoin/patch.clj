(ns techjoin.patch
  (:require [tech.libs.tablesaw]))

;;monkey patching.
(in-ns 'tech.libs.tablesaw)


(deftype TablesawColumn [^Column col metadata cache]
  col-proto/PIsColumn
  (is-column? [this] true)

  col-proto/PColumn
  (column-name [this] (or (:name metadata) (.name col)))
  (set-name [this colname]
    (TablesawColumn. col (assoc metadata :name colname) {}))

  (supported-stats [this] (col-proto/supported-stats col))

  (metadata [this] (merge metadata
                          {:name (col-proto/column-name this)
                           :size (dtype/ecount col)
                           :datatype (dtype/get-datatype col)}))

  (set-metadata [this data-map]
    (TablesawColumn. col data-map cache))

  (cache [this] cache)

  (set-cache [this cache-map]
    (TablesawColumn. col metadata cache-map))

  (missing [this] (col-proto/missing col))

  (unique [this] (col-proto/unique col))

  (stats [this stats-set]
    (when-not (instance? NumericColumn col)
      (throw (ex-info "Stats aren't available on non-numeric columns"
                      {:column-type (dtype/get-datatype col)
                       :column-name (col-proto/column-name this)
                       :column-java-type (type col)})))
    (let [stats-set (set (if-not (seq stats-set)
                           dtype-tbl/available-stats
                           stats-set))
          existing (->> stats-set
                        (map (fn [skey]
                               (when-let [cached (get metadata skey)]
                                 [skey cached])))
                        (remove nil?)
                        (into {}))
          missing-stats (c-set/difference stats-set (set (keys existing)))]
      (merge existing
             (col-proto/stats col missing-stats))))

  (correlation [this other-column correlation-type]
    (col-proto/correlation col
                           (.col ^TablesawColumn other-column)
                           correlation-type))
  ;;Patched in.
  (get-column-value [this idx] (col-proto/get-column-value col idx))
  (column-values [this] (col-proto/column-values col))

  (is-missing? [this idx] (col-proto/is-missing? col idx))

  (select [this idx-seq] (make-column (col-proto/select col idx-seq) metadata {}))

  (empty-column [this datatype elem-count metadata]
    (dtype-proto/make-container :tablesaw-column datatype elem-count
                                (assoc (select-keys metadata [:name])
                                       :empty? true)))

  (new-column [this datatype elem-count-or-values metadata]
    (dtype-proto/make-container :tablesaw-column datatype
                                elem-count-or-values metadata))

  (clone [this]
    (dtype-proto/make-container :tablesaw-column
                                (dtype/get-datatype this)
                                (col-proto/column-values this)
                                metadata))

  (to-double-array [this error-missing?]
    (col-proto/to-double-array col error-missing?))

  dtype-proto/PDatatype
  (get-datatype [this] (dtype-base/get-datatype col))


  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (dtype-proto/copy-raw->item! col ary-target target-offset options))

  dtype-proto/PPrototype
  (from-prototype [src datatype shape]
    (col-proto/new-column src datatype (first shape) (select-keys [:name] metadata)))


  dtype-proto/PToNioBuffer
  (convertible-to-nio-buffer? [item]
    (dtype-proto/convertible-to-nio-buffer? col))
  (->buffer-backing-store [item]
    (dtype-proto/as-nio-buffer col))


  dtype-proto/PToList
  (convertible-to-fastutil-list? [item]
    (dtype-proto/convertible-to-fastutil-list? col))
  (->list-backing-store [item]
    (dtype-proto/as-list col))


  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item options]
    (dtype-proto/->reader col options))


  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [item options]
    (dtype-proto/->writer col options))


  dtype-proto/PToIterable
  (convertible-to-iterable? [item] true)
  (->iterable [item options]
    (dtype-proto/->reader col options))


  dtype-proto/PToMutable
  (convertible-to-mutable? [item] true)
  (->mutable [item options]
    (dtype-proto/->mutable col options))

  dtype-proto/PBuffer
  (sub-buffer [item offset length]
    (dtype-proto/sub-buffer col offset length))

  dtype-proto/PToArray
  (->sub-array [src] (dtype-proto/->sub-array col))
  (->array-copy [src] (dtype-proto/->array-copy col))

  dtype-proto/PCountable
  (ecount [item] (dtype-proto/ecount col))

  ObjectReader
  (lsize [item] (long (dtype-proto/ecount col)))
  (read [item idx]
    (.get col idx))

  Object
  (toString [item]
    (let [n-items (dtype/ecount item)
          format-str (if (> n-items 20)
                       "#tablesaw-column<%s>%s\n%s\n[%s...]"
                       "#tablesaw-column<%s>%s\n%s\n[%s]")]
      (format format-str
              (name (dtype/get-datatype item))
              [n-items]
              (col-proto/column-name item)
              (-> (dtype/->reader item)
                  (dtype-proto/sub-buffer 0 (min 20 n-items))
                  (dtype-pp/print-reader-data))))))


#_(defmethod print-method TablesawColumn
  [col ^java.io.Writer w]
  (.write w (.toString ^Object col)))


(defn make-column
  [datatype-col metadata & [cache]]
  (if (instance? TablesawColumn datatype-col)
    (throw (ex-info "Nested" {})))
  (TablesawColumn. datatype-col metadata cache))


(in-ns 'tech.libs.tablesaw.tablesaw-column)



(extend-type Column
  col-proto/PIsColumn
  (is-column? [this] true)
  col-proto/PColumn
  (column-name [col] (.name col))
  (supported-stats [col] available-stats)
  (metadata [col] {:name (col-proto/column-name col)
                   :size (dtype/ecount col)
                   :datatype (base/get-datatype col)})

  (cache [this] {})
  (missing [col]
    (-> (.isMissing ^Column col)
        (.toArray)))
  (unique [col]
    (-> (.unique col)
        (.asList)
        set))

  (stats [col stats-set]
    (when-not (instance? NumericColumn col)
      (throw (ex-info "Stats aren't available on non-numeric columns"
                      {:column-type (base/get-datatype col)
                       :column-name (col-proto/column-name col)
                       :column-java-type (type col)})))
    (let [stats-set (set (if-not (seq stats-set)
                           available-stats
                           stats-set))
          missing-stats stats-set
          ^NumericColumn col col]
      (->> missing-stats
           (map (fn [skey]
                  [skey
                   (case skey
                     :mean (.mean col)
                     :variance (.variance col)
                     :median (.median col)
                     :min (.min col)
                     :max (.max col)
                     :skew (.skewness col)
                     :kurtosis (.kurtosis col)
                     :geometric-mean (.geometricMean col)
                     :sum-of-squares (.sumOfSquares col)
                     :sum-of-logs (.sumOfLogs col)
                     :quadratic-mean (.quadraticMean col)
                     :standard-deviation (.standardDeviation col)
                     :population-variance (.populationVariance col)
                     :sum (.sum col)
                     :product (.product col)
                     :quartile-1 (.quartile1 col)
                     :quartile-3 (.quartile3 col)
                     )]))
           (into {}))))

  (correlation
    [col other-column correlation-type]
    (let [^NumericColumn column (jna/ensure-type NumericColumn col)
          ^NumericColumn other-column (jna/ensure-type NumericColumn other-column)]
      (case correlation-type
        :pearson (.pearsons column other-column)
        :spearman (.spearmans column other-column)
        :kendall (.kendalls column other-column))))

  ;;patched
  (get-column-value [this idx]
    (.get this idx))
  (column-values [this]
    (when-not (= 0 (base/ecount this))
      (or (dtype-proto/->array this)
          (dtype-proto/->array-copy this))))

  (is-missing? [col idx]
    (-> (.isMissing col)
        (.contains (int idx))))

  (select [col idx-seq]
    (let [^ints int-data (if (instance? (Class/forName "[I") idx-seq)
                           idx-seq
                           (dtype/make-container :java-array :int32 idx-seq))]
      ;;We can't cache much metadata now as we don't really know.
      (.subset col int-data)))


  (empty-column [this datatype elem-count metadata]
    (dtype-proto/make-container :tablesaw-column datatype
                                elem-count (merge metadata
                                                  {:empty? true})))


  (new-column [this datatype elem-count-or-values metadata]
    (dtype-proto/make-container :tablesaw-column datatype
                                elem-count-or-values metadata))


  (clone [this]
    (dtype-proto/make-container :tablesaw-column
                                (base/get-datatype this) (col-proto/column-values this)
                                {:name (col-proto/column-name this)}))


  (to-double-array [col error-missing?]
    (when (and error-missing?
               (> (.countMissing col) 0))
      (throw (ex-info (format "Missing values detected: %s - %s"
                              (col-proto/column-name col)
                              (.countMissing col)))))
    (base/copy! col (double-array (base/ecount col))))

  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (base/raw-dtype-copy! (dtype/->reader raw-data)
                          ary-target
                          target-offset options))


  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item options]
    (column-reader->reader
     item
     (reify ObjectReader
       (getDatatype [reader] (dtype-proto/get-datatype item))
       (lsize [reader] (base/ecount item))
       (read [reader idx] (.get item idx)))
     options))


  dtype-proto/PBuffer
  (sub-buffer [item offset len]
    (let [offset (long offset)
          len (long len)]
      (reify
        ObjectReader
        (getDatatype [reader] (dtype-proto/get-datatype item))
        (lsize [reader] len)
        (read [reader idx] (.get item (+ idx offset)))
        ObjectWriter
        (write [writer idx value]
          (.set item (+ idx offset) value)))))


  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [col options]
    (-> (reify ObjectWriter
          (getDatatype [writer] (dtype-proto/get-datatype col))
          (lsize [writer] (base/ecount col))
          (write [writer idx value]
            (locking writer
              (.set col idx value))))
        (dtype-proto/->writer options)))


  dtype-proto/PToMutable
  (convertible-to-mutable? [item] true)
  (->mutable [col options]
    (-> (reify ObjectMutable
          (getDatatype [mut] (dtype-proto/get-datatype col))
          (lsize [mut] (base/ecount col))
          (insert [mut idx value]
            (when-not (= idx (.lsize mut))
              (throw (ex-info "Only insertion at the end of a column is supported." {})))
            (.append col value)))
        (dtype-proto/->mutable options)))


  dtype-proto/PPrototype
  (from-prototype [col datatype shape]
    (when-not (= 1 (count shape))
      (throw (ex-info "Base containers cannot have complex shapes"
                      {:shape shape})))
    (dtype-proto/make-container :tablesaw-column
                                datatype
                                (base/shape->ecount shape)
                                {:name (.name col)}))

  dtype-proto/PToArray
  (->sub-array [col] nil)
  (->array-copy [col] (.asObjectArray col))

  dtype-proto/PCountable
  (ecount [col]
    (.size col)))

(in-ns 'techjoin.patch)
