(ns bitfinex-clj.public.books
  (:require [clojure.spec.alpha :as spec]
            [clojure.core.async :as async :refer [go >! pipe chan sub alts! sliding-buffer]]
            [bitfinex-clj.core :as core]
            [bitfinex-clj.client :as client]))

;; Utils
(defn is-raw-book? [opts] (= (:prec opts) "R0"))

(def BOOKS_DEFAULT_PREC "P0")
(def BOOKS_DEFAULT_FREQ "F0")
(def BOOKS_DEFAULT_LEN "25")

; Default frequency
(def sub-defaults {:prec BOOKS_DEFAULT_PREC
                   :len BOOKS_DEFAULT_LEN
                   :freq BOOKS_DEFAULT_FREQ})

(def sub-raw-defaults (dissoc sub-defaults :freq))

; options spec
(spec/def ::options (spec/keys :req-un [::core/symbol]
                               :opt-un [::prec ::len ::freq]))

;; Data sepc
(spec/def ::price number?)
(spec/def ::count integer?)
(spec/def ::amount number?)
(spec/def ::rate number?)
(spec/def ::period number?)
(spec/def ::order-id number?)
(spec/def ::offer-id number?)


(core/defentity book-trading [::price ::count ::amount])
(core/defentity book-funding [::rate ::period ::count ::amount])
(core/defentity raw-book-trading [::order-id ::price ::amount])
(core/defentity raw-book-funding [::offer-id ::period ::rate ::amount])

(spec/def ::any-book (spec/or :trading ::book-trading
                              :funding ::book-funding
                              :raw-trading ::raw-book-trading
                              :raw-funding ::raw-book-funding))

(spec/def ::book-update (spec/cat :chan-id ::core/chan-id :book (spec/spec ::any-book)))
(spec/def ::book-snapshot (spec/cat :chan-id ::core/chan-id :books (spec/coll-of ::any-book)))

(defn get->book
  [opts]
  (let [s [(is-raw-book? opts) (core/is-trading? (:symbol opts))]]
    (case s
      [true true] ->raw-book-trading
      [true false] ->raw-book-funding
      [false true] ->book-trading
      [false false] ->book-funding)))

(defn ->book-subscription
  [opts]
  (let [props (select-keys opts [:symbol :prec :freq :len])
        new-opts (if (is-raw-book? props)
                   (merge sub-raw-defaults props)
                   (merge sub-defaults props))]

    (core/->subscription "book" new-opts)))

;; Subscribe API
(defn subscribe!
  [{:keys [queue]} opts]
  {:pre [(spec/assert ::options opts)]}
  (go (>! queue [:subscribe (->book-subscription opts)])))

(defn unsubscribe!
  [{:keys [queue]} opts]
  {:pre [(spec/assert ::options opts)]}
  (go (>! queue [:unsubscribe (->book-subscription opts)])))

;; Subscriptions
(defn sub-update
  ([client opts ch] (sub-update client opts ch true))
  ([client opts ch close?]
   (let [t (->book-subscription opts)
         ->book (get->book opts)
         c (chan (sliding-buffer 1)
                 (comp (map second)
                       (core/filter-spec ::book-update)
                       (map core/public-msg->data)
                       (map ->book)))]

     (core/sub-pipe (:publication client) t c ch close?))))

(defn sub-snapshot
  ([client opts ch] (sub-snapshot client opts ch true))
  ([client opts ch close?]
   (let [t (->book-subscription opts)
         ->book (get->book opts)
         c (chan (sliding-buffer 1)
                 (comp (map second)
                       (core/filter-spec ::book-snapshot)
                       (map core/public-msg->data)
                       (map (partial core/->entity-snapshot ->book))))]

     (core/sub-pipe (:publication client) t c ch close?))))


(def aux-snapshot
  '(27059 [[6580 48 67.27908305] [6570 51 124.99574515] [6560 41 154.90551369] [6550 54 149.87960808] [6540 52 406.45217696] [6530 104 153.70814539] [6520 66 109.4108712] [6510 66 150.53668782] [6500 105 247.41414437] [6490 57 35.43415941] [6480 66 182.49940037] [6470 72 107.56248913] [6460 66 403.92531722] [6450 88 191.46265441] [6440 56 154.43647746] [6430 60 225.84285189] [6420 52 118.63100141] [6410 47 49.42329825] [6400 87 133.35720462] [6390 28 71.76865805] [6380 42 112.55308265] [6370 28 32.75928028] [6360 24 2.78521257] [6350 53 131.74846537] [6340 31 48.27785822] [6590 13 -20.20531872] [6600 54 -52.0275073] [6610 63 -104.61371866] [6620 57 -140.87195365] [6630 33 -81.86307417] [6640 44 -145.46409588] [6650 27 -95.92580415] [6660 51 -176.77567036] [6670 44 -58.0536242] [6680 45 -82.34972134] [6690 52 -88.72487203] [6700 114 -173.08932757] [6710 42 -31.22937484] [6720 47 -146.87596653] [6730 56 -165.12459423] [6740 57 -67.04889605] [6750 80 -157.31506975] [6760 53 -141.21989078] [6770 42 -86.18857434] [6780 58 -83.09492905] [6790 66 -92.53267961] [6800 118 -170.38388882] [6810 42 -89.20645759] [6820 49 -146.83502048] [6830 34 -22.41020626]]))

(defn split-books
  [coll]
  (split-with #(> (:amount %) 0) coll))

(def comparators {:bids > :asks <})

(defn aggregated-snapshot-xf
  [->entity]
  (fn [xf]
    (let [aggr (volatile! {:bids [] :asks []})]
      (fn ([] (xf))
          ([result] (xf result))
          ([result input]
           (cond
             (core/is-valid? ::book-snapshot input)
             (let [[bids asks] (->> input
                                    (core/public-msg->data)
                                    (map ->entity)
                                    (split-books))
                   new-books {:bids bids
                              :asks asks}]
               (prn "snapshot" new-books)
               (vreset! aggr new-books)
               (xf result new-books))

             (core/is-valid? ::book-update input)
             (let [msg (->> input
                            (core/public-msg->data)
                            (->entity))
                   bid-ask-kw (if (> (:amount msg) 0) :bids :asks)
                   new-books (if (zero? (:count msg))
                               (assoc @aggr bid-ask-kw
                                 (filter #(not (= (:price msg) (:price %)) @aggr)))
                               (let [new-coll (as-> (bid-ask-kw @aggr) c
                                                (into
                                                  (sorted-map-by (bid-ask-kw comparators))
                                                  (map #(identity [(:price %) %]) c))
                                                (assoc c (:price msg) msg)
                                                (vals c))]
                                 (assoc @aggr bid-ask-kw new-coll)))]
               (prn "update msg" msg " and new books: " new-books)
               (vreset! aggr new-books)
               (xf result new-books))))))))


(defn sub-aggregated-snapshot
  ([client opts ch] (sub-aggregated-snapshot client opts ch true))
  ([client opts ch close?]
   (let [t (->book-subscription opts)
         ->book (get->book opts)
         c (chan (sliding-buffer 1)
                 (comp (map second)
                       (aggregated-snapshot-xf ->book)))]

      (core/sub-pipe (:publication client) t c ch close?))))


; (def options {:symbol "tBTCUSD"})
; (def options {:symbol "tBTCUSD" :freq "F1" :prec "P2"})
;
; (def snapshot (chan 1))
; (def snapshot-aggr (chan 1))
; (def update-ch (chan))
;
; (def gloop (async/go-loop []
;              (let [[v p] (alts! [snapshot update-ch])] ;;snapshot-aggr
;                (cond
;                  (= p snapshot) (prn "snapshot" v)
;                  (= p update-ch) (prn "update" v)
;                  (= p snapshot-aggr) (prn "snapshot-aggr" v)
;                  :default (prn v p))
;                (recur))))
;
; (def c (client/client))
;
; (def update-sub (sub-update c options update-ch false))
; (def snapshot-sub (sub-snapshot c options snapshot false))
; (def aggr-sub (sub-aggregated-snapshot c options snapshot-aggr false))
;
; (client/connect c)
; (client/close c)
;
; (subscribe! c options)
; (unsubscribe! c options)
