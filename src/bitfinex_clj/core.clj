(ns bitfinex-clj.core
  (:require [cheshire.core :as json]
            [clojure.spec.alpha :as spec]
            [clojure.set :as set]
            [clojure.core.async :as async :refer [go go-loop >! <! >!! <!! sub pipe]]))


;; Spec common
(spec/def ::symbol string?)
(spec/def ::chan-id integer?)

;; Some utility functions & macros
(defmacro kat [& ks]
  "Version of spec/cat that uses the same keyword as key and predicate of returned map"
  (let [ks' (mapcat #(repeat 2 %) ks)]
    `(spec/cat ~@ks')))

(defmacro defentity
  "Given an entity symbol and a vector of namespaced keys,
  it defines a tuple spec with expanded keys and name ::[entity],
  and defines a fn with name ->[entity] to transform data in the spec
  format to a map with the same namespaced keys and values in data"
  [entity ks]
  `(do
     (spec/def ~(keyword (str *ns*) (name entity)) (spec/tuple ~@ks))
     (defn ~(symbol (str "->" (name entity)))
       [data#]
       (zipmap (map (comp keyword name) ~ks) data#))))

(defn field-checker
  [prop value]
  (fn [msg] (= value (prop msg))))

(defn find-key
  [pred coll]
  (some (fn [[k v]] (when (pred v) k)) coll))

(defn filter-by
  [prop v coll]
  (filter #(= (prop (val %)) v) coll))

(defn subset?
  [n m]
  (set/subset? (set n) (set m)))

(defn is-valid?
  [spec data]
  (spec/valid? spec data))

(defn filter-spec
  [spec]
  (filter (partial is-valid? spec)))

(defn public-msg->data
  [inc]
  (last inc))

(defn ->entity-snapshot
  [entity snapshot]
  (map entity snapshot))

(defn log-xf [prefix]
  (fn [xf]
    (fn
      ([] (xf))
      ([r] (xf r))
      ([r i] (prn prefix) (prn i) (xf r i)))))

(defn sub-pipe
  ([p t from to]
   (sub-pipe p t from to true))
  ([p t from to close?]
   (sub p t from true)
   (pipe from to close?)))

(defn is-funding?
  [symbol]
  (= (first symbol) \f))

(defn is-trading?
  [symbol]
  (= (first symbol) \t))

(defn can-send?
  [{:keys [ws-connected? ws-maintenance?]}]
  (and ws-connected? (not ws-maintenance?)))

(defn ->subscription
  "Creates a subscribe message"
  [channel opts]
  (merge {:channel channel} opts))

;; process main functions: event-handler and effect-handler
(defmulti effect-handler (fn [fx deps] (first fx)))

(defmulti event-handler (fn [cofx in] (first in)))

;; loop
(defn process-loop
  [{:keys [queue fx-deps cofx-fn]}]
  (go-loop []
    (if-let [in (<! queue)]
      (do (prn (first in))
          (let [effects (event-handler (cofx-fn) in)]
            (doseq [fx effects]
              (effect-handler fx fx-deps))
            (recur))))))
