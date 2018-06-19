(ns bitfinex-clj.subscriptions
  (:require [clojure.string :as str]
            [clojure.set :as set]
            [bitfinex-clj.actions :as acts]))

;;;;; Symbols
;; Better way to handle this?
(defn is-funding?
  [s]
  (= (count s) 3))

;; Subscriptions messages
(defn subscribe-msg
  "Constructs the subscribe message"
  [channel opts]
  (merge {:event "subscribe" :channel channel} opts))

(defn unsubscribe-msg
  [chan-id]
  {:event "unsubscribe" :chanId chan-id})

;; Transforms from the user defined subscription format to bitfinex websocket one
(defmulti user-sub->ws-sub :channel)

(defmethod user-sub->ws-sub :candles
  [{:keys [symbol timeframe]}]
  (subscribe-msg
   "candles"
   {:key (str "trade:" (name timeframe) ":t" symbol)}))

(defmethod user-sub->ws-sub :ticker
  [{:keys [symbol]}]
  (let [prefix (if (is-funding? symbol) "f" "t")]
    (subscribe-msg
     "ticker"
     {:symbol (str prefix symbol)})))

(defmethod user-sub->ws-sub :trades
  [{:keys [symbol]}]
  (let [prefix (if (is-funding? symbol) "f" "t")]
    (subscribe-msg
     "trades"
     {:symbol (str prefix symbol)})))

(defmethod user-sub->ws-sub :books
  [{:keys [symbol] :as opts}]
  (let [prefix (if (is-funding? symbol) "f" "t")
        opts (merge opts {:symbol (str prefix symbol)})]
    (subscribe-msg
     "book"
     opts)))

;; Transforms from the subscribed message to the user-defined subscription
(defmulti ws-sub->user-sub :channel)
(defmethod ws-sub->user-sub "ticker"
  [{:keys [pair currency]}]
  {:channel :ticker
   :symbol (or pair currency)})
(defmethod ws-sub->user-sub "candles"
  [{:keys [key]}]
  (let [[_ tf s] (str/split key #":")
        symbol (subs s 1)
        timeframe (keyword tf)]
    {:channel :candles
     :symbol symbol
     :timeframe timeframe}))

(defmethod ws-sub->user-sub "trades"
  [{:keys [pair currency]}]
  {:channel :candles
   :symbol (or pair currency)})

(defmethod ws-sub->user-sub "book"
  [{:keys [symbol prec freq len]}]
  (let [opts {:symbol (subs symbol 1) :prec prec :len len}]
    (merge {:channel :books}
           (if freq (merge opts {:freq freq}) opts))))

;; Multi to get default options for each channnel. Used specifically in books
(defn is-raw-book [prec] (= prec "R0"))

(def BOOKS_DEFAULT_PREC "P0")
(def BOOKS_DEFAULT_FREQ "F0")
(def BOOKS_DEFAULT_LEN "25")

(defmulti add-defaults :channel)
(defmethod add-defaults :books
  [opts]
  (let [defaults {:prec BOOKS_DEFAULT_PREC
                  :len BOOKS_DEFAULT_LEN}]
    (if (is-raw-book (:prec opts))
      (merge defaults opts)
      (merge defaults {:freq BOOKS_DEFAULT_FREQ} opts))))

(defmethod add-defaults :default [opts] opts)

;; Functions to process subscriptions state and actions
(defn create-sub-state
  [s ch]
  {:state s :chan-id ch})

(defn add-user-subscription
  [user-subscriptions new-sub]
  (conj user-subscriptions new-sub))

(defn remove-user-subscription
  [user-subscriptions unsub]
  (disj user-subscriptions unsub))

(defn state?
  "Checks if a subscription value its at certain state"
  [state v]
  (= (:state v) state))

(defn process-user-subscription
  [[{:keys [user-subscriptions] :as state} actions] {:keys [data]}]
  [(assoc state :user-subscriptions (add-user-subscription user-subscriptions data))
   actions])

(defn process-user-unsubscription
  [[{:keys [user-subscriptions] :as state} actions] {:keys [data]}]
  [(assoc state :user-subscriptions (remove-user-subscription user-subscriptions data))
   actions])

(defn subscription-from-chan-id
  [ws-subscriptions chan-id]
  (some (fn [[k v]] (when (= (:chan-id v) chan-id) k)) ws-subscriptions))

(defn ws-on-subscribed
  [ws-subscriptions subscribed-msg]
  (assoc
    ws-subscriptions
    (ws-sub->user-sub subscribe-msg)
    (create-sub-state :subscribed (:chanId subscribe-msg))))

(defn ws-on-unsubscribed
  [ws-subscriptions unsubscribed-msg]
  (let [sub (-> ws-subscriptions
                (subscription-from-chan-id (:chanId unsubscribe-msg)))]
    (dissoc
      ws-subscriptions
      sub)))

(defn sync-subscribe
  [ws-subscriptions user-subscriptions]
  (let [pending-subs (set/difference user-subscriptions (into #{} (keys ws-subscriptions)))
        pending-subs-msgs (-> pending-subs (map user-sub->ws-sub))
        new-ws-subs (into ws-subscriptions (for [p pending-subs]
                                             [p (create-sub-state :subscribing)]))]
    [new-ws-subs pending-subs-msgs]))

(defn sync-unsubscribe
  [ws-subscriptions user-subscriptions]
  (let [sub-chan-id (fn [s] (get-in ws-subscriptions [s :chan-id]))
        diff (set/difference (into #{} (keys ws-subscriptions)) user-subscriptions)
        ; We only unsub subscribed ones
        to-unsub (filter (partial state? :subscribed) diff)
        unsub-msgs (-> to-unsub
                       (map (fn [s] (unsubscribe-msg (sub-chan-id s)))))
        new-ws-subs (into ws-subscriptions (for [u to-unsub]
                                             [u (create-sub-state
                                                  :unsubscribing
                                                  (sub-chan-id u))]))]
    [new-ws-subs unsub-msgs]))

(defn process-sync-ws-subscriptions
  "Given an array of [state actions], returns a new array of [state actions]
  to synchronize user subscriptions and websocket subscriptions"
  [[{:keys [user-subscriptions ws-subscriptions] :as state} actions]]
  (let [[new-subs-ws sub-msgs] (sync-subscribe ws-subscriptions user-subscriptions)
        [new-unsubs-ws unsub-msgs] (sync-unsubscribe new-subs-ws user-subscriptions)
        msg-actions (->> (into sub-msgs unsub-msgs)
                         (map (partial acts/create-action :send)))
        new-state (assoc state :ws-subscrptions new-unsubs-ws)
        new-acts (into actions msg-actions)]
    [new-state new-acts]))
