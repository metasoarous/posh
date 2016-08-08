(ns posh.reagent
  (:require-macros [reagent.ratom :refer [reaction]]
                   [cljs.core.async.macros :as async-macros :refer [go go-loop]]
                   [servant.macros :refer [defservantfn]])
  (:require
    [posh.core :as p]
    [posh.stateful :as ps]
    [posh.lib.db :as db]
    [posh.lib.update :as u]
    [datascript.core :as d]
    [reagent.core :as r]
    [reagent.ratom :as ra]
    [com.stuartsierra.component :as component]
    [cljs.core.async :as async :refer [chan close! timeout put! <! >!]]
    ;[cljs.spec :as s]
    ;[cljs.core.match :as match :refer-macros [match]]
    [servant.core :as servant]
    [servant.worker :as worker]
    [com.stuartsierra.component :as component]
    [datascript.core :as d]
    [reagent.core :as r]))


(def dcfg
  {:db d/db
   :pull d/pull
   :q d/q
   :filter d/filter
   :with d/with
   :entid d/entid
   :transact! d/transact!})


(defservantfn
  handle-transaction
  [posh-tree conn-tx-reports]
  (p/after-transact posh-tree conn-tx-reports))


;; Don't know if we'll need this, but might be useful to have a logical ordinal clock to make sure late messages
;; aren't overriding newer ones
(let [s (atom 0)]
  (defn clock-ord!
    []
    (swap! s inc)))

(defn merge-tx-reports
  [txrs]
  {:db-before (:db-before (first txrs))
   :db-after (:db-after (last txrs))
   ;; Shoud we be returning a set?
   :datoms (distinct (mapcat :datoms txrs))})

;; QUESTION should maybe be private?
;; need to set last-tx-t in conn so that it doesn't try the same tx twice
(defn set-conn-listener! [posh-atom conn db-id]
  "Adds a connection listener on the datascript db that sends transaction reports to a channel `(:tx-report-chan @posh-atom)`:
  Also adds a posh-dispensor listener function which does nothing for db values, but when called explicitly can be
  used to access posh atom state."
  (let [tx-report-chan (:tx-report-chan @posh-atom) ;; need to make sure :tx-report chan is in place for this to work
        posh-vars {:posh-atom posh-atom
                   :db-id db-id}]
    (do
      ;; This is flipping brilliant if not a tad hacky; the posh-dispenser listener is not actually a listener,
      ;; but a way of retrieving data in posh-vars
      (d/listen! conn :posh-dispenser
                 (fn [var]
                   (when (keyword? var)
                     (get posh-vars var))))
      (d/listen! conn :posh-listener
                 (fn [tx-report]
                   ;;(println "CHANGED: " (keys (:changed (p/after-transact @posh-atom {conn tx-report}))))
                   (go (>! tx-report-chan [conn tx-report]))
                   (let [new-posh-tree-chan]
                     (let [{:keys [ratoms changed]}
                           ;; This should really be in a swap, yeah?
                           (swap! posh-atom p/after-transact conn tx-report)]
                       (doseq [[k v] changed]
                         ;; Oh... this is where changes flow through
                         (reset! (get ratoms k) (:results v)))))))
      conn)))


(def default-posh-options
  {:worker-count 4
   ;; QUESTION Not sure what's best here, but for now...
   :worker-script "js/app.js"})

;(defn drain!
;  [input-chan]
;  (let [out-chan (async/promise-chan)]
;    (go-loop [results []]
;      (if (seq results)
;        (let [x (async/alts!)])))
;    out-chan))

(defn consume-tx-reports!
  [posh-atom servant-channel conn tx-report-chan]
  (go
    (let [;; For now just hard coding async/take 100 (same n as default buffer size);
          ;; If we expose an option for channel buffer size, we should make sure it's accessible or finish implementing the drain! function above.
          reports-ch (async/take 100 tx-report-chan)
          grouped-reports
          (->> (<! reports-ch)
               (group-by first)
               (map (fn [conn conn-txrs]
                      [conn (merge-tx-reports (mapv last conn-txrs))])))
          new-posh-tree-chan (servant/servant-thread servant-channel servant/standard-message handle-transaction @posh-atom grouped-reports)
          ;; Park for the computation to compute in background (hopefully...)
          new-posh-tree (<! new-posh-tree-chan)
          {:keys [ratoms changed]} new-posh-tree]
        (reset! posh-atom new-posh-tree)
        (doseq [[k v] changed]
          ;; Oh... this is where changes flow through
          (reset! (get ratoms k) (:results v))))))


(defn posh! [options-or-conn & conns]
  (if (map? options-or-conn)
    (let [options (merge options-or-conn default-posh-options)
          posh-atom (atom {})]
      (reset! posh-atom
              (loop [n 0
                     conns conns
                     posh-tree (-> (p/empty-tree dcfg [:results])
                                   (assoc :ratoms {}
                                          :servant-channel (get options :servant-chanel (servant/spawn-servants (:worker-count options) (:worker-script options)))
                                          ;; We'll place tx reports on a sliding buffered chan, since we only really ever need the most up to date value
                                          ;:tx-report-chan (chan (async/sliding-buffer 1))
                                          :tx-report-chan (chan 100)
                                          :reactions {}))]
                (consume-tx-reports!)
                (if (empty? conns)
                  posh-tree
                  (recur (inc n)
                         (rest conns)
                         (let [db-id (keyword (str "conn" n))]
                           (p/add-db posh-tree
                                     db-id
                                     (set-conn-listener! posh-atom (first conns) db-id)
                                     (:schema @(first conns)))))))))
    (posh! {} conns)))


;; Posh's state atoms are stored inside a listener in the meta data of
;; the datascript conn
(defn get-conn-var [conn var]
  ((:posh-dispenser @(:listeners (meta conn))) var))

(defn get-posh-atom [poshdb-or-conn]
  (if (d/conn? poshdb-or-conn)
    (get-conn-var poshdb-or-conn :posh-atom)
    (ps/get-posh-atom poshdb-or-conn)))

(defn get-db [poshdb-or-conn]
  (if (d/conn? poshdb-or-conn)
    (with-meta
      [:db (get-conn-var poshdb-or-conn :db-id)]
      {:posh (get-conn-var poshdb-or-conn :posh-atom)})
    poshdb-or-conn))

(defn rm-posh-item [posh-atom storage-key]
  (swap! posh-atom
         (fn [posh-atom-val]
           (assoc (p/remove-item posh-atom-val storage-key)
             :ratoms (dissoc (:ratoms posh-atom-val) storage-key)
             :reactions (dissoc (:reactions posh-atom-val) storage-key)))))

(defn make-query-reaction [options posh-atom storage-key add-query-fn]
  (if-let [r (get (:reactions @posh-atom) storage-key)]
      r
      (->
       (swap!
        posh-atom
        (fn [posh-atom-val]
          (let [posh-atom-with-query (add-query-fn posh-atom-val)
                query-result         (:results (get (:cache posh-atom-with-query) storage-key))
                query-ratom          (or (get (:ratoms posh-atom-with-query) storage-key)
                                         (r/atom query-result))
                ;; Here we shoot off the web workers task
                query-reaction       (ra/make-reaction
                                      (fn []
                                        ;;(println "RENDERING: " storage-key)
                                        @query-ratom)
                                      :on-dispose
                                      (fn [_ _]
                                        ;;(println "no DISPOSING: " storage-key)
                                        ;; The when-not here _should_ make it so that when options :cache? is set, the reactions is always alive
                                        (when-not (:cache? options)
                                          (swap! posh-atom
                                                 (fn [posh-atom-val]
                                                   (assoc (p/remove-item posh-atom-val storage-key)
                                                     :ratoms (dissoc (:ratoms posh-atom-val) storage-key)
                                                     :reactions (dissoc (:reactions posh-atom-val) storage-key)))))))]
            (assoc posh-atom-with-query
              :ratoms (assoc (:ratoms posh-atom-with-query) storage-key query-ratom)
              :reactions (assoc (:reactions posh-atom-with-query) storage-key query-reaction)))))
       :reactions
       (get storage-key))))


(defservantfn add-pull
  [posh-tree]
  (p/add-pull posh-tree))

(defn pull [poshdb pull-pattern eid]
  (let [true-poshdb (get-db poshdb)
        storage-key [:pull true-poshdb pull-pattern eid]
        posh-atom   (get-posh-atom poshdb)]
    ;; install options
    (make-query-reaction {}
                         posh-atom
                         storage-key
                         #(p/add-pull % true-poshdb pull-pattern eid))))

(defn pull-info [poshdb pull-pattern eid]
  (let [true-poshdb (get-db poshdb)
        storage-key [:pull true-poshdb pull-pattern eid]
        posh-atom   (get-posh-atom poshdb)]
    (dissoc
     (u/update-pull @posh-atom storage-key)
     :reload-fn)))

(defn pull-tx [tx-patterns poshdb pull-pattern eid]
  (println "pull-tx is deprecated. Calling pull without your tx-patterns.")
  (pull poshdb pull-pattern eid))

;;; q needs to find the posh-atom, go through args and convert any
;;; conn's to true-poshdb's, generate the storage-key with true dbs

(defn q [query & args]
  (let [true-poshdb-args (map #(if (d/conn? %) (get-db %) %) args)
        posh-atom        (first (remove nil? (map get-posh-atom args)))
        storage-key      [:q query true-poshdb-args]]
    ;; install options
    (make-query-reaction {}
                         posh-atom
                         storage-key
                         #(apply (partial p/add-q % query) true-poshdb-args))))

(defn q-info [query & args]
  (let [true-poshdb-args (map #(if (d/conn? %) (get-db %) %) args)
        posh-atom        (first (remove nil? (map get-posh-atom args)))
        storage-key      [:q query true-poshdb-args]]
    (dissoc
     (u/update-q @posh-atom storage-key)
     :reload-fn)))

(defn q-tx [tx-patterns query & args]
  (println "q-tx is deprecated. Calling q without your tx-patterns.")
  (apply q query args))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn filter-tx [poshdb tx-patterns]
  (ps/add-filter-tx (get-db poshdb) tx-patterns))

(defn filter-pull [poshdb pull-pattern eid]
  (ps/add-filter-pull (get-db poshdb) pull-pattern eid))

(defn filter-q [query & args]
  (let [true-poshdb-args (map #(if (d/conn? %) (get-db %) %) args)]
    (apply ps/add-filter-q query true-poshdb-args)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn transact! [poshdb-or-conn txs]
  (d/transact!
   (if (d/conn? poshdb-or-conn)
     poshdb-or-conn
     (ps/poshdb->conn poshdb-or-conn))
   txs))
