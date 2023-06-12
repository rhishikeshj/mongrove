(ns mongrove.update-test
  (:require
    [clojure.string :as cs]
    [clojure.test :refer :all]
    [java-time.api :as t]
    [mongrove.core :as mc]))


(def shared-connection (atom nil))


(defn- init-connection-fixture
  "This will run once before tests are executed
  and will initialise a connection into the shared-connection atom"
  [tests]
  (let [c (mc/connect :replica-set [{:host "localhost"
                                     :port 27017}])]
    (reset! shared-connection c)
    (tests)))


(defn- before-each-test
  []
  nil)


(defn- after-each-test
  "Clean up any test data that we might create.
  Note : All test data should be created under databases starting with `test-db`"
  []
  (let [dbs (mc/get-database-names @shared-connection)
        test-dbs (filter #(cs/starts-with? % "test-db") dbs)]
    (doseq [db test-dbs]
      (mc/drop-database (mc/get-db @shared-connection db)))))


(defn each-fixture
  [tests]
  (before-each-test)
  (tests)
  (after-each-test))


(use-fixtures :once (join-fixtures [init-connection-fixture each-fixture]))


(deftest insert-test
  (testing "Update single field with $set"
    (let [client @shared-connection
          doc {:_id 1
               :name (.toString (java.util.UUID/randomUUID))
               :age (rand-int 60)
               :city (.toString (java.util.UUID/randomUUID))
               :country (.toString (java.util.UUID/randomUUID))
               :created-at (t/zoned-date-time 2023 06 11)}
          db (mc/get-db client (str "test-db-" (.toString (java.util.UUID/randomUUID))))
          coll (str "test-coll-" (.toString (java.util.UUID/randomUUID)))]
      (mc/insert db coll doc)
      (mc/update db coll {:_id 1} {:$set {:name "updated-name"}})
      (let [db-doc (mc/query db coll {})]
        (is (= 1 (count db-doc)))
        (is (= (-> doc
                   (update :created-at t/java-date)
                   (assoc :name "updated-name")) (first db-doc))))))
  (testing "Update single field with $inc"
    (let [client @shared-connection
          doc {:_id 1
               :name (.toString (java.util.UUID/randomUUID))
               :age 41
               :city (.toString (java.util.UUID/randomUUID))
               :country (.toString (java.util.UUID/randomUUID))
               :created-at (t/zoned-date-time 2023 06 11 22 32)}
          db (mc/get-db client (str "test-db-" (.toString (java.util.UUID/randomUUID))))
          coll (str "test-coll-" (.toString (java.util.UUID/randomUUID)))]
      (mc/insert db coll doc :multi? false :write-concern :majority)
      (mc/update db coll {:_id 1} {:$inc {:age 1}})
      (let [db-doc (mc/query db coll {})]
        (is (= 1 (count db-doc)))
        (is (= (-> doc
                   (update :created-at t/java-date)
                   (assoc :age 42))
               (first db-doc))))))
  (testing "Update multiple documents"
    (let [client @shared-connection
          docs (for [i (range 10)]
                 {:_id i
                  :name (.toString (java.util.UUID/randomUUID))
                  :age (rand-int 60)
                  :city (.toString (java.util.UUID/randomUUID))
                  :country (.toString (java.util.UUID/randomUUID))
                  :created-at (t/zoned-date-time 2023 06 11 22 35)})
          db (mc/get-db client (str "test-db-" (.toString (java.util.UUID/randomUUID))))
          coll (str "test-coll-" (.toString (java.util.UUID/randomUUID)))]
      (mc/insert db coll docs :multi? true :write-concern :majority)
      (mc/update db coll {} {:$unset {:city :val-does-not-matter}} :multi? true)
      (let [db-docs (mc/query db coll {})]
        (is (= 10 (count db-docs)))
        (is (= (map (fn [d]
                      (dissoc (update d :created-at t/java-date) :city))
                    docs) db-docs))))))
