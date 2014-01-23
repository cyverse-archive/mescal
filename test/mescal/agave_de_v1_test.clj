(ns mescal.agave-de-v1-test
  (:use [clojure.test])
  (:require [mescal.agave-de-v1 :as adv1]))

(defn- build-run-option
  [run-opt provided-value]
  (let [[name label type default-value visible?] (@#'adv1/run-option-info run-opt)]
    {:name         name
     :label        label
     :id           name
     :type         type
     :order        0
     :defaultValue (if (nil? provided-value) default-value provided-value)
     :isVisible    visible?}))

(deftest individual-run-options
  (testing "individual run options without provided values"
    (is (= (@#'adv1/format-run-option :software-name nil)
           (build-run-option :software-name nil)))
    (is (= (@#'adv1/format-run-option :processor-count nil)
           (build-run-option :processor-count nil)))
    (is (= (@#'adv1/format-run-option :max-memory nil)
           (build-run-option :max-memory nil)))
    (is (= (@#'adv1/format-run-option :requested-time nil)
           (build-run-option :requested-time nil))))
  (testing "individual run options with provided values"
    (is (= (@#'adv1/format-run-option :software-name "blah")
           (build-run-option :software-name "blah")))
    (is (= (@#'adv1/format-run-option :processor-count "27")
           (build-run-option :processor-count "27")))
    (is (= (@#'adv1/format-run-option :max-memory "42")
           (build-run-option :max-memory "42")))
    (is (= (@#'adv1/format-run-option :requested-time "2:00:00")
           (build-run-option :requested-time "2:00:00")))))

(defn- build-run-options
  [app]
  [(build-run-option :software-name (:id app))
   (build-run-option :processor-count (:defaultProcessors app))
   (build-run-option :max-memory (:defaultMemory app))
   (build-run-option :requested-time (:defaultRequestedTime app))])

(defn- test-run-options
  [app]
  (is (= (@#'adv1/format-run-options app)
         (build-run-options app))))

(deftest run-options
  (testing "all run options without provided values"
    (test-run-options {}))
  (testing "all run options with provided values"
    (test-run-options {:id                   "foo-1.2.3u27"
                       :defaultProcessors    "42"
                       :defaultMemory        "64"
                       :defaultRequestedTime "2:00:00"})))
