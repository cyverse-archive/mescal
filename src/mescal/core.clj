(ns mescal.core
  (:use [clojure.java.io :only [reader]])
  (:require [cheshire.core :as cheshire]
            [cemerick.url :as curl]
            [clj-http.client :as client]))

(defn- decode-json
  [source]
  (if (string? source)
    (cheshire/decode source true)
    (cheshire/decode-stream (reader source) true)))

(defn- extract-result-v1
  [response]
  ((comp :result decode-json :body) response))

(defn- authenticate-v1
  [base-url proxy-user proxy-pass user]
  ((comp :token :result decode-json :body)
   (client/post (str (curl/url base-url "auth-v1") "/")
                {:accept      :json
                 :as          :stream
                 :form-params {:username user}
                 :basic-auth  [proxy-user proxy-pass]})))

(defn- list-systems-v1
  [base-url]
  (extract-result-v1
   (client/get (str (curl/url base-url "apps-v1" "systems" "list"))
               {:accept :json
                :as     :stream})))

(defn- list-public-apps-v1
  [base-url]
  (extract-result-v1
   (client/get (str (curl/url base-url "apps-v1" "apps" "list"))
               {:accept :json
                :as     :stream})))

(defn- list-my-apps-v1
  [base-url]
  (extract-result-v1
   (client/get (str (curl/url base-url "apps-v1" "apps" "shared" "list"))
               {:accept :json
                :as     :stream})))

(defn- get-app-v1
  [base-url app-id]
  (extract-result-v1
   (client/get (str (curl/url base-url "apps-v1" "apps" app-id))
               {:accept :json
                :as     :stream})))

(defn- required-field
  [field m]
  (let [v (m field)]
    (when (nil? v)
      (throw (IllegalArgumentException. (str "Missing required field, " (name field)))))
    v))

(defn- common-params-v1
  [params]
  {:softwareName   (required-field :softwareName params)
   :jobName        (required-field :jobName params)
   :processorCount (:processorCount params)
   :maxMemory      (:maxMemory params)
   :requestedTime  (required-field :requestedTime params)
   :callbackUrl    (:callbackUrl params)
   :archive        (:archive params)
   :archivePath    (:archivePath params)})

(defn- param-value-v1
  [params param-def]
  (let [{id :id {:keys [required default]} :value} param-def
        field-name                                 (keyword id)
        field-value                                (or (field-name params) default)]
    (when (and required (nil? field-value))
      (throw (IllegalArgumentException. (str "Missing required fiedl, " (name field-name)))))
    [field-name field-value]))

(defn- params-v1
  [param-defs params]
  (into {} (map (partial param-value-v1 params) param-defs)))

(defn- submit-job-v1
  [base-url user token params]
  (let [form (common-params-v1 params)
        app  (get-app-v1 base-url (:softwareName form))
        form (merge form (params-v1 (:inputs app) params))
        form (merge form (params-v1 (:parameters app) params))
        form (into {} (remove (fn [[_ v]] (nil? v)) form))]
    ((comp slurp :body)
     (client/post (str (curl/url base-url "apps-v1" "job") "/")
                  {:basic-auth  [user token]
                   :form-params form
                   :accept      :json
                   :as          :stream}))))

(defprotocol AgaveClient
  "A client for the Agave API."
  (listSystems [this])
  (listPublicApps [this])
  (countPublicApps [this])
  (listMyApps [this])
  (countMyApps [this])
  (getApp [this app-id])
  (submitJob [this params]))

(deftype AgaveClientV1 [base-url user token]
  AgaveClient
  (listSystems [this]
    (list-systems-v1 base-url))
  (listPublicApps [this]
    (list-public-apps-v1 base-url))
  (countPublicApps [this]
    (count (list-public-apps-v1 base-url)))
  (listMyApps [this]
    (list-my-apps-v1 base-url user token))
  (countMyApps [this]
    (count (list-my-apps-v1 base-url user token)))
  (getApp [this app-id]
    (get-app-v1 base-url app-id))
  (submitJob [this params]
    (submit-job-v1 base-url user token params)))

(defn agave-client-v1
  [base-url proxy-user proxy-pass user]
  (let [token (authenticate-v1 base-url proxy-user proxy-pass user)]
    (AgaveClientV1. base-url user token)))
