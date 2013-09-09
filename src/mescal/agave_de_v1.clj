(ns mescal.agave-de-v1
  (:require [clojure.string :as string]))

(def ^:private hpc-group-description "Apps that run on HPC resources.")
(def ^:private hpc-group-name "High-Performance Computing")
(def ^:private hpc-group-id "HPC")
(def ^:private unknown-value "UNKNOWN")

(defn public-app-group
  []
  {:description    hpc-group-description
   :id             hpc-group-id
   :is_public      true
   :name           hpc-group-name
   :template_count -1
   :workspace_id   0})

(defn- system-statuses
  [agave]
  (into {} (map (fn [m] [(:resource.id m) (:status m)]) (.listSystems agave))))

(defn- app-enabled?
  [statuses jobs-enabled? listing]
  (and jobs-enabled?
       (:available listing)
       (= "up" (statuses (:executionHost listing)))))

(defn- format-app-listing
  [statuses jobs-enabled? listing]
  (let [disabled? (not (app-enabled? statuses jobs-enabled? listing))]
    (-> listing
        (dissoc :available :checkpointable :deploymentPath :executionHost :executionType
                :helpURI :inputs :longDescription :modules :ontolog :outputs :parallelism
                :parameters :public :revision :shortDescription :tags :templatePath
                :testPath :version)
        (assoc
            :can_run              true
            :deleted              false
            :description          (:shortDescription listing)
            :disabled             disabled?
            :edited_date          (System/currentTimeMillis)
            :group_id             hpc-group-id
            :group_name           hpc-group-name
            :integration_date     (System/currentTimeMillis)
            :integrator_email     unknown-value
            :integrator_name      unknown-value
            :is_favorite          false
            :is_public            (:public listing)
            :pipeline_eligibility {:is_valid false :reason "HPC App"}
            :rating               {:average 0.0}
            :step-count           1
            :wiki_url             ""))))

(defn list-public-apps
  [agave jobs-enabled?]
  (let [statuses (system-statuses agave)
        listing  (.listPublicApps agave)]
    (assoc (public-app-group)
      :templates      (map (partial format-app-listing statuses jobs-enabled?) listing)
      :template-count (count listing))))

(defn- get-boolean
  [value default]
  (cond (nil? value)    default
        (string? value) (Boolean/parseBoolean value)
        :else           value))

(defn- format-group
  [name params]
  {:name       name
   :label      name
   :id         name
   :type       ""
   :properties params
   :visible    true})

(defn- format-run-option
  ([name label type value]
     (format-run-option name label type value true))
  ([name label type value visible?]
     {:name    name
      :label   label
      :id      name
      :type    type
      :order   0
      :value   value
      :visible visible?}))

(defn- format-run-options
  [app-id]
  [(format-run-option "softwareName" "App ID" "Text" app-id false)
   (format-run-option "processorCount" "Processor Count" "Integer" "1")
   (format-run-option "maxMemory" "Maximum Memory in Gigabytes" "Integer" "31")
   (format-run-option "requestedTime" "Requested Runtime" "Text" "00:30:00")])

(defn- format-input-validator
  [input]
  {:required (get-boolean (get-in input [:value :required]) false)})

(defn- format-input
  [input]
  {:arguments    []
   :defaultValue (get-in input [:value :default])
   :description  (get-in input [:details :description])
   :id           (:id input)
   :isVisible    (:isVisible data_object)
   :label        (get-in input [:details :label])
   :name         (:id input)
   :required     (get-boolean (get-in input [:value :required]) false)
   :type         "FileInput"
   :validators   []})

(defn get-app
  [agave jobs-enabled? app-id]
  (let [app       (.getApp agave app-id)
        app-name  (first (remove string/blank? (map #(% app) [:name :id])))
        app-label (first (remove string/blank? (map #(% app) [:label :name :id])))]
    {:id           (str hpc-group-id "-" app-id)
     :name         app-name
     :label        app-label
     :component_id hpc-group-id
     :groups       [(format-group "Run Options" (format-run-options app-id))
                    (format-group "Inputs" (map format-input (:inputs app)))
                    (format-group "Parameters" (map format-param (:parameters app)))
                    (format-group "Outputs" (map format-output (:outputs app)))]}))
