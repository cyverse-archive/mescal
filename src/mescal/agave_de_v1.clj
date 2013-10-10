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
     {:name         name
      :label        label
      :id           name
      :type         type
      :order        0
      :defaultValue value
      :isVisible    visible?}))

(defn- format-run-options
  [app-id]
  [(format-run-option "softwareName" "App ID" "Text" app-id false)
   (format-run-option "processorCount" "Processor Count" "Integer" "1")
   (format-run-option "maxMemory" "Maximum Memory in Gigabytes" "Integer" "31")
   (format-run-option "requestedTime" "Requested Runtime" "Text" "00:30:00")])

(defn- format-input-validator
  [input]
  {:required (get-boolean (get-in input [:value :required]) false)})

(defn- number-type-for
  [xsd-type]
  (cond
   (= xsd-type "xs:decimal")       "Double"
   (= xsd-type "xs:float")         "Double"
   (= xsd-type "xs:double")        "Double"
   (= xsd-type "xs:integer")       "Integer"
   (= xsd-type "xs:long")          "Integer"
   (= xsd-type "xs:int")           "Integer"
   (= xsd-type "xs:short")         "Integer"
   (= xsd-type "xs:byte")          "Integer"
   (= xsd-type "xs:unsignedLong")  "Integer"
   (= xsd-type "xs:unsignedInt")   "Integer"
   (= xsd-type "xs:unsignedShort") "Integer"
   (= xsd-type "xs:unsignedByte")  "Integer"
   :else                           "Double"))

(defn- string-type-for
  [xsd-type]
  (cond
   (= xsd-type "xs:boolean") "Flag"
   :else                     "Text"))

(defn- get-param-type
  [param]
  (let [type     (get-in param [:value :type])
        ontology (get-in param [:semantics :ontology])
        xsd-type (first (filter (partial re-matches #"xs:.*") ontology))
        regex    (get-in param [:value :validator]) ]
    (cond
     (= type "number") (number-type-for xsd-type)
     (= type "string") (string-type-for xsd-type)
     (= type "bool")   "Flag")))

(defn- format-param
  [get-type param]
  {:arguments    []
   :defaultValue (get-in param [:value :default])
   :description  (get-in param [:details :description])
   :id           (:id param)
   :isVisible    (get-boolean (get-in param [:value :visible]) true)
   :label        (get-in param [:details :label])
   :name         (:id param)
   :order        0
   :required     (get-boolean (get-in param [:value :required]) false)
   :type         (get-type param)
   :validators   []})

(def ^:private format-input-param
  (partial format-param (constantly "FileInput")))

(def ^:private format-opt-param
  (partial format-param get-param-type))

(def ^:private format-output-param
  (partial format-param (constantly "Output")))

(defn get-app
  [agave app-id]
  (let [app       (.getApp agave app-id)
        app-name  (first (remove string/blank? (map #(% app) [:name :id])))
        app-label (first (remove string/blank? (map #(% app) [:label :name :id])))]
    {:id           app-id
     :name         app-name
     :label        app-label
     :component_id hpc-group-id
     :groups       [(format-group "Run Options" (format-run-options app-id))
                    (format-group "Inputs" (map format-input-param (:inputs app)))
                    (format-group "Parameters" (map format-opt-param (:parameters app)))
                    (format-group "Outputs" (map format-output-param (:outputs app)))]}))

(defn get-deployed-component-for-app
  [agave app-id]
  (let [app  (.getApp agave app-id)
        path (:deploymentPath app)]
    {:attribution ""
     :description (:shortDescription app)
     :id          (:id app)
     :location    (string/replace path #"/[^/]+$" "")
     :name        (string/replace path #"^.*/" "")
     :type        (:executionType app)
     :version     (:version app)}))

(defn- regex-quote
  [s]
  (java.util.regex.Pattern/quote s))

(def ^:private path-fix-regex
  (memoize (fn [irods-home] (re-pattern (str "^" (regex-quote irods-home) "|/$")))))

(defn- fix-path
  [path irods-home]
  (when-not (nil? path)
    (string/replace path (path-fix-regex irods-home) "")))

(defn- get-archive-path
  [irods-home submission]
  (let [irods-home (string/replace irods-home #"/$" "")]
    (str (fix-path (:outputDirectory submission) irods-home)
         "/" (:name submission))))

(defn- fix-input-paths
  [irods-home app config]
  (reduce
   (fn [config {input-id :id}]
     (update-in config [(keyword input-id)] fix-path irods-home))
   config
   (:inputs app)))

(defn job-config
  [irods-home app submission]
  (assoc (fix-input-paths irods-home app (:config submission))
    :archive     true
    :archivePath (get-archive-path irods-home submission)
    :jobName     (:name submission)))

(defn submit-job
  [agave irods-home submission]
  (let [app    (.getApp agave (:analysis_id submission))
        config (job-config irods-home app submission)]
    (.submitJob agave app config)))
