(ns mescal.agave-de-v1)

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
