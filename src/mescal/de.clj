(ns mescal.de
  (:require [mescal.agave-de-v1 :as v1]
            [mescal.core :as core]))

(defprotocol DeAgaveClient
  "An Agave client with customizations that are specific to the discovery environment."
  (publicAppGroup [this])
  (listPublicApps [this])
  (getApp [this app-id])
  (getAppDeployedComponent [this app-id])
  (submitJob [this submission]))

(deftype DeAgaveClientV1 [agave jobs-enabled? irods-home]
  DeAgaveClient
  (publicAppGroup [this]
    (v1/public-app-group))
  (listPublicApps [this]
    (v1/list-public-apps agave jobs-enabled?))
  (getApp [this app-id]
    (v1/get-app agave app-id))
  (getAppDeployedComponent [this app-id]
    (v1/get-deployed-component-for-app agave app-id))
  (submitJob [this submission]
    (v1/submit-job agave irods-home submission)))

(defn de-agave-client-v1
  [base-url proxy-user proxy-pass user jobs-enabled? irods-home]
  (DeAgaveClientV1.
   (core/agave-client-v1 base-url proxy-user proxy-pass user)
   jobs-enabled?
   irods-home))
