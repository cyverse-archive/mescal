(ns mescal.de
  (:require [mescal.agave-de-v1 :as v1]
            [mescal.core :as core]))

(defprotocol DeAgaveClient
  "An Agave client with customizations that are specific to the discovery environment."
  (publicAppGroup [this])
  (listPublicApps [this]))

(deftype DeAgaveClientV1 [agave jobs-enabled?]
  DeAgaveClient
  (publicAppGroup [this]
    (v1/public-app-group))
  (listPublicApps [this]
    (v1/list-public-apps agave jobs-enabled?)))

(defn de-agave-client-v1
  [base-url proxy-user proxy-pass user jobs-enabled?]
  (DeAgaveClientV1.
   (core/agave-client-v1 base-url proxy-user proxy-pass user)
   jobs-enabled?))
