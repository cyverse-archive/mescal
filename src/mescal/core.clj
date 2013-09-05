(ns mescal.core
  (:require [cheshire.core :as cheshire]
            [cemerick.url :as curl]
            [clj-http.client :as client]))

(defprotocol AgaveClient
  "A client for the Agave API.")
